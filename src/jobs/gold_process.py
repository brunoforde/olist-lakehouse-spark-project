from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

def process_dim_products(spark):
    print(">>> Criando dim_produtos... <<<")
    
    # 1. Ler as tabelas da Silver (já em formato Delta)
    df_products = spark.read.format("delta").load("/app/data/silver/olist_products_dataset")
    df_translation = spark.read.format("delta").load("/app/data/silver/product_category_name_translation")
    
    # 2. Join para pegar o nome da categoria em Inglês
    # Left Join: Queremos todos os produtos, mesmo que não tenham tradução
    df_joined = df_products.join(
        df_translation,
        df_products.product_category_name == df_translation.product_category_name,
        "left"
    )
    
    # 3. Selecionar e Renomear colunas finais (Modelagem)
    dim_products = df_joined.select(
        col("product_id").alias("sk_produto"), # Surrogate Key
        col("product_category_name_english").alias("categoria"),
        col("product_photos_qty").alias("qtd_fotos"),
        col("product_weight_g").alias("peso_g"),
        col("product_length_cm").alias("comprimento_cm")
    )
    
    # Data quality check
    check_data_quality(dim_products, "dim_produtos", "sk_produto")
    
    # 4. Salvar na Gold
    output_path = "/app/data/gold/dim_produtos"
    dim_products.write.format("delta").mode("overwrite").save(output_path)
    print(f">>> [SUCCESS] dim_produtos salva em {output_path} <<<")


def process_dim_clientes(spark):
    print(">>> [INFO] Criando dim_clientes...")
    
    # 1. Carregar tabelas (Caminhos do Docker corrigidos)
    df_customers = spark.read.format("delta").load("/app/data/silver/olist_customers_dataset")
    df_geo = spark.read.format("delta").load("/app/data/silver/olist_geolocation_dataset")

    # 2. A Mágica da Deduplicação (Seu instinto estava certo!)
    # Vamos manter apenas uma linha para cada CEP na tabela de geolocalização
    df_geo_unique = df_geo.dropDuplicates(["geolocation_zip_code_prefix"])

    # 3. O Join
    # Left Join: Queremos todos os clientes, mesmo que o CEP dele não exista na base de geolocalização
    df_join = df_customers.join(
        df_geo_unique,
        df_customers.customer_zip_code_prefix == df_geo_unique.geolocation_zip_code_prefix,
        "left"        
    )
    
    # 4. Select e Alias
    dim_clientes = df_join.select(
        col("customer_id").alias("sk_cliente"), # Chave única do pedido do cliente
        col("customer_unique_id"),              # Identificador único da pessoa
        col("customer_zip_code_prefix").alias("zip_code"),
        col("customer_city").alias("city"),
        col("customer_state").alias("state"),
        col("geolocation_lat").alias("latitude"),   # Vindo da tabela Geo
        col("geolocation_lng").alias("longitude")   # Vindo da tabela Geo
    )
    
    # 5. Data quality check
    check_data_quality(dim_clientes, "dim_clientes", "sk_cliente")
    
    # 6. Salvar na Gold
    output_path = "/app/data/gold/dim_clientes"
    dim_clientes.write.format("delta").mode("overwrite").save(output_path)
    print(f">>> [SUCCESS] dim_clientes salva em {output_path} <<<")
    
def process_fato_pedidos(spark):
    print(">>> [INFO] Criando fato_pedidos...")
    
    # 1. Leitura
    df_orders = spark.read.format("delta").load("/app/data/silver/olist_orders_dataset")
    df_payments = spark.read.format("delta").load("/app/data/silver/olist_order_payments_dataset") 
    df_itens = spark.read.format("delta").load("/app/data/silver/olist_order_items_dataset")
    
    # 2. Agregação com Alias Imediato e Sintaxe Correta
    # Usamos _sum em vez de sum, e passamos os nomes das colunas como string
    df_group_payments = df_payments.groupBy("order_id").agg(
        _sum("payment_value").alias("total_order_payment")
    )
    
    # 3. Joins sem Ambiguidade
    df_joined = (df_itens
        .join(df_orders, "order_id", "left") 
        .join(df_group_payments, "order_id", "left")
        )
    
    # 4. Select Final (Agora apenas "puxando" as colunas prontas)
    fct_orders = df_joined.select(
        col("customer_id").alias("sk_customer"),
        col("product_id").alias("sk_product"),
        col("order_id"), # Já estava limpo pelo Join
        col("order_item_id"),
        col("price").alias("preco"), # Padronizando para pt-br como pediu
        col("freight_value").alias("valor_frete"),
        col("total_order_payment"), # A coluna que já criamos lá no passo 2
        col("order_status").alias("status_pedido"),
        col("order_purchase_timestamp").alias("data_compra")
    )
    
    # 5. Data quality check
    check_data_quality(fct_orders, "fct_orders", None)
    
    # 6. Salvar na camada Gold
    output_path = "/app/data/gold/fato_pedidos" # Mantive o nome padrão do Lakehouse
    fct_orders.write.format("delta").mode("overwrite").save(output_path)
    print(f">>> [SUCCESS] fato_pedidos salva em {output_path} <<<")
   
    
def check_data_quality(df, table_name, pk_column=None):
    """
    Função utilitária de Observabilidade.
    Valida volumetria e unicidade de chaves primárias.
    """
    count = df.count()
    print(f"[{table_name}] Total de linhas: {count}")
    
    if count == 0:
        raise ValueError(f"!!! [ERRO CRITICO] A tabela {table_name} ficou vazia após o processamento!")
    
    # Checagem dinâmica de duplicatas
    if pk_column and pk_column in df.columns:
        distinct_count = df.select(pk_column).distinct().count()
        if count != distinct_count:
            print(f"!!! [ALERTA] Duplicatas detectadas na tabela {table_name}! Chave: {pk_column}. Sobrando: {count - distinct_count} linhas.")
        else:
            print(f"[{table_name}] [OK] Qualidade Validada: Nenhuma duplicata na chave '{pk_column}'.")
            

            
def main():
    spark = SparkSession.builder \
        .appName("GoldLayer") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    process_dim_products(spark)
    
    process_dim_clientes(spark)
    
    process_fato_pedidos(spark)
    
    spark.stop()

if __name__ == "__main__":
    main()