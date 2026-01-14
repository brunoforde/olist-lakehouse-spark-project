from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def process_dim_products(spark):
    print(">>> [INFO] Criando dim_produtos...")
    
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
    
    # 4. Salvar na Gold
    output_path = "/app/data/gold/dim_produtos"
    dim_products.write.format("delta").mode("overwrite").save(output_path)
    print(f">>> [SUCCESS] dim_produtos salva em {output_path}")


def check_data_quality(df, table_name):
    """
    Mini-Observabilidade: Garante que não estamos salvando tabelas vazias
    ou duplicando dados indevidamente.
    """
    count = df.count()
    print(f"[{table_name}] Total de linhas: {count}")
    
    if count == 0:
        raise ValueError(f"!!! [ERRO CRITICO] A tabela {table_name} ficou vazia após o processamento!")
    
    # Exemplo de observabilidade simples: Verificar duplicidade na chave primária
    if "sk_produto" in df.columns:
        distinct_count = df.select("sk_produto").distinct().count()
        if count != distinct_count:
            print(f"!!! [ALERTA] Duplicatas detectadas em sk_produto: {count - distinct_count}")
            
def main():
    spark = SparkSession.builder \
        .appName("GoldLayer") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    process_dim_products(spark)
    
    # TODO: Aqui virá a process_dim_clientes(spark)
    
    spark.stop()

if __name__ == "__main__":
    main()