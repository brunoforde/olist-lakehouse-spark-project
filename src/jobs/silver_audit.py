from pyspark.sql import SparkSession

def main():
    print(">>> [INFO] Iniciando Auditoria Visual (Bronze vs Silver)...")

    spark = SparkSession.builder \
        .appName("AuditSilver") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Vamos comparar a tabela de PRODUTOS
    table_name = "olist_products_dataset"
    
    # 1. Leitura da BRONZE (O Passado)
    df_bronze = spark.read.format("parquet").load(f"/app/data/bronze/{table_name}")
    
    # 2. Leitura da SILVER (O Presente)
    df_silver = spark.read.format("delta").load(f"/app/data/silver/{table_name}")

    print("\n" + "="*50)
    print(f" COMPARATIVO: {table_name}")
    print("="*50)

    # Comparação de SCHEMA (Aqui você vê a limpeza dos nomes)
    print("\n--- [1] ESTRUTURA ORIGINAL (Bronze) ---")
    df_bronze.printSchema()

    print("\n--- [2] ESTRUTURA LIMPA (Silver) ---")
    df_silver.printSchema()

    # Comparação de DADOS (Visualizar registros)
    print("\n" + "-"*50)
    print(" AMOSTRA DE DADOS (Top 3)")
    print("-"*50)
    
    print(">>> BRONZE (Raw):")
    df_bronze.select(df_bronze.columns[:3]).show(3, truncate=False) # Mostra só 3 primeiras colunas pra caber na tela
    
    print(">>> SILVER (Delta):")
    df_silver.select(df_silver.columns[:3]).show(3, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()