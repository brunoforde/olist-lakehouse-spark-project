from pyspark.sql import SparkSession
import os
import sys

def process_file(spark, file_name):
    """
    Função responsável por ler um único arquivo CSV e salvar como Parquet.
    """
    print(f">>> [INFO] Processando arquivo: {file_name}")

    # 1. Definir caminhos (Input e Output)
    input_path = f"/app/data/raw/{file_name}"
    
    # Truque de Sênior: Se o arquivo é "clientes.csv", a tabela deve chamar "clientes"
    table_name = file_name.replace(".csv", "") 
    output_path = f"/app/data/bronze/{table_name}"

    # --- SUA MISSÃO COMEÇA AQUI ---
    
    # TODO 1: Ler o arquivo CSV usando o Spark
    # Dica: Use spark.read.csv(caminho, header=True, inferSchema=True)
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    # TODO 2: Salvar o DataFrame em formato Parquet
    # Dica: Use df.write.format("parquet").mode("overwrite").save(caminho)
    # Lembre-se de usar o output_path definido acima
    df.write.format("parquet").mode("overwrite").save(output_path)
    
    # --- FIM DA SUA MISSÃO ---
    
    print(f"-> [SUCCESS] Tabela {table_name} salva em: {output_path}")


def main():
    print("-> [INFO] Iniciando Ingestão Bronze...")

    # Configuração Padrão (igual ao Hello World)
    spark = SparkSession.builder \
        .appName("IngestaoBronze") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # 2. Listar arquivos da pasta RAW
    raw_folder = "/app/data/raw"
    files = os.listdir(raw_folder)

    # 3. Loop para processar cada arquivo
    for file in files:
        if file.endswith(".csv"):
            # Chama a função que você vai completar lá em cima
            process_file(spark, file)
        else:
            print(f"-> [SKIP] Arquivo ignorado: {file}")

    print("-> [INFO] Pipeline finalizado com sucesso!")
    spark.stop()

if __name__ == "__main__":
    main()