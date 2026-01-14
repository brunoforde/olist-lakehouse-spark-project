from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import sys

# Truque para importar de outras pastas (adiciona a raiz do projeto no path)
sys.path.append("/app")
from src.utils.text_cleaner import normalize_column_name

class BronzeToSilver:
    def __init__(self, spark):
        self.spark = spark
        self.input_path = "/app/data/bronze"
        self.output_path = "/app/data/silver"

    def list_tables(self):
        """Lista todas as pastas dentro da camada Bronze"""
        try:
            return [f for f in os.listdir(self.input_path) if not f.startswith(".")]
        except FileNotFoundError:
            print(f"-> [ERROR] Pasta {self.input_path} não encontrada.")
            return []

    def process_table(self, table_name):
        print(f"-> [INFO] Transformando tabela: {table_name}")
        
        # 1. Leitura (Agora lendo Parquet, que é mais rápido)
        path = f"{self.input_path}/{table_name}"
        df = self.spark.read.format("parquet").load(path)
        
        # 2. Transformação (Renomear colunas usando nossa função utilitária)
        new_columns = [normalize_column_name(c) for c in df.columns]
        df_clean = df.toDF(*new_columns)
        
        # 3. Escrita em DELTA (A grande mudança)
        # Delta cria logs de transação (_delta_log)
        target_path = f"{self.output_path}/{table_name}"
        
        df_clean.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(target_path)
            
        print(f"-> [SUCCESS] Salvo em Delta: {target_path}")

    def run(self):
        tables = self.list_tables()
        for table in tables:
            self.process_table(table)

def main():
    print("-> [INFO] Iniciando Pipeline Bronze -> Silver...")
    
    spark = SparkSession.builder \
        .appName("BronzeToSilver") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    pipeline = BronzeToSilver(spark)
    pipeline.run()
    
    print("-> [DONE] Processamento Silver finalizado.")
    spark.stop()

if __name__ == "__main__":
    main()