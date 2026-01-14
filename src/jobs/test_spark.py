from pyspark.sql import SparkSession
import os
import sys

def main():
    print(">>> [INFO] Starting Spark Session...")
    
    # Initialize Spark Session with Delta Lake support
    # Configuration tailored for local Docker environment
    spark = SparkSession.builder \
        .appName("OlistHello") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    print(">>> [INFO] Spark Session created successfully.")

    # Path mapped inside the Docker container
    file_path = "/app/data/raw/olist_customers_dataset.csv"
    
    if not os.path.exists(file_path):
        print(f">>> [ERROR] File not found: {file_path}")
        print(">>> [HINT] Ensure the dataset is mounted correctly to /app/data/raw")
        sys.exit(1)
    else:
        print(f">>> [INFO] Reading file: {file_path}")
        # Reading CSV with inferred schema for initial validation
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        
        print("\n=== SCHEMA ===")
        df.printSchema()
        
        print("\n=== SAMPLE DATA (TOP 5) ===")
        df.show(5)

    spark.stop()

if __name__ == "__main__":
    main()