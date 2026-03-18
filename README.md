# olist-lakehouse-spark-project
Build a pipeline that ingests raw CSVs, cleans the data, and creates an aggregation layer for Business Intelligence.
# 🛒 Olist E-Commerce Data Lakehouse

An end-to-end Data Engineering project building a Medallion Architecture (Bronze, Silver, Gold) Lakehouse using PySpark, Delta Lake, and Docker. 

This project processes real-world e-commerce data from [Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), transforming raw CSVs into a business-ready Star Schema, focusing on data quality, idempotency, and scalable transformations.

## 🏗️ Architecture Overview

The pipeline follows the Medallion architecture pattern:

* **🥉 Bronze Layer (Raw):** Ingests raw `.csv` files and converts them to **Parquet** format. This layer acts as a historical archive with optimized reading performance.
* **🥈 Silver Layer (Cleansed):** Data is cleaned, column names are standardized (snake_case), and the format is upgraded to **Delta Lake** to support ACID transactions and time travel.
* **🥇 Gold Layer (Business):** Highly refined data modeled into a **Star Schema** (Dimensions and Facts) ready for BI tools and Analytics.

## 🛠️ Tech Stack

* **Apache Spark (PySpark):** Core distributed data processing engine.
* **Delta Lake:** Storage layer bringing reliability (ACID transactions) to data lakes.
* **Docker:** Containerization to ensure reproducible environments ("works on my machine" is not an option).
* **Python 3.9:** Scripting and orchestration.

## 💡 Key Architectural Decisions & Problem Solving

During the development of the **Gold Layer**, specific business logic challenges were addressed to ensure data integrity:

1.  **Preventing Cartesian Products in Fact Tables (`fato_pedidos`):**
    * *Challenge:* A single order can have multiple items and multiple payment methods. A direct join would explode the granularity and duplicate revenue.
    * *Solution:* Implemented a **Pre-Aggregation Strategy**. Payments were grouped and summed by `order_id` *before* joining with the items table, maintaining the exact granularity of 1 row per order item.
2.  **Handling Duplicate Dimensions (`dim_clientes`):**
    * *Challenge:* The geolocation dataset contained multiple slightly different coordinates for the same ZIP code.
    * *Solution:* Applied exact deduplication (`dropDuplicates`) on the ZIP code prefix to ensure a 1:1 safe join with the customer dataset.
3.  **Built-in Data Observability:**
    * Developed a custom `check_data_quality` utility function that runs before any Delta table is written. It validates row counts (preventing empty overwrites) and enforces Primary Key uniqueness dynamically.

## 🚀 Quickstart (How to Run)

The entire environment is containerized. No local Spark installation is required.

**1. Clone the repository and add the raw data:**
Ensure the Olist Kaggle dataset CSVs are placed in the `data/raw/` folder.

**2. Build the Docker Image:**
```bash
docker build -t olist-spark-image .

## 📊 Business Value & Interview FAQ

This project was built not just to move data, but to answer critical business questions reliably. Here is the thought process behind the architecture:

### Q1: "Why did you create a pre-aggregation step for payments in the Fact Table?"
**Business Context:** In e-commerce, calculating Gross Merchandise Value (GMV) or Total Revenue is the most critical metric for the C-level. 
**The Engineering Fix:** A single order can be paid with multiple methods (e.g., $50 on Credit Card, $20 on Voucher). If I joined the payments table directly with the items table, the database would create a Cartesian product, multiplying the item prices and inflating the company's revenue dashboard. By aggregating payments at the `order_id` level first, I guaranteed a 1:1 relationship, ensuring financial metrics are 100% accurate.

### Q2: "How would the Data Analytics team use this Star Schema?"
**Business Context:** Analysts need to slice and dice data without writing complex SQL every time.
**The Engineering Fix:** * To analyze **"Average Shipping Cost by Product Category"**, they simply join `fct_orders` with `dim_produtos`.
* To analyze **"Revenue Concentration by State/City"**, they join `fct_orders` with `dim_clientes`. The complexity of the raw data was abstracted away in the Data Engineering layer.

### Q3: "How do you ensure the CEO is not looking at wrong data?"
**Business Context:** Broken pipelines lead to broken trust. 
**The Engineering Fix:** I implemented a `check_data_quality` utility that acts as a Quality Gate. It dynamically checks for empty dataframes and Primary Key duplications (e.g., ensuring `sk_cliente` is strictly unique). If the raw data sends duplicate records, the pipeline alerts the engineering team before the bad data reaches the Gold layer and the BI dashboards.