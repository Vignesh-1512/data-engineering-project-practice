
Brazilian E-Commerce Data Engineering Project
(Databricks • PySpark • Medallion Architecture)

📌 Overview
This project demonstrates a real-world Data Engineering pipeline built using Python and Apache Spark on Databricks, following industry-standard Medallion Architecture and config-driven execution.
The pipeline processes the Brazilian E-Commerce (Olist) dataset end-to-end and delivers business-ready analytics marts aligned with real business requirements (BR-1 to BR-7).
Key Highlights


Fully config-driven (YAML)


Single entry point to run any layer


Dynamic table resolution using catalog & schema


Reusable utilities (ingest, refine, model, mart)


Production-style error handling


Designed for Databricks Unity Catalog



🧱 Architecture (Medallion)
c:\Users\Welcome\Downloads\ChatGPT Image Jan 30, 2026, 06_51_49 PM.png


📂 Project Structure
brazillian_e_commerce/
│
├── main.py                         # Single entry point
│
├── ingest/                         # Bronze layer
│   └── ingest_runner.py
│
├── refine/                         # Silver layer
│   └── refine_runner.py
│
├── gold/                           # Gold modeling
│   └── model.py
│
├── mart/                           # Business marts
│   ├── mart_runner.py
│   └── builders.py
│
├── utils/                          # Reusable utilities
│   ├── spark_session.py
│   ├── config_loader.py
│   ├── file_read.py
│   ├── file_hive.py
│   ├── data_cast.py
│   ├── data_prep.py
│   ├── fact_builder.py
│   ├── dim_builder.py
│   ├── watermark.py
│   ├── metadata.py
│   └── exceptions.py
│
├── config/
│   └── tables.yaml                 # Single source of truth
│
└── README.md


⚙️ Configuration (YAML-Driven)
All table definitions, schemas, load types, and business logic dependencies are defined in tables.yaml.
Example (Bronze)
bronze:
  orders:
    catalog: brazillian_e_commerce
    source_schema: source
    target_schema: bronze
    table_name: orders
    load_type: incremental
    watermark_column: order_purchase_timestamp

Why this matters:


No hardcoded table names in code


Switching source systems requires YAML change only


Supports Unity Catalog / multiple environments



🚀 Entry Point (main.py)
A single unified entry point controls the entire pipeline.
from brazillian_e_commerce.main import run

# Run full bronze ingestion
run("bronze")

# Run single silver table
run("silver", "orders")

# Run gold layer
run("gold")

# Run specific business requirement
run("mart", "seller_performance")

Supported Layers


bronze → Ingest


silver → Refine


gold → Model


mart → Business analytics



🟫 Bronze Layer (Ingest)
Purpose: Raw ingestion with minimal transformation.
Features


Full & incremental loads


Watermark-based ingestion


Metadata enrichment


Append vs overwrite handled dynamically


run("bronze")
run("bronze", "orders")


⚪ Silver Layer (Refine)
Purpose: Data quality & standardization.
Operations


Type casting


Column renaming


Null handling


Schema stabilization


run("silver")
run("silver", "payments")


🟨 Gold Layer (Model)
Purpose: Business-ready dimensional modeling.
Models Built
Dimensions


dim_customers


dim_products


dim_sellers


dim_date


Facts


fact_orders


fact_sales


fact_reviews


fact_payments


run("gold")
run("gold", "fact_sales")


🟦 Mart Layer (Business Requirements)
Final analytical tables answering real business questions.
Implemented BRs
BRMart TableDescriptionBR-1sales_performanceSales trend & revenueBR-2order_delivery_summaryDelivery performanceBR-3customer_analyticsCustomer behaviorBR-4seller_performanceSeller KPIsBR-5product_category_performanceCategory trendsBR-6payment_analyticsPayment insightsBR-7customer_satisfaction_and_reviewsReviews & satisfaction
run("mart")
run("mart", "payment_analytics")


❗ Error Handling
Custom, user-friendly exceptions are implemented:
class PipelineException(Exception): ...
class ConfigError(PipelineException): ...
class DataReadError(PipelineException): ...
class DataWriteError(PipelineException): ...
class TransformationError(PipelineException): ...

Example Error Message
[Bronze] Failed reading source table 'orders'
Reason: Table not found


🧪 Debugging & Observability


Strategic print() statements added


Row counts before & after transformations


Clear source → target visibility


Ideal for beginners & reviewers



🛠 Build & Install (Wheel)
pip install build
python -m build
pip install dist/brazillian_e_commerce-0.1.0-py3-none-any.whl


🧠 Key Learnings


Medallion architecture in practice


Config-driven pipelines


Incremental ingestion


Dimensional modeling


Business-first data design


Production-grade structure



👤 Author
Vignesh S
Aspiring Data Engineer | PySpark | Databricks | SQL

If you want next:


📄 Architecture diagram image


🎤 Interview explanation version


🧪 Test checklist


📈 Performance optimizations


🔁 Logging instead of print


Just tell me 💙