# 🛍 Retail Medallion Lakehouse Data Engineering Project

**Databricks • PySpark • Unity Catalog • Config-Driven Architecture**

------------------------------------------------------------------------

## 📌 Overview

This project demonstrates a production-style Retail Data Engineering
pipeline built using:

-   Python\
-   PySpark\
-   Databricks (Unity Catalog)\
-   Medallion Architecture\
-   Fully YAML-driven configuration

The pipeline processes retail transactional data end-to-end and
delivers:

-   Cleaned business-ready datasets\
-   Fact & Dimension tables\
-   Aggregated analytical tables\
-   BI & ML-ready publish outputs

------------------------------------------------------------------------

## 🔑 Key Highlights

✔ Fully config-driven (YAML controlled)\
✔ Single orchestration entry point\
✔ Multi-layer Medallion architecture\
✔ Reusable utilities (joins, validations, builders)\
✔ Safe schema handling for Delta\
✔ Star schema modeling in Publish layer\
✔ Partition-aware writes\
✔ Table-level execution supported\
✔ Unity Catalog compatible

------------------------------------------------------------------------

# 🧱 Architecture (Medallion Design)

<img width="1024" height="1536" alt="ChatGPT Image Feb 24, 2026, 01_06_32 PM" src="https://github.com/user-attachments/assets/d9af8616-b1d7-4de7-b7c8-2f02ae959980" />



------------------------------------------------------------------------

# 📂 Project Structure

<img width="1024" height="1536" alt="ChatGPT Image Feb 24, 2026, 01_16_38 PM" src="https://github.com/user-attachments/assets/d4241375-9b14-4d84-a527-700aa4dd3500" />


------------------------------------------------------------------------

# ⚙️ YAML-Driven Configuration

All logic is controlled via tables.yaml.

No hardcoded: - Table names\
- Column names\
- Join logic\
- Calculations\
- Aggregations

------------------------------------------------------------------------

# 🚀 Entry Point (main.py)

from retail_medallion_lakehouse.main import run

run("pre_landing") run("landing") run("unification") run("refinement")
run("publish")

------------------------------------------------------------------------

# 🟫 Pre-Landing Layer

Purpose: Raw ingestion from API / JSON.

Features: - Explode nested JSON\
- Contact extraction (email/phone)\
- Translation mapping\
- Append-based ingestion\
- Minimal transformation

------------------------------------------------------------------------

# ⚪ Landing Layer

Purpose: Clean & standardize.

Operations: - Type casting\
- Not-null validation\
- Positive value checks\
- Deduplication\
- Partition by year\
- Lineage tracking

------------------------------------------------------------------------

# 🟨 Unification Layer

Purpose: Business-level joining.

Features: - YAML-driven joins\
- Generic safe join\
- Dynamic duplicate column handling\
- Partition-aware writing\
- Audit column management

Grain: transaction_id + product_id

------------------------------------------------------------------------

# 🟦 Refinement Layer

Purpose: Business calculations & curated dataset.

Business Logic: - Net amount calculation\
- GST computation\
- Final invoice amount\
- Allocation ratio\
- YAML-driven column selection

Outputs: - net_amount\
- gst_amount\
- final_invoice_amount\
- allocation_ratio

------------------------------------------------------------------------

# 🟩 Publish Layer

Purpose: Deliver optimized business-ready datasets.

Dimensions: - customer_dimension\
- product_dimension

Facts: - cleaned_fact_transactions\
- payment_fact

Aggregates: - daily_sales_summary\
- product_performance\
- customer_lifetime_value\
- payment_success_rate

------------------------------------------------------------------------

# 🧠 Key Learnings

-   Medallion architecture in practice\
-   Config-driven pipelines\
-   Schema governance in Delta\
-   Partition overwrite strategies\
-   Star schema modeling\
-   Surrogate key generation\
-   Business-first data modeling\
-   Enterprise-style orchestration

------------------------------------------------------------------------

# 👤 Author

Vignesh S\
Aspiring Data Engineer\
PySpark • Databricks • SQL • Data Modeling
