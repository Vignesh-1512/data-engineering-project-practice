
# Brazilian E-Commerce Data Engineering Project
**Databricks • PySpark • Medallion Architecture**

## 📌 Overview
This project demonstrates a real-world **Data Engineering pipeline** built using **Python and Apache Spark on Databricks**, following industry-standard **Medallion Architecture** and **config-driven execution**.

The pipeline processes the **Brazilian E-Commerce (Olist) dataset** end-to-end and delivers **business-ready analytics marts** aligned with real business requirements (**BR-1 to BR-7**).

### 🔑 Key Highlights
- Fully **config-driven (YAML)**
- **Single entry point** to run any layer
- Dynamic table resolution using **catalog & schema**
- Reusable utilities (ingest, refine, model, mart)
- Production-style error handling
- Designed for **Databricks Unity Catalog**
- **ROW-count & data-quality validations** at each layer

---

## 🧱 Architecture (Medallion)


<img width="1024" height="1536" alt="ChatGPT Image Jan 8, 2026, 11_18_37 AM" src="https://github.com/user-attachments/assets/669ce17f-acec-4843-bec0-7b970d9dbd05" />



---

## 📂 Project Structure


<img width="1024" height="1536" alt="ChatGPT Image Feb 2, 2026, 05_22_10 PM" src="https://github.com/user-attachments/assets/b4322eba-7a0b-4109-8d8e-ada1b550a5cc" />



---

## ⚙️ Configuration (YAML-Driven)

All table definitions, schemas, load types, and business logic dependencies are defined in `tables.yaml`.

### Example – Bronze Configuration
```yaml
bronze:
  orders:
    catalog: brazillian_e_commerce
    source_schema: source
    target_schema: bronze
    table_name: orders
    load_type: incremental
    watermark_column: order_purchase_timestamp
```

### ✅ Why this matters
- No hardcoded table names in code
- Switching source systems requires **YAML change only**
- Supports **Unity Catalog** & multi-environment setups
- Enables **repeatable and auditable pielines**

---

## 🚀 Entry Point (main.py)

A single unified entry point controls the entire pipeline.

```python
from brazillian_e_commerce.main import run

run("bronze")                         # Full bronze ingestion
run("silver", "orders")               # Single silver table
run("gold")                           # Gold layer
run("mart", "seller_performance")     # Specific business requirement
```

### Supported Layers
- `bronze` → Ingest
- `silver` → Refine
- `gold`   → Model
- `mart`   → Business analytics

---

## 🟫 Bronze Layer (Ingest)
**Purpose:** Raw ingestion with minimal transformation.

**Features**
- Full & incremental loads
- Watermark-based ingestion
- Metadata enrichment
- Append vs overwrite handled dynamically
- Bronze table treated as **immutable source of truth**

### ⏱ Dynamic Incremental Ingestion
Incremental ingestion is handled dynamically using watermark columns defined in YAML.

- Each table defines its own `watermark_column`
- The pipeline automatically fetches the **last processed timestamp**
- Only new or changed records are ingested on subsequent runs
- Full vs incremental behavior is resolved at runtime

This allows the same codebase to handle:
- First-time full loads
- Subsequent incremental loads
- Table-specific ingestion strategies

```python
run("bronze")
run("bronze", "orders")
```

---

## ⚪ Silver Layer (Refine)
**Purpose:** Data quality & standardization.

**Operations**
- Type casting
- Column renaming
- Null handling
- Schema stabilization
- Deterministic transformation
  
### 🔄 Delta MERGE Strategy (Silver Layer)
Silver tables are written using **Delta Lake MERGE** to support incremental refinement.

- MERGE is based on **business keys** (e.g., `order_id`)
- Supports idempotent re-runs
- Prevents duplicate inserts during incremental loads
- Updates existing records when source data changes

> Note: Silver MERGE updates/inserts records.  
> Row-level deletes are applied only when explicitly defined.

This ensures Silver remains **incremental, replayable, and production-safe**.

**🔍 Data Quality Guarantees**
- **Row-count validation performed on tables(not dataframes)**
- Explicit checks for **business-key uniqueness**
- silver tables are safe to rebuild, bronze is never modified
  
```python
run("silver")
run("silver", "payments")
```

---

## 🟨 Gold Layer (Model)
**Purpose:** Business-ready dimensional modeling.

### Dimensions
- dim_customers
- dim_products
- dim_sellers
- dim_date

### Facts
- fact_orders
- fact_sales
- fact_reviews
- fact_payments

**Design Principles**
- Clear grain definition
- No accidental row multiplication
- Facts validations against silver counts
  
```python
run("gold")
run("gold", "fact_sales")
```

---

## 🟦 Mart Layer (Business Requirements)
Final analytical tables answering real business questions.

| BR | Mart Table | Description |
|----|-----------|-------------|
| BR-1 | sales_performance | Sales trend & revenue |
| BR-2 | order_delivery_summary | Delivery performance |
| BR-3 | customer_analytics | Customer behavior |
| BR-4 | seller_performance | Seller KPIs |
| BR-5 | product_category_performance | Category trends |
| BR-6 | payment_analytics | Payment insights |
| BR-7 | customer_satisfaction_and_reviews | Reviews & satisfaction |

```python
run("mart")
run("mart", "payment_analytics")
```

---

## ❗ Error Handling

Custom, user-friendly exceptions:
```python
class PipelineException(Exception): ...
class ConfigError(PipelineException): ...
class DataReadError(PipelineException): ...
class DataWriteError(PipelineException): ...
class TransformationError(PipelineException): ...
```

**Example**
```
[Bronze] Failed reading source table 'orders'
Reason: Table not found
```

---

## 🧪 Debugging & Observability
- Strategic `print()` statements
- Row counts before & after transformations
- Clear source → target visibility
- cache cleared between rebuilds when required
- Beginner & reviewer friendly yet **production-realistic**

---
### 📊 Table-Level Validation
To avoid misleading metrics, row counts are always validated **after data is written to Delta tables**, not on intermediate DataFrames.

- Prevents false positives during MERGE-based pipelines
- Ensures table state reflects actual data
- Used consistently across Silver and Gold layers

This mirrors real-world production validation practices.

---

## 🛠 Build & Install (Wheel)

```bash
pip install build
python -m build
pip install dist/brazillian_e_commerce-0.1.0-py3-none-any.whl
```

---

## 🧠 Key Learnings
- Medallion architecture in practice
- Config-driven pipelines
- Incremental ingestion patterns
- Delta Lake behaviour (MERGE vs Overwrite)
- importance of **table-level-validation**
- Dimensional modeling fundamentals
- Business-first data design
- Production-grade structure & debugging mindset

---

## 👤 Author
**Vignesh S**  
Aspiring Data Engineer | PySpark | Databricks | SQL
