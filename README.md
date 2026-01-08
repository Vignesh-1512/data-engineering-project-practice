#  Python Data Engineering Project (Databricks + Spark)

##  Overview
This project demonstrates a **basic Data Engineering pipeline** using **Python and Apache Spark on Databricks**.  
The pipeline follows a **modular, job-based architecture** inspired by real-world DE projects.

The current implementation supports:
- **Extract**: Read CSV files from Databricks Volumes
- **Transform**: Apply SQL-based transformations using Spark
- **No database / table persistence** (only TEMP VIEWS are used)

---

##  High-Level Architecture

CSV Files (Databricks Volume)
        |
        v
Extract Job (Spark Read CSV)
        |
        v
TEMP VIEW (Raw Data)
events_YYYY_MM_DD
        |
        v
Transform Job (Spark SQL)
        |
        v
TEMP VIEW (Transformed Data)
events_transformed_YYYY_MM_DD

---

##  Project Structure

de_project/
│
├── src/de_project/
│   ├── main.py
│   ├── jobs/
│   │   ├── extract_job.py
│   │   └── transform_job.py
│   ├── utils/
│   │   └── config_loader.py
│   └── config/
│       └── app_config.yaml
│
├── pyproject.toml
├── README.md

---

##  Configuration

All dataset-level configuration is defined in YAML.

```yaml
datasets:
  events:
    csv_path: "/Volumes/de_catalog/raw/default"
```

---

##  Entry Point

`main.py` acts as a unified entry point for extract and transform jobs.

---

##  Extract Job

- Reads CSV from Databricks Volume
- Infers schema
- Creates Spark TEMP VIEW
- Displays data

Naming:
- CSV → events_2026_01_07.csv
- View → events_2026_01_07

---

##  Transform Job

- Reads from extract TEMP VIEW
- Applies SQL transformations
- Creates transformed TEMP VIEW

Naming:
- Source → events_2026_01_07
- Target → events_transformed_2026_01_07

---

##  Why TEMP VIEWS?

- Fast iteration
- No storage dependency
- Ideal for learning and prototyping

---

##  How to Run

```python
from de_project.main import main
main("events", "2026_01_07", "extract")
main("events", "2026_01_07", "transform")
```

---

##  Author
Vignesh S
