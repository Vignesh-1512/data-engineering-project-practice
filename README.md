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

<img width="1024" height="1536" alt="ChatGPT Image Jan 8, 2026, 11_18_37 AM" src="https://github.com/user-attachments/assets/7d00b1d4-3229-4d00-8d4a-b3fc180562d3" />


---

##  Project Structure

<img width="1024" height="1536" alt="ChatGPT Image Jan 8, 2026, 11_33_25 AM" src="https://github.com/user-attachments/assets/d32c7514-0a4a-43ff-a5a0-083cba95b6b1" />


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


