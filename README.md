# Python Data Engineering Project (Databricks + Spark)

## Overview
This project demonstrates a **basic Data Engineering pipeline** using **Python and Apache Spark on Databricks**.  
The pipeline follows a **modular, job-based architecture**, inspired by real-world DE projects.

The pipeline supports:

- **Extract**: Read CSV files from Databricks Volumes or local data
- **Transform**: Apply SQL-based transformations using Spark
- **TEMP VIEWS only** (no database or permanent table storage)
- **Config-driven environment** (local vs Databricks automatically)
---

##  High-Level Architecture

<img width="1024" height="1536" alt="ChatGPT Image Jan 8, 2026, 11_18_37 AM" src="https://github.com/user-attachments/assets/7d00b1d4-3229-4d00-8d4a-b3fc180562d3" />


> ⚡ All transformations happen in memory via TEMP VIEWS. Fast iteration and zero dependency on external storage.
---

##  Project Structure

<img width="1024" height="1536" alt="ChatGPT Image Jan 8, 2026, 11_33_25 AM" src="https://github.com/user-attachments/assets/d32c7514-0a4a-43ff-a5a0-083cba95b6b1" />


---

## Configuration

All datasets and paths are defined in **YAML**:

```yaml
runtime:
  mode: databricks  # automatically detects local or databricks

datasets:
  events_databricks:
    table_name: "default.events_{process_date}"
    csv_path: "/Volumes/de_catalog/raw/shared_data"

  events_local:
    table_name: "default.events_{process_date}"
    csv_path: "data/raw"
```

- **`runtime.mode`** is automatically determined (Databricks or Local)
- The correct dataset config is selected automatically
- No manual switching required

  
---

## Entry Point

`main.py` is a **single unified entry point** for:

- Extract
- Transform
- Extract + Transform together

```python
from de_project.main import main

# Run extract only
main("events", "2026_01_07", "extract")

# Run transform only
main("events", "2026_01_07", "transform")

# Run both sequentially
main("events", "2026_01_07", "extract_transform")
```

---

## Extract Job

- Reads CSV from configured path
- Infers schema automatically
- Creates Spark **TEMP VIEW**
- Displays data in Spark UI / notebook

**Naming Convention**:

- CSV: `events_2026_01_07.csv`
- TEMP VIEW: `events_2026_01_07`

---

## Transform Job

- Reads from extract TEMP VIEW
- Applies SQL transformations
- Creates **transformed TEMP VIEW**

**Naming Convention**:

- Source: `events_2026_01_07`
- Target: `events_transformed_2026_01_07`

---

## Why TEMP VIEWS?

- Fast, in-memory processing
- No dependency on external tables or databases
- Ideal for **learning and prototyping**

---

## Building & Installing Wheel

1. **Install dependencies for building:**

```bash
pip install build
```

2. **Build the wheel:**

```bash
python -m build
```

- Output wheel file will appear in `dist/` folder

3. **Install wheel locally:**

```bash
pip install dist/de_project-0.1.0-py3-none-any.whl
```

4. **Run the project after installation:**

```bash
python -m de_project.main events 2026_01_07 extract
```

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



