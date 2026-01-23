# Databricks notebook source
# MAGIC %pip uninstall -y de_project

# COMMAND ----------

# MAGIC %pip install /Workspace/Shared/data/de_project-0.1.0-py3-none-any.whl

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

dbutils.library.restartPython() 

# COMMAND ----------

from de_project.main import main
import inspect
print(inspect.signature(main))


# COMMAND ----------

from de_project.main import main
print(main)

# COMMAND ----------

dbutils.widgets.text("dataset", "events")
dbutils.widgets.text("process_date", "2026_01_05")

dataset = dbutils.widgets.get("dataset")
process_date = dbutils.widgets.get("process_date")

# COMMAND ----------

params = {
    "dataset": "events",
    "process_date": "2026_01_05",
    "job": "transform"
}

# COMMAND ----------

from de_project.main import main
main(**params)




# COMMAND ----------

dbutils.fs.ls("/Volumes/de_catalog/raw/shared_data")


# COMMAND ----------

