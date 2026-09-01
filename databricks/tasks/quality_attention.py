# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow Task: Quality Attention Alert Handler
# MAGIC **Task Key:** `quality_attention`
# MAGIC Executed on condition: `quarantine_alert_triggered == true`.
# MAGIC Records quarantine attention status and alerts operations without aborting the downstream pipeline.

# COMMAND ----------

import logging

logger = logging.getLogger(__name__)

# COMMAND ----------

dbutils.widgets.text("environment", "dev", "Environment")
dbutils.widgets.text("quarantine_rate", "0.0", "Quarantine Rate")
dbutils.widgets.text("quarantine_count", "0", "Quarantine Count")

env = dbutils.widgets.get("environment")

# Retrieve values from upstream task values if available, otherwise fallback to widget
try:
    quarantine_rate = float(dbutils.jobs.taskValues.get(taskKey="silver_transformation", key="quarantine_rate"))
except Exception:
    quarantine_rate = float(dbutils.widgets.get("quarantine_rate") or "0.0")

try:
    quarantine_count = int(dbutils.jobs.taskValues.get(taskKey="silver_transformation", key="silver_quarantine_rows"))
except Exception:
    quarantine_count = int(dbutils.widgets.get("quarantine_count") or "0")

print("=" * 70)
print(f"⚠️  QUALITY ATTENTION ALERT (Environment: {env.upper()})")
print(f"  Quarantine Rate  : {quarantine_rate * 100:.2f}%")
print(f"  Quarantined Rows : {quarantine_count}")
print("  Action           : Non-fatal alert recorded for operational monitoring.")
print("=" * 70)

try:
    dbutils.jobs.taskValues.set(key="quality_attention_required", value=True)
    dbutils.jobs.taskValues.set(key="quarantine_alert_logged", value=True)
except Exception as e:
    print(f"Task values set locally: {e}")
