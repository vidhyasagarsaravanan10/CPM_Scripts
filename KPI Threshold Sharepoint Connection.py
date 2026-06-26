# Databricks notebook source
pip install office365-rest-python-client --trusted-host pypi.org --trusted-host files.pythonhosted.org openpyxl

# COMMAND ----------

import io
import pandas as pd
from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File

try:
    tenant_id = ""
    client_id = ""
    client_secret = ""

    site_url = "https://share.philips.com/sites/RI-CPMExcellence" 
    file_relative_url = "/sites/RI-CPMExcellence/Shared Documents/General/Dashboard/input/kpi_thresholds.xlsx"

    credentials = ClientCredential(client_id, client_secret)
    ctx = ClientContext(site_url).with_credentials(credentials)

    # Download the file's raw bytes
    response = File.open_binary(ctx, file_relative_url)
    file_content = response.content

    # Load into a DataFrame
    df = pd.read_excel(io.BytesIO(file_content),sheet_name="Thresholds")

    print(df.head())


except Exception as e:
    print(f"Connection establishment with Sharepoint failed with error: {e}")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE qa_wb.saasfactory.g_dim_cpm_kpi_thresholds          

# COMMAND ----------

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col

# Target column names, in order
target_columns = [
    "high_risk_projects",
    "sow",
    "high_value_projects_dasb",
    "htd_lead_time",
    "htd_rework",
    "htd_completion",
    "htd_readiness",
    "htd_not_ready",
    "uat_pass_rate",
    "uat_completion",
    "uat_rework",
    "uat_at_risk",
    "gld_ticket_threshold_p1",
    "gld_ticket_threshold_p2",
    "go_live_success_ratio",
    "hypercare_incident_p1",
    "hypercare_incident_p2",
    "tts_lead_time",
    "tts_completion",
    "billable_utilization",
    "nps",
    "extended_hypercare_cases",
]

# Sanity check before blindly renaming by position
if len(df.columns) != len(target_columns):
    raise ValueError(
        f"Column count mismatch: df has {len(df.columns)} columns, "
        f"expected {len(target_columns)}. "
        f"df columns: {list(df.columns)}"
    )

# Rename by position
df.columns = target_columns

# Coerce to numeric, NaN-safe (still pandas-side cleanup)
for col_name in target_columns:
    df[col_name] = pd.to_numeric(df[col_name], errors="coerce").astype("Int64")

print(df.head())
print(df.dtypes)

# Convert pandas DataFrame to Spark DataFrame
spark_df = spark.createDataFrame(df)

# Force every target column to 32-bit int to match the Delta table exactly
for col_name in target_columns:
    spark_df = spark_df.withColumn(col_name, col(col_name).cast(IntegerType()))

spark_df.printSchema()  # sanity check — confirm all columns now show as 'integer'

# Append to the existing table
spark_df.write.mode("append").saveAsTable("qa_wb.saasfactory.g_dim_cpm_kpi_thresholds")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from qa_wb.saasfactory.g_dim_cpm_kpi_thresholds