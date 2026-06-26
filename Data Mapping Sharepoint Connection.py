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
    file_relative_url = "/sites/RI-CPMExcellence/Shared Documents/General/Dashboard/input/final_project_account_mapping.xlsx"

    credentials = ClientCredential(client_id, client_secret)
    ctx = ClientContext(site_url).with_credentials(credentials)

    # Download the file's raw bytes
    response = File.open_binary(ctx, file_relative_url)
    file_content = response.content

    # Load into a DataFrame
    df = pd.read_excel(io.BytesIO(file_content),sheet_name="Sheet1")

    print(df.head())

    # # If you want record-by-record access like SharePoint list items:
    # records = df.to_dict(orient="records")
    # for record in records:
    #     print(record)

except Exception as e:
    print(f"Connection establishment with Sharepoint failed with error: {e}")

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import col

# Target column names, in order
target_columns = [
    "psa_project_id",
    "psa_project_name",
    "snow_account_id",
    "reqtest_project_id",
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

# Coerce types (pandas-side cleanup)
string_columns = ["psa_project_id", "psa_project_name", "snow_account_id"]
int_columns = ["reqtest_project_id"]

for col_name in string_columns:
    df[col_name] = df[col_name].astype("string")

for col_name in int_columns:
    df[col_name] = pd.to_numeric(df[col_name], errors="coerce").astype("Int64")

print(df.head())
print(df.dtypes)

# Convert pandas DataFrame to Spark DataFrame
spark_df = spark.createDataFrame(df)

# Force columns to match the Delta table schema exactly
for col_name in string_columns:
    spark_df = spark_df.withColumn(col_name, col(col_name).cast(StringType()))

for col_name in int_columns:
    spark_df = spark_df.withColumn(col_name, col(col_name).cast(IntegerType()))

spark_df.printSchema()  # sanity check — confirm columns show as 'string'/'integer'

# Append to the existing table
spark_df.write.mode("append").saveAsTable("qa_wb.saasfactory.g_dim_cpm_project_acct_mapping_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from qa_wb.saasfactory.g_dim_cpm_project_acct_mapping_new

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct reqtest_project_id from qa_wb.saasfactory.g_dim_cpm_project_acct_mapping_new