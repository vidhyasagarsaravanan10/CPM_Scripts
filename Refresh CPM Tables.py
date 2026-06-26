# Databricks notebook source
# DBTITLE 1,Problem Table
# Databricks notebook source
from pyspark.sql.functions import col

df_incident = spark.table("prod_l2.services.tbl_ocs_ei_problem")

result = df_incident.select(col("problem_number"),\
    col("company_id"),\
    col("priority"),\
    col("opened_on_timestamp"),\
    col("resolved_on_timestamp"),\
    col("closed_on_timestamp"))

result.write.format("delta").mode("overwrite").saveAsTable("qa_wb.saasfactory.g_fct_cpm_problem")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- show create table qa_wb.saasfactory.g_fct_cpm_incident
# MAGIC select * from qa_wb.saasfactory.g_fct_cpm_problem

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,PSA 2 Tables
# Databricks notebook source
from pyspark.sql.functions import split, col, when, lit, sum, max, to_timestamp, datediff, current_date, year, to_date, current_timestamp, concat_ws
from pyspark.sql.types import DecimalType

# Read source tables
opp = spark.table("prod_l2.x360.opportunity_header")
pd = spark.table("prod_l2.services.tbl_ocs_ps_project")
po = spark.table("prod_l2.x360.project_order")
rate = spark.table("prod_l2.foundation.fordh_exch_exchange_rate_aop_tbl")
pse_psa = spark.table("prod_l2.services.sfdc_psa_pse__proj__c")
practice = spark.table("prod_l2.services.tbl_ocs_ps_practice")
region = spark.table("prod_l2.services.tbl_ocs_ps_region")

# Join source tables
joined = (
    pd.join(
        opp,
        (opp.OpportunityID == pd.opportunity_id),
        "left"
    )
    .join(
        po, 
        po.Project_ID == pd.project_id,
        "left"
    )
    .join(
        region, 
        region.region_id == pd.region_id,
        "left"
    )
    .join(
        rate,
        split(po.Project_Order_Net_Value, "\\*")[1] == rate.ISOCode,
        "left"
    )
    .join(
        practice,
        practice.practice_id == pd.practice_id,
        "left"
    )
)

# COMMAND ----------

# Calculate Net_Value
net_value = sum(split(col("Project_Order_Net_Value"), "\\*").getItem(0).cast("decimal(18,2)"))
curr = when((split(col("Project_Order_Net_Value"), "\\*").getItem(1) == "EUR") | (split(col("Project_Order_Net_Value"), "\\*").getItem(1).isNull()), lit(1)).otherwise(col("EUR"))
quotation_per_qty = when(col("QuotationPer").isNotNull(), col("QuotationPer")).otherwise(lit(1))

# Prepare select columns
result = (
    joined.filter(
        ((col("Practice_Name") == "RI") | (col("Practice_Name") == "HCIS")) &
        (col("Region_ID_Chain").like("%a3Y0W000002FJCAUA4,a3Yd00000008epbEAA"))  &
        (col("StageName") == "Order Booked") & (col("solid_complexity_level").isin(["Advanced", "Integrated"])))
    .groupBy(
        pd["Project_ID"],
        pd["Project_Name"],
        pd["site_name"], #Project_Site_ID
        pd["Project_Status"],
        pd["stage"],
        pd["Project_Number"],
        pd["Project_Record_Type"],
        pd["sow_final_available"],
        pd["Region_Name"],
        pd["country_name"],
        pd["Project_Milestone"],
        pd["planned_htd_date"],
        pd["latest_htd_date"],
        pd["planned_uat_date"],
        pd["latest_uat_date"],
        pd["planned_gld_date"],
        pd["latest_gld_date"],
        pd["planned_tts_date"],
        pd["latest_tts_date"],
        pd["actual_htd_date"],
        pd["actual_gld_date"],
        pd["actual_tts_date"],
        pd["project_manager_name"], 
        pd["planned_hours"],
        opp["Expected_Order_Date"],
        opp["StageName"],
        opp["OpportunityId"],
        pd["actual_uat_date"],
        pd["is_active"],
        pd["actual_pla_date"],
        pd["planned_pla_date"],
        pd["latest_pla_date"],
        pd["end_date"],
        pd["start_date"],
        pd["parent_project_id"],
        pd["is_milestone_relevant"],
        pd["opportunity_account_name"],
        pd["project_status_notes"],
        pd["domain"]
    )
    .agg(net_value.alias("curr_Net_Value"), max(curr).alias("currency"), max(quotation_per_qty).alias("QuotationPerQuote"), max("ISOCode").alias("Currency_Code"))
)

result = result.withColumn("Net_Value", ((col("curr_Net_Value")/col("QuotationPerQuote")) * col("currency")))

# COMMAND ----------

result = result\
    .withColumnRenamed("stage", "Project_Stage")\
    .withColumnRenamed("sow_final_available", "Project_SOW_Final_Available")\
    .withColumnRenamed("country_name", "Project_Country")\
    .withColumnRenamed("planned_htd_date", "Project_HTD_Planned_Date")\
    .withColumnRenamed("actual_htd_date", "Project_HTD_Actual_Date")\
    .withColumnRenamed("latest_htd_date", "Project_Latest_HTD_Date")\
    .withColumnRenamed("actual_gld_date", "Project_GLD_Actual_Date")\
    .withColumnRenamed("latest_gld_date", "Project_Latest_GLD_Date")\
    .withColumnRenamed("planned_gld_date", "Project_GLD_Planned_Date")\
    .withColumnRenamed("actual_pla_date", "Project_PLA_Actual_Date")\
    .withColumnRenamed("latest_pla_date", "Project_Latest_PLA_Date")\
    .withColumnRenamed("planned_pla_date", "Project_PLA_Planned_Date")\
    .withColumnRenamed("planned_tts_date", "Project_TTS_Planned_Date")\
    .withColumnRenamed("actual_tts_date", "Project_TTS_Actual_Date")\
    .withColumnRenamed("latest_tts_date", "Project_Latest_TTS_Date")\
    .withColumnRenamed("actual_uat_date", "Project_UAT_Actual_Date")\
    .withColumnRenamed("planned_uat_date", "Project_UAT_Planned_Date")\
    .withColumnRenamed("latest_uat_date", "Project_Latest_UAT_Date")\
    .withColumnRenamed("start_date", "Project_Start_Date")\
    .withColumnRenamed("end_date", "Project_End_Date")\
    .withColumnRenamed("project_manager_name", "Contact_Name")\
    .withColumnRenamed("planned_hours", "Project_Planned_Hours")\
    .withColumnRenamed("is_active", "Project_Is_Active")\
    .withColumnRenamed("site_name", "Project_Site_ID")\
    .withColumnRenamed("parent_project_id", "Project_Parent_Project")\
    .withColumnRenamed("is_milestone_relevant", "Not_Milestone_Relevant")\
    .withColumnRenamed("opportunity_account_name", "Project_Account_Name")\
    .withColumn("Project", concat_ws(", ", col("Project_Number"), col("Project_Name")))\
    .withColumn("Project_HTD_Planned_Date", to_date("Project_HTD_Planned_Date"))\
    .withColumn("Project_Latest_HTD_Date", to_date("Project_Latest_HTD_Date"))\
    .withColumn("Project_UAT_Planned_Date", to_date("Project_UAT_Planned_Date"))\
    .withColumn("Project_Latest_UAT_Date", to_date("Project_Latest_UAT_Date"))\
    .withColumn("Project_GLD_Planned_Date", to_date("Project_GLD_Planned_Date"))\
    .withColumn("Project_Latest_GLD_Date", to_date("Project_Latest_GLD_Date"))\
    .withColumn("Project_TTS_Planned_Date", to_date("Project_TTS_Planned_Date"))\
    .withColumn("Project_Latest_TTS_Date", to_date("Project_Latest_TTS_Date"))\
    .withColumn("Project_HTD_Actual_Date", to_date("Project_HTD_Actual_Date"))\
    .withColumn("Project_GLD_Actual_Date", to_date("Project_GLD_Actual_Date"))\
    .withColumn("Project_TTS_Actual_Date", to_date("Project_TTS_Actual_Date"))\
    .withColumn("Expected_Order_Date", to_date("Expected_Order_Date"))\
    .withColumn("Project_UAT_Actual_Date", to_date("Project_UAT_Actual_Date"))\
    .withColumn("Project_PLA_Actual_Date", to_date("Project_PLA_Actual_Date"))\
    .withColumn("Project_PLA_Planned_Date", to_date("Project_PLA_Planned_Date"))\
    .withColumn("Project_Latest_PLA_Date", to_date("Project_Latest_PLA_Date"))\
    .withColumn("Project_End_Date", to_date("Project_End_Date"))\
    .withColumn("Project_Start_Date", to_date("Project_Start_Date"))\
    .withColumn("SOW_Status", when(col("Project_SOW_Final_Available") == "Available, and approved by Customer", lit("On Target")).when(col("Project_SOW_Final_Available").isNull(), lit("Missing")).otherwise(lit("Off Target")))\
    .withColumn("Project_Status", when(col("Project_Status").isNotNull(), col("Project_Status")).otherwise(lit("Missing")))\
    .withColumn("Project_Milestone", when(col("Project_Milestone").isNotNull(), col("Project_Milestone")).otherwise(lit("Missing")))\
    .withColumn("HTD_Status", when(col("Project_HTD_Actual_Date").isNull(), lit("Not Started")).when(col("Project_HTD_Actual_Date") <= col("Project_HTD_Planned_Date"), lit("On Time")).when(col("Project_HTD_Planned_Date").isNull(), lit("Not Planned")).otherwise(lit("Delayed")))\
    .withColumn("UAT_Status", when(col("Project_UAT_Actual_Date").isNull(), lit("Not Started")).when(col("Project_UAT_Actual_Date") <= col("Project_UAT_Planned_Date"), lit("On Time")).when(col("Project_UAT_Planned_Date").isNull(), lit("Not Planned")).otherwise(lit("Delayed")))\
    .withColumn("GLD_Status", when(col("Project_GLD_Actual_Date").isNull(), lit("Not Started")).when(col("Project_GLD_Actual_Date") <= col("Project_GLD_Planned_Date"), lit("On Time")).when(col("Project_GLD_Planned_Date").isNull(), lit("Not Planned")).otherwise(lit("Delayed")))\
    .withColumn("TTS_Status", when(col("Project_TTS_Actual_Date").isNull(), lit("Not Started")).when(col("Project_TTS_Actual_Date") <= col("Project_TTS_Planned_Date"), lit("On Time")).when(col("Project_TTS_Planned_Date").isNull(), lit("Not Planned")).otherwise(lit("Delayed")))\
    .withColumn("Net_Value",col("Net_Value").cast(DecimalType(10, 0)))\
    .withColumn("Project_Planned_Hours",col("Project_Planned_Hours").cast(DecimalType(10, 0)))\
    .withColumn("HTD_Lead_Time",datediff(col("Project_HTD_Actual_Date"),col("Expected_Order_Date")))\
    .withColumn("TTS_Lead_Time",datediff(col("Project_TTS_Actual_Date"),col("Project_GLD_Actual_Date")))\
    .withColumn("Is_In_Hypercare",when((col("Project_GLD_Actual_Date") < current_date()) &\
         (col("Project_TTS_Actual_Date").isNull() | (col("Project_TTS_Actual_Date") > current_date())),lit("Yes")
        ).otherwise(lit("No")))\
    .withColumn("UAT_Risk",datediff(col("Project_GLD_Planned_Date"),col("Project_UAT_Planned_Date")))\
    .withColumn("Project_Region", when(col("Project_Country").isin(["Germany", "Austria", "Switzerland","Netherlands", "Belgium"]), lit("Central Europe")).when(col("Project_Country").isin(["Jersey", "United Kingdom", "Ireland","Gibraltar", "Finland", "Sweden", "Norway", "Iceland", "Denmark"]), lit("North Europe")).when(col("Project_Country").isin(["Portugal", "Andorra", "Spain", "France", "Réunion", "Holy See (Vatican City State)", "Italy", "Cyprus", "Israel","Greece"]), lit("South Europe")))\
    .withColumnRenamed("Contact_Name", "Contact_Manager")\
    .withColumnRenamed("StageName", "OpportunityStageName")\
    .withColumn("load_timestamp", current_timestamp())\
    .withColumn("isActive", lit(True))\
    .withColumnRenamed("project_status_notes", "Project_Status_Notes")\
    .withColumnRenamed("domain", "Domain")
    

# COMMAND ----------

result = result.select(
        col("Project_ID"),
        col("Project_Name"),
        col("Project_Site_ID"),
        col("Project_Status"),
        col("Project_Stage"),
        col("Project_Number"),
        col("Project_Record_Type"),
        col("Project_SOW_Final_Available"),
        col("Region_Name"),
        col("Project_Country"),
        col("Project_Milestone"),
        col("Project_HTD_Planned_Date"),
        col("Project_Latest_HTD_Date"),
        col("Project_UAT_Planned_Date"),
        col("Project_Latest_UAT_Date"),
        col("Project_GLD_Planned_Date"),
        col("Project_Latest_GLD_Date"),
        col("Project_TTS_Planned_Date"),
        col("Project_Latest_TTS_Date"),
        col("Net_Value"),
        col("Project_HTD_Actual_Date"),
        col("Project_GLD_Actual_Date"),
        col("Project_TTS_Actual_Date"),
        col("Contact_Manager"),
        col("SOW_Status"),
        col("Project_Planned_Hours"),
        col("Expected_Order_Date"),
        col("OpportunityStageName"),
        col("OpportunityId"),
        col("Project_UAT_Actual_Date"),
        col("HTD_Status"),
        col("UAT_Status"),
        col("GLD_Status"),
        col("TTS_Status"),
        col("HTD_Lead_Time"),
        col("TTS_Lead_Time"),
        col("Project_Is_Active"),
        col("Is_In_Hypercare"),
        col("Project_PLA_Actual_Date"),
        col("Project_PLA_Planned_Date"),
        col("Project_Latest_PLA_Date"),
        col("Project_End_Date"),
        col("Project_Start_Date"),
        col("Project_Parent_Project"),
        col("Not_Milestone_Relevant"),
        col("Project_Account_Name"),
        col("curr_Net_Value"),
        col("Currency_Code"),
        col("Project_Region"),
        col("UAT_Risk"),
        col("load_timestamp"),
        col("isActive"),
        col("Project"),
        col("Project_Status_Notes"),
        col("Domain")
    )

# COMMAND ----------

# Insert into destination table
result.write.format("delta").mode("overwrite").saveAsTable("qa_wb.saasfactory.g_fct_cpm_project_delivery")
result.write.format("delta").mode("append").saveAsTable("qa_wb.saasfactory.g_fct_cpm_project_delivery_hist")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from qa_wb.saasfactory.g_fct_cpm_project_delivery

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Incident
# Databricks notebook source
# MAGIC %sql
# MAGIC -- select * from qa_wb.saasfactory.g_fct_cpm_incident
# MAGIC -- alter table qa_wb.saasfactory.g_fct_cpm_incident add column is_active boolean

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, md5, concat_ws
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from delta.tables import DeltaTable

# Step 1: Read source
incident = spark.table("prod_l2.services.tbl_ocs_ei_incident")

# Step 2: Build fresh result
result = (
    incident.select(
        col("incident_number"),
        col("close_notes"),
        col("priority"),
        col("contact_type"),
        col("opened_on_timestamp"),
        col("resolved_on_timestamp"),
        col("company"),
        col("is_active")
    )
    .withColumn("load_date", current_date())
)

# Step 3: Overwrite current incident table (always latest snapshot)
result.write.format("delta").mode("overwrite").saveAsTable("qa_wb.saasfactory.g_fct_cpm_incident")

# # Step 4: CDC — append only new/changed records to hist

# # Columns to track for changes
# compare_cols = ["close_notes", "priority", "contact_type", 
#                 "opened_on_timestamp", "resolved_on_timestamp", 
#                 "company"]

# # Hash new result
# result_hashed = result.withColumn(
#     "row_hash", md5(concat_ws("||", *[col(c).cast("string") for c in compare_cols]))
# )

# # Get hist table
# hist = spark.table("qa_wb.saasfactory.g_fct_cpm_incident_hist")

# # Hash hist and keep only latest record per incident
# hist_hashed = hist.withColumn(
#     "row_hash", md5(concat_ws("||", *[col(c).cast("string") for c in compare_cols]))
# )

# window = Window.partitionBy("incident_number").orderBy(col("load_date").desc())
# hist_latest = hist_hashed \
#     .withColumn("rn", row_number().over(window)) \
#     .filter(col("rn") == 1) \
#     .drop("rn")

# # Find new records — not in hist at all
# new_records = result_hashed.join(
#     hist_latest.select("incident_number"),
#     on="incident_number",
#     how="left_anti"
# ).drop("row_hash")

# # Find changed records — hash differs from latest hist record
# changed_records = result_hashed.alias("new").join(
#     hist_latest.select("incident_number", "row_hash").alias("old"),
#     on="incident_number",
#     how="inner"
# ).filter(
#     col("new.row_hash") != col("old.row_hash")
# ).select(
#     col("new.incident_number"),
#     col("new.close_notes"),
#     col("new.priority"),
#     col("new.contact_type"),
#     col("new.opened_on_timestamp"),
#     col("new.resolved_on_timestamp"),
#     col("new.company"),
#     col("new.is_active"),
#     col("new.load_date")
# )

# print(f"New records: {new_records.count()}")
# print(f"Changed records: {changed_records.count()}")

# # Append only new + changed to hist
# new_records.union(changed_records) \
#     .write.format("delta").mode("append") \
#     .saveAsTable("qa_wb.saasfactory.g_fct_cpm_incident_hist")

# print("Done.")

# COMMAND ----------

# from pyspark.sql.functions import col

# df_incident = spark.table("prod_l2.services.tbl_ocs_ei_incident")

# result = df_incident.select(col("company"),\
#     col("incident_number"),\
#     col("close_notes"),\
#     col("priority"),\
#     col("opened_on_timestamp"),\
#     col("resolved_on_timestamp"),\
#     col("is_active"))

# result.write.format("delta").mode("overwrite").saveAsTable("qa_wb.saasfactory.g_fct_cpm_incident")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from qa_wb.saasfactory.g_fct_cpm_incident
# MAGIC -- select * from prod_l2.services.tbl_ocs_ei_incident

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from qa_wb.saasfactory.g_fct_cpm_incident

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from qa_wb.saasfactory.g_fct_cpm_incident_hist

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM qa_wb.saasfactory.g_fct_cpm_incident_hist 
# MAGIC WHERE DATE(load_timestamp) = CURRENT_DATE();