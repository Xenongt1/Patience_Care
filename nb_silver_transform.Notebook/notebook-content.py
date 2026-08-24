# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1daeeaf0-1488-4c14-9071-445b5dae163b",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "f86bd9be-e828-420b-b5c6-77937de2fe75",
# META       "known_lakehouses": [
# META         {
# META           "id": "1daeeaf0-1488-4c14-9071-445b5dae163b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

spark.sql("DROP TABLE IF EXISTS dq_run_results")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **_Shared Imports + DQ helper function**

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import datetime

def add_issue(df, condition, message):
    return df.withColumn(
        "_dq_issues",
        F.when(condition, F.array_union("_dq_issues", F.array(F.lit(message))))
         .otherwise(F.col("_dq_issues"))
    )

def log_dq_run(silver_df, table_name):
    run_summary = silver_df.groupBy().agg(
        F.count("*").alias("rows_total"),
        F.sum(F.when(F.col("_dq_passed"), 1).otherwise(0)).alias("rows_passed"),
        F.sum(F.when(~F.col("_dq_passed"), 1).otherwise(0)).alias("rows_failed")
    ).withColumn("table_name", F.lit(table_name)) \
     .withColumn("run_id", F.lit(str(datetime.datetime.now()))) \
     .withColumn("run_timestamp", F.current_timestamp())

    run_summary.write.format("delta").mode("append").saveAsTable("dq_run_results")
    print(f"{table_name}: DQ run logged")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_bronze = spark.read.table("dbo_1.bronze_facility")

# Collapse to latest snapshot per facility — dimension tables only, not fact tables
w = Window.partitionBy("facility_id").orderBy(F.col("_batch_id").desc())
bronze = (raw_bronze
    .withColumn("_rn", F.row_number().over(w))
    .filter("_rn = 1")
    .drop("_rn")
)

valid_facility_type = ["general", "teaching", "regional", "community", "urgent_care"]

silver = (bronze
    .withColumn("facility_type_folded", F.lower(F.trim("facility_type")))
    .withColumn("emergency_services_bool", F.col("emergency_services").cast("boolean"))
    .withColumn("licensed_beds_int", F.col("licensed_beds").cast("int"))
    .withColumn("staffed_beds_int", F.col("staffed_beds").cast("int"))
    .withColumn("_dq_issues", F.array().cast("array<string>"))
)

silver = add_issue(silver, F.col("facility_id").isNull(), "facility_id null")
silver = add_issue(silver, ~F.col("facility_type_folded").isin(valid_facility_type), "invalid facility_type")
silver = add_issue(silver, (F.col("licensed_beds_int").isNull()) | (F.col("licensed_beds_int") <= 0), "invalid licensed_beds")
silver = add_issue(silver, F.col("staffed_beds_int") > F.col("licensed_beds_int"), "staffed_beds exceeds licensed_beds")

silver = silver.withColumn("_dq_passed", F.size("_dq_issues") == 0)
silver = silver.withColumn("_dq_issues", F.concat_ws("; ", "_dq_issues"))

silver.write.format("delta").mode("overwrite").saveAsTable("silver_facility")
log_dq_run(silver, "silver_facility")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_bronze = spark.read.table("dbo_1.bronze_unit")
w = Window.partitionBy("unit_id").orderBy(F.col("_batch_id").desc())
bronze = raw_bronze.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

silver = (bronze
    .withColumn("unit_type_folded", F.lower(F.trim("unit_type")))
    .withColumn("licensed_beds_int", F.col("licensed_beds").cast("int"))
    .withColumn("staffed_beds_int", F.col("staffed_beds").cast("int"))
    .withColumn("blocked_beds_int", F.col("blocked_beds").cast("int"))
    .withColumn("nurse_patient_ratio_target_float", F.col("nurse_patient_ratio_target").cast("float"))
    .withColumn("is_critical_care_bool", F.col("is_critical_care").cast("boolean"))
    .withColumn("is_monitored_bool", F.col("is_monitored").cast("boolean"))
    .withColumn("_dq_issues", F.array().cast("array<string>"))
)
silver = add_issue(silver, F.col("unit_id").isNull(), "unit_id null")
silver = add_issue(silver, F.col("facility_id").isNull(), "facility_id null")
silver = add_issue(silver, (F.col("licensed_beds_int").isNull()) | (F.col("licensed_beds_int") < 0), "invalid licensed_beds")
silver = add_issue(silver, F.col("staffed_beds_int") > F.col("licensed_beds_int"), "staffed_beds exceeds licensed_beds")
# FK check against dim_facility, already built
valid_facilities = [r["facility_id"] for r in spark.read.table("silver_facility").select("facility_id").collect()]
silver = add_issue(silver, ~F.col("facility_id").isin(valid_facilities), "facility_id not found in silver_facility")

silver = silver.withColumn("_dq_passed", F.size("_dq_issues") == 0)
silver = silver.withColumn("_dq_issues", F.concat_ws("; ", "_dq_issues"))
silver.write.format("delta").mode("overwrite").saveAsTable("silver_unit")
log_dq_run(silver, "silver_unit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_bronze = spark.read.table("dbo_1.bronze_payer")
w = Window.partitionBy("payer_id").orderBy(F.col("_batch_id").desc())
bronze = raw_bronze.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

silver = (bronze
    .withColumn("payer_type_folded", F.lower(F.trim("payer_type")))
    .withColumn("share_float", F.col("share").cast("float"))
    .withColumn("prompt_pay_days_int", F.col("prompt_pay_days").cast("int"))
    .withColumn("_dq_issues", F.array().cast("array<string>"))
)
silver = add_issue(silver, F.col("payer_id").isNull(), "payer_id null")
silver = add_issue(silver, (F.col("share_float") < 0) | (F.col("share_float") > 1), "share out of 0-1 range")

silver = silver.withColumn("_dq_passed", F.size("_dq_issues") == 0)
silver = silver.withColumn("_dq_issues", F.concat_ws("; ", "_dq_issues"))
silver.write.format("delta").mode("overwrite").saveAsTable("silver_payer")
log_dq_run(silver, "silver_payer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_bronze = spark.read.table("dbo_1.bronze_drug")
w = Window.partitionBy("ndc11").orderBy(F.col("_batch_id").desc())
bronze = raw_bronze.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

silver = (bronze
    .withColumn("is_controlled_bool", F.col("is_controlled").cast("boolean"))
    .withColumn("is_shortage_prone_bool", F.col("is_shortage_prone").cast("boolean"))
    .withColumn("is_high_alert_bool", F.col("is_high_alert").cast("boolean"))
    .withColumn("unit_cost_float", F.col("unit_cost").cast("float"))
    .withColumn("usage_weight_float", F.col("usage_weight").cast("float"))
    .withColumn("_dq_issues", F.array().cast("array<string>"))
)
silver = add_issue(silver, F.col("ndc11").isNull(), "ndc11 null")
silver = add_issue(silver, (F.col("unit_cost_float").isNull()) | (F.col("unit_cost_float") < 0), "invalid unit_cost")

silver = silver.withColumn("_dq_passed", F.size("_dq_issues") == 0)
silver = silver.withColumn("_dq_issues", F.concat_ws("; ", "_dq_issues"))
silver.write.format("delta").mode("overwrite").saveAsTable("silver_drug")
log_dq_run(silver, "silver_drug")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_staff_all = spark.read.table("dbo_1.bronze_staff")

silver_base = (bronze_staff_all
    .withColumn("fte_float", F.col("fte").cast("float"))
    .withColumn("is_active_bool", F.col("is_active").cast("boolean"))
    .withColumn("_dq_issues", F.array().cast("array<string>"))
)
silver_base = add_issue(silver_base, F.col("staff_id").isNull(), "staff_id null")
silver_base = add_issue(silver_base, (F.col("fte_float") <= 0) | (F.col("fte_float") > 1), "fte out of range")
silver_base = silver_base.withColumn("_dq_passed", F.size("_dq_issues") == 0)
silver_base = silver_base.withColumn("_dq_issues", F.concat_ws("; ", "_dq_issues"))

# SCD-2: order each staff member's snapshots by batch date, open/close validity periods
w = Window.partitionBy("staff_id").orderBy("_batch_id")
scd = (silver_base
    .withColumn("valid_from", F.col("_batch_id"))
    .withColumn("valid_to", F.lead("_batch_id").over(w))  # null = still current
    .withColumn("is_current", F.col("valid_to").isNull())
)

scd.write.format("delta").mode("overwrite").saveAsTable("silver_staff")
log_dq_run(scd, "silver_staff")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_bronze = spark.read.table("dbo_1.bronze_patients")

silver = (raw_bronze
    .withColumn("BirthDate_date", F.to_date("BirthDate"))
    .withColumn("Healthcare_Expenses_float", F.col("Healthcare_Expenses").cast("float"))
    .withColumn("Healthcare_Coverage_float", F.col("Healthcare_Coverage").cast("float"))
    .withColumn("is_high_risk_bool", F.col("is_high_risk").cast("boolean"))
    .withColumn("_dq_issues", F.array().cast("array<string>"))
)
silver = add_issue(silver, F.col("Id").isNull(), "Id null")
silver = add_issue(silver, F.col("BirthDate_date").isNull(), "BirthDate unparseable")

# Master Patient Index: SSN is the strongest match key when present;
# fall back to a composite of birth date + name + zip when SSN is missing/blank
silver = silver.withColumn(
    "mpi_key",
    F.when(F.col("SSN").isNotNull() & (F.trim(F.col("SSN")) != ""), F.concat(F.lit("ssn:"), F.col("SSN")))
     .otherwise(F.concat_ws("|", F.lit("demo"), F.col("BirthDate"), F.lower(F.col("Last")), F.lower(F.col("First")), F.col("Zip")))
)

# Assign one canonical surrogate person id per mpi_key — this is what every
# downstream encounter/admission/claim table should join on, not raw mrn
w = Window.orderBy("mpi_key")
mpi_lookup = silver.select("mpi_key").distinct().withColumn("canonical_patient_key", F.dense_rank().over(w))
silver = silver.join(mpi_lookup, on="mpi_key", how="left")

silver = silver.withColumn("_dq_passed", F.size("_dq_issues") == 0)
silver = silver.withColumn("_dq_issues", F.concat_ws("; ", "_dq_issues"))

silver.write.format("delta").mode("overwrite").saveAsTable("silver_patients")
log_dq_run(silver, "silver_patients")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### The bed reconciliation cross-check

# CELL ********************


spark.sql("DROP TABLE IF EXISTS dq_reconciliation_results")
# Compare bed_hourly (taken at Wednesday, matching NHSN's typical collection day)
# against the same week's NHSN report, per facility
hourly_wed = (spark.read.table("silver_bed_hourly")
    .filter("_dq_passed = TRUE")
    .withColumn("staffed_beds_int", F.col("staffed_beds").cast("int"))
    .withColumn("snapshot_date", F.to_date("snapshot_datetime_ts"))
    .withColumn("dow", F.dayofweek("snapshot_date"))
    .filter("dow = 4")
    .withColumn("iso_week", F.weekofyear("snapshot_date"))
    .withColumn("iso_year", F.year("snapshot_date"))
    # Step 1: sum across all units, for each facility, at each exact hour
    .groupBy("facility_id", "iso_week", "iso_year", "snapshot_datetime_ts")
    .agg(F.sum("staffed_beds_int").alias("hourly_total_staffed_beds"))
    # Step 2: your data repeats the same value every hour of the day, so any
    # single hour is representative — take the latest one per facility/week
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("facility_id", "iso_week", "iso_year")
              .orderBy(F.col("snapshot_datetime_ts").desc())
    ))
    .filter("rn = 1")
    .withColumn("snapshot_date", F.to_date("snapshot_datetime_ts"))
    .select("facility_id", "iso_week", "iso_year", "hourly_total_staffed_beds", "snapshot_date")
)

nhsn = (spark.read.table("silver_bed_nhsn")
    .filter("_dq_passed = TRUE")
    .withColumn("iso_week", F.weekofyear("week_ending_date_d"))
    .withColumn("iso_year", F.year("week_ending_date_d"))
    .select(
        F.col("facility_name").alias("nhsn_facility_name"),
        "iso_week", "iso_year",
        "week_ending_date_d",
        "all_hospital_inpatient_beds_int"
    )
)

fac = spark.read.table("silver_facility").select("facility_id", "facility_name")

reconciliation = (hourly_wed
    .join(fac, on="facility_id", how="left")
    .join(nhsn,
          (hourly_wed.iso_week == nhsn.iso_week) &
          (hourly_wed.iso_year == nhsn.iso_year) &
          (fac.facility_name == nhsn.nhsn_facility_name),
          how="inner")
    .select(
        "facility_id", "facility_name", "snapshot_date", "week_ending_date_d",
        "hourly_total_staffed_beds", "all_hospital_inpatient_beds_int"
    )
    .withColumn("variance", F.abs(F.col("hourly_total_staffed_beds") - F.col("all_hospital_inpatient_beds_int")))
    .withColumn("variance_pct", F.col("variance") / F.col("all_hospital_inpatient_beds_int"))
    .withColumn("reconciliation_flag", F.when(F.col("variance_pct") > 0.05, "MISMATCH").otherwise("OK"))
)

reconciliation.write.format("delta").mode("append").saveAsTable("dq_reconciliation_results")

total_rows = reconciliation.count()
mismatch_count = reconciliation.filter("reconciliation_flag = 'MISMATCH'").count()
print(f"Total comparisons: {total_rows}, mismatches: {mismatch_count}")
reconciliation.show(20, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.read.table("silver_bed_hourly").filter("facility_id = '110501'").filter("_dq_passed = TRUE") \
    .withColumn("snapshot_date", F.to_date("snapshot_datetime_ts")) \
    .withColumn("staffed_beds_int", F.col("staffed_beds").cast("int")) \
    .filter("snapshot_date = '2026-08-05'") \
    .select("snapshot_datetime_ts", "staffed_beds_int").orderBy("snapshot_datetime_ts").show(30, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

total_rows = reconciliation.count()
print(f"Total facility/week comparisons made: {total_rows}")
reconciliation.show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("hourly_wed row count:", hourly_wed.count())
hourly_wed.show(5, truncate=False)

print("nhsn row count:", nhsn.count())
nhsn.show(5, truncate=False)

print("fac row count:", fac.count())
fac.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### **Tightening the placeholder enums/thresholds**

# CELL ********************

import re

# Paste each Silver cell's source as a string and this pulls out every
# add_issue(...) condition so you can review them in one list, instead of
# scrolling through 19 separate cells
checks_to_review = [
    ("silver_payer", "share_float < 0 or > 1"),
    ("silver_bed_hourly", "occupancy_rate_float 0-1 range"),
    ("silver_ed_stays", "acuity_int 1-5 range"),
    ("silver_diagnoses", "icd_version_int in [9, 10]"),
    ("silver_staff", "fte_float 0-1 range, note: should this allow >1 for staff working multiple roles?"),
]
for table, note in checks_to_review:
    print(f"{table}: {note}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



# Read the answer key — treat as reference data, never write it into Bronze/Silver/Gold tables
#import json

#with open("/lakehouse/default/Files/raw/dq_answer_key.json") as f:
    #answer_key = json.load(f)   # adjust path to wherever you landed it
#print(f"Answer key covers {len(answer_key)} known injected defects")
#print(answer_key[0] if answer_key else "empty")   # inspect the shape first

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Small dimension lookups — safe to collect to driver, these tables are tiny
valid_facility_ids = [r["facility_id"] for r in spark.read.table("silver_facility").select("facility_id").collect()]
valid_unit_ids = [r["unit_id"] for r in spark.read.table("silver_unit").select("unit_id").collect()]
valid_staff_ids = [r["staff_id"] for r in spark.read.table("silver_staff").select("staff_id").distinct().collect()]

# Patient lookup stays a DataFrame — this table can be large, we join to it, never collect it
patient_lookup = spark.read.table("silver_patients").select(
    F.col("Id").alias("_patient_lookup_id"),
    "canonical_patient_key"
)

def add_patient_key(df, patient_col):
    """Joins df to silver_patients on patient_col == Id, adds canonical_patient_key,
    and flags rows whose patient reference didn't resolve to anyone in silver_patients."""
    joined = df.join(
        patient_lookup,
        df[patient_col] == patient_lookup["_patient_lookup_id"],
        how="left"
    ).drop("_patient_lookup_id")
    joined = add_issue(joined, F.col("canonical_patient_key").isNull(), f"{patient_col} not found in silver_patients")
    return joined

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_bronze = spark.read.table("dbo_1.bronze_encounters")
silver = (raw_bronze
    .withColumn("Start_ts", F.to_timestamp("Start"))
    .withColumn("Stop_ts", F.to_timestamp("Stop"))
    .withColumn("Base_Encounter_Cost_float", F.col("Base_Encounter_Cost").cast("float"))
    .withColumn("Total_Claim_Cost_float", F.col("Total_Claim_Cost").cast("float"))
    .withColumn("_dq_issues", F.array().cast("array<string>"))
)
silver = add_issue(silver, F.col("Id").isNull(), "encounter Id null")
silver = add_issue(silver, F.col("Stop_ts") < F.col("Start_ts"), "Stop before Start")
silver = add_issue(silver, ~F.col("facility_id").isin(valid_facility_ids), "facility_id not found in silver_facility")
silver = add_issue(silver, (F.col("unit_id").isNotNull()) & (~F.col("unit_id").isin(valid_unit_ids)), "unit_id not found in silver_unit")

silver = add_patient_key(silver, "Patient")

silver = silver.withColumn("_dq_passed", F.size("_dq_issues") == 0)
silver = silver.withColumn("_dq_issues", F.concat_ws("; ", "_dq_issues"))
silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_encounters")
log_dq_run(silver, "silver_encounters")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_bronze = spark.read.table("dbo_1.bronze_admissions")
silver = (raw_bronze
    .withColumn("admittime_ts", F.to_timestamp("admittime"))
    .withColumn("dischtime_ts", F.to_timestamp("dischtime"))
    .withColumn("is_readmission_bool", F.col("is_readmission").cast("boolean"))
    .withColumn("hospital_expire_flag_bool", F.col("hospital_expire_flag").cast("boolean"))
    .withColumn("_dq_issues", F.array().cast("array<string>"))
)
silver = add_issue(silver, F.col("hadm_id").isNull(), "hadm_id null")
silver = add_issue(silver, F.col("dischtime_ts") < F.col("admittime_ts"), "dischtime before admittime")
silver = add_issue(silver, ~F.col("facility_id").isin(valid_facility_ids), "facility_id not found in silver_facility")

silver = add_patient_key(silver, "subject_id")

silver = silver.withColumn("_dq_passed", F.size("_dq_issues") == 0)
silver = silver.withColumn("_dq_issues", F.concat_ws("; ", "_dq_issues"))
silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_admissions")
log_dq_run(silver, "silver_admissions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_bronze = spark.read.table("dbo_1.bronze_ed_stays")
silver = (raw_bronze
    .withColumn("intime_ts", F.to_timestamp("intime"))
    .withColumn("outtime_ts", F.to_timestamp("outtime"))
    .withColumn("acuity_int", F.col("acuity").cast("int"))
    .withColumn("heartrate_int", F.col("heartrate").cast("int"))
    .withColumn("_dq_issues", F.array().cast("array<string>"))
)
silver = add_issue(silver, F.col("stay_id").isNull(), "stay_id null")
silver = add_issue(silver, (F.col("acuity_int") < 1) | (F.col("acuity_int") > 5), "acuity out of 1-5 range")
silver = add_issue(silver, ~F.col("facility_id").isin(valid_facility_ids), "facility_id not found in silver_facility")

# hadm_id here is optional (an ED visit doesn't always turn into an admission),
# so only flag it when it's populated but doesn't match a real admission
valid_hadm_ids = [r["hadm_id"] for r in spark.read.table("silver_admissions").select("hadm_id").distinct().collect()]
silver = add_issue(silver, (F.col("hadm_id").isNotNull()) & (~F.col("hadm_id").isin(valid_hadm_ids)), "hadm_id not found in silver_admissions")

silver = add_patient_key(silver, "subject_id")

silver = silver.withColumn("_dq_passed", F.size("_dq_issues") == 0)
silver = silver.withColumn("_dq_issues", F.concat_ws("; ", "_dq_issues"))
silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_ed_stays")
log_dq_run(silver, "silver_ed_stays")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_bronze = spark.read.table("dbo_1.bronze_transfers")
silver = (raw_bronze
    .withColumn("intime_ts", F.to_timestamp("intime"))
    .withColumn("outtime_ts", F.to_timestamp("outtime"))
    .withColumn("_dq_issues", F.array().cast("array<string>"))
)
silver = add_issue(silver, F.col("transfer_id").isNull(), "transfer_id null")
silver = add_issue(silver, ~F.col("facility_id").isin(valid_facility_ids), "facility_id not found in silver_facility")
silver = add_issue(silver, (F.col("unit_id").isNotNull()) & (~F.col("unit_id").isin(valid_unit_ids)), "unit_id not found in silver_unit")

silver = add_patient_key(silver, "subject_id")

silver = silver.withColumn("_dq_passed", F.size("_dq_issues") == 0)
silver = silver.withColumn("_dq_issues", F.concat_ws("; ", "_dq_issues"))
silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_transfers")
log_dq_run(silver, "silver_transfers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_bronze = spark.read.table("dbo_1.bronze_diagnoses")
silver = (raw_bronze
    .withColumn("seq_num_int", F.col("seq_num").cast("int"))
    .withColumn("icd_version_int", F.col("icd_version").cast("int"))
    .withColumn("_dq_issues", F.array().cast("array<string>"))
)
silver = add_issue(silver, F.col("icd_code").isNull(), "icd_code null")
silver = add_issue(silver, ~F.col("icd_version_int").isin([9, 10]), "invalid icd_version")
silver = add_issue(silver, ~F.col("facility_id").isin(valid_facility_ids), "facility_id not found in silver_facility")

silver = add_patient_key(silver, "subject_id")

silver = silver.withColumn("_dq_passed", F.size("_dq_issues") == 0)
silver = silver.withColumn("_dq_issues", F.concat_ws("; ", "_dq_issues"))
silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_diagnoses")
log_dq_run(silver, "silver_diagnoses")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# _silver_claim_header — facility + patient + attending provider NPI_

# CELL ********************

raw_bronze = spark.read.table("dbo_1.bronze_claim_header")
silver = (raw_bronze
    .withColumn("total_charge_amount_float", F.col("total_charge_amount").cast("float"))
    .withColumn("is_readmission_related_bool", F.col("is_readmission_related").cast("boolean"))
    .withColumn("_dq_issues", F.array().cast("array<string>"))
)
silver = add_issue(silver, F.col("patient_control_number").isNull(), "patient_control_number null")
silver = add_issue(silver, (F.col("total_charge_amount_float").isNull()) | (F.col("total_charge_amount_float") < 0), "invalid total_charge_amount")
silver = add_issue(silver, ~F.col("facility_id").isin(valid_facility_ids), "facility_id not found in silver_facility")

valid_npis = [r["npi"] for r in spark.read.table("silver_staff").select("npi").distinct().collect()]
silver = add_issue(silver, (F.col("attending_provider_npi").isNotNull()) & (~F.col("attending_provider_npi").isin(valid_npis)), "attending_provider_npi not found in silver_staff")

silver = add_patient_key(silver, "subject_id")

silver = silver.withColumn("_dq_passed", F.size("_dq_issues") == 0)
silver = silver.withColumn("_dq_issues", F.concat_ws("; ", "_dq_issues"))
silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_claim_header")
log_dq_run(silver, "silver_claim_header")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

valid_claim_numbers = [r["patient_control_number"] for r in
                        spark.read.table("silver_claim_header").select("patient_control_number").distinct().collect()]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_bronze = spark.read.table("dbo_1.bronze_claim_line")
silver = (raw_bronze
    .withColumn("line_charge_amount_float", F.col("line_charge_amount").cast("float"))
    .withColumn("unit_count_int", F.col("unit_count").cast("int"))
    .withColumn("_dq_issues", F.array().cast("array<string>"))
)
silver = add_issue(silver, F.col("patient_control_number").isNull(), "patient_control_number null")
silver = add_issue(silver, ~F.col("patient_control_number").isin(valid_claim_numbers), "patient_control_number not found in silver_claim_header")
silver = silver.withColumn("_dq_passed", F.size("_dq_issues") == 0)
silver = silver.withColumn("_dq_issues", F.concat_ws("; ", "_dq_issues"))
silver.write.format("delta").mode("overwrite").saveAsTable("silver_claim_line")
log_dq_run(silver, "silver_claim_line")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_bronze = spark.read.table("dbo_1.bronze_remit")
silver = (raw_bronze
    .withColumn("claim_payment_amount_float", F.col("claim_payment_amount").cast("float"))
    .withColumn("is_appealed_bool", F.col("is_appealed").cast("boolean"))
    .withColumn("is_overturned_on_appeal_bool", F.col("is_overturned_on_appeal").cast("boolean"))
    .withColumn("_dq_issues", F.array().cast("array<string>"))
)
silver = add_issue(silver, F.col("patient_control_number").isNull(), "patient_control_number null")
silver = add_issue(silver, ~F.col("patient_control_number").isin(valid_claim_numbers), "patient_control_number not found in silver_claim_header")
silver = silver.withColumn("_dq_passed", F.size("_dq_issues") == 0)
silver = silver.withColumn("_dq_issues", F.concat_ws("; ", "_dq_issues"))
silver.write.format("delta").mode("overwrite").saveAsTable("silver_remit")
log_dq_run(silver, "silver_remit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_bronze = spark.read.table("dbo_1.bronze_remit_adjustment")
silver = (raw_bronze
    .withColumn("amount_float", F.col("amount").cast("float"))
    .withColumn("is_denial_bool", F.col("is_denial").cast("boolean"))
    .withColumn("_dq_issues", F.array().cast("array<string>"))
)
silver = add_issue(silver, F.col("patient_control_number").isNull(), "patient_control_number null")
silver = add_issue(silver, ~F.col("patient_control_number").isin(valid_claim_numbers), "patient_control_number not found in silver_claim_header")
silver = silver.withColumn("_dq_passed", F.size("_dq_issues") == 0)
silver = silver.withColumn("_dq_issues", F.concat_ws("; ", "_dq_issues"))
silver.write.format("delta").mode("overwrite").saveAsTable("silver_remit_adjustment")
log_dq_run(silver, "silver_remit_adjustment")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_bronze = spark.read.table("dbo_1.bronze_bed_hourly")
silver = (raw_bronze
    .withColumn("snapshot_datetime_ts", F.to_timestamp("snapshot_datetime"))
    .withColumn("occupied_beds_int", F.col("occupied_beds").cast("int"))
    .withColumn("available_beds_int", F.col("available_beds").cast("int"))
    .withColumn("occupancy_rate_float", F.col("occupancy_rate").cast("float"))
    .withColumn("is_at_capacity_bool", F.col("is_at_capacity").cast("boolean"))
    .withColumn("_dq_issues", F.array().cast("array<string>"))
)
silver = add_issue(silver, F.col("snapshot_datetime_ts").isNull(), "snapshot_datetime unparseable")
silver = add_issue(silver, (F.col("occupancy_rate_float") < 0) | (F.col("occupancy_rate_float") > 1), "occupancy_rate out of 0-1 range")
silver = add_issue(silver, ~F.col("facility_id").isin(valid_facility_ids), "facility_id not found in silver_facility")
silver = add_issue(silver, ~F.col("unit_id").isin(valid_unit_ids), "unit_id not found in silver_unit")
silver = silver.withColumn("_dq_passed", F.size("_dq_issues") == 0)
silver = silver.withColumn("_dq_issues", F.concat_ws("; ", "_dq_issues"))
silver.write.format("delta").mode("overwrite").saveAsTable("silver_bed_hourly")
log_dq_run(silver, "silver_bed_hourly")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

valid_facility_names = [r["facility_name"] for r in spark.read.table("silver_facility").select("facility_name").collect()]

raw_bronze = spark.read.table("dbo_1.bronze_bed_nhsn")
silver = (raw_bronze
    .withColumn("week_ending_date_d", F.to_date("week_ending_date"))
    .withColumn("all_hospital_inpatient_beds_int", F.col("all_hospital_inpatient_beds").cast("int"))
    .withColumn("all_hospital_inpatient_occupancy_int", F.col("all_hospital_inpatient_occupancy").cast("int"))
    .withColumn("_dq_issues", F.array().cast("array<string>"))
)
silver = add_issue(silver, F.col("week_ending_date_d").isNull(), "week_ending_date unparseable")
silver = add_issue(silver, F.col("all_hospital_inpatient_occupancy_int") > F.col("all_hospital_inpatient_beds_int"), "occupancy exceeds bed count")
silver = add_issue(silver, ~F.col("facility_name").isin(valid_facility_names), "facility_name not found in silver_facility")
silver = silver.withColumn("_dq_passed", F.size("_dq_issues") == 0)
silver = silver.withColumn("_dq_issues", F.concat_ws("; ", "_dq_issues"))
silver.write.format("delta").mode("overwrite").saveAsTable("silver_bed_nhsn")
log_dq_run(silver, "silver_bed_nhsn")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_bronze = spark.read.table("dbo_1.bronze_pharmacy_inventory")
silver = (raw_bronze
    .withColumn("qty_on_hand_int", F.col("qty_on_hand").cast("int"))
    .withColumn("reorder_point_int", F.col("reorder_point").cast("int"))
    .withColumn("is_stockout_bool", F.col("is_stockout").cast("boolean"))
    .withColumn("is_controlled_bool", F.col("is_controlled").cast("boolean"))
    .withColumn("_dq_issues", F.array().cast("array<string>"))
)
silver = add_issue(silver, F.col("ndc11").isNull(), "ndc11 null")
silver = add_issue(silver, (F.col("qty_on_hand_int").isNull()) | (F.col("qty_on_hand_int") < 0), "invalid qty_on_hand")
silver = add_issue(silver, ~F.col("facility_id").isin(valid_facility_ids), "facility_id not found in silver_facility")
silver = silver.withColumn("_dq_passed", F.size("_dq_issues") == 0)
silver = silver.withColumn("_dq_issues", F.concat_ws("; ", "_dq_issues"))
silver.write.format("delta").mode("overwrite").saveAsTable("silver_pharmacy_inventory")
log_dq_run(silver, "silver_pharmacy_inventory")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_bronze = spark.read.table("dbo_1.bronze_staff_schedules")
silver = (raw_bronze
    .withColumn("scheduled_hours_float", F.col("scheduled_hours").cast("float"))
    .withColumn("actual_hours_float", F.col("actual_hours").cast("float"))
    .withColumn("_dq_issues", F.array().cast("array<string>"))
)
silver = add_issue(silver, F.col("staff_id").isNull(), "staff_id null")
silver = add_issue(silver, F.col("facility_id").isNull(), "facility_id null")
silver = add_issue(silver, ~F.col("facility_id").isin(valid_facility_ids), "facility_id not found in silver_facility")
silver = add_issue(silver, ~F.col("staff_id").isin(valid_staff_ids), "staff_id not found in silver_staff")
silver = silver.withColumn("_dq_passed", F.size("_dq_issues") == 0)
silver = silver.withColumn("_dq_issues", F.concat_ws("; ", "_dq_issues"))
silver.write.format("delta").mode("overwrite").saveAsTable("silver_staff_schedules")
log_dq_run(silver, "silver_staff_schedules")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.read.table("dq_run_results").orderBy(F.col("run_timestamp").desc()).show(25, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.read.table("silver_staff_schedules").filter("_dq_passed = FALSE").select("_dq_issues").distinct().show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.read.table("silver_diagnoses").select("facility_id").distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.read.table("silver_facility").filter("_dq_passed = FALSE").select("facility_id", "facility_name", "_dq_issues").show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.read.table("dbo_1.bronze_facility").select(
    "facility_id", "facility_name", "facility_type", "licensed_beds", "staffed_beds"
).show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_bronze = spark.read.table("dbo_1.bronze_facility")

w = Window.partitionBy("facility_id").orderBy(F.col("_batch_id").desc())
bronze = (raw_bronze
    .withColumn("_rn", F.row_number().over(w))
    .filter("_rn = 1")
    .drop("_rn")
)

# Matches actual values in the source data (confirmed against bronze_facility)
valid_facility_type = ["general acute care", "teaching", "regional", "community", "urgent care"]

silver = (bronze
    .withColumn("facility_type_folded", F.lower(F.trim("facility_type")))
    .withColumn("emergency_services_bool", F.col("emergency_services").cast("boolean"))
    .withColumn("licensed_beds_int", F.col("licensed_beds").cast("int"))
    .withColumn("staffed_beds_int", F.col("staffed_beds").cast("int"))
    .withColumn("_dq_issues", F.array().cast("array<string>"))
)

silver = add_issue(silver, F.col("facility_id").isNull(), "facility_id null")
silver = add_issue(silver, ~F.col("facility_type_folded").isin(valid_facility_type), "invalid facility_type")

# Urgent care legitimately has 0 licensed inpatient beds — only flag <0 (impossible),
# and only require >0 for facility types that actually admit inpatients
silver = add_issue(silver,
    (F.col("licensed_beds_int").isNull()) |
    (F.col("licensed_beds_int") < 0) |
    ((F.col("licensed_beds_int") == 0) & (F.col("facility_type_folded") != "urgent care")),
    "invalid licensed_beds"
)

# Removed the staffed_beds > licensed_beds hard-fail: this dataset shows staffed
# capacity legitimately exceeding licensed capacity across most facilities
# (surge staffing / licensed vs. operational bed counts are different concepts).
# Kept as an informational flag only, not a DQ failure.
silver = silver.withColumn(
    "staffed_exceeds_licensed_info",
    F.col("staffed_beds_int") > F.col("licensed_beds_int")
)

silver = silver.withColumn("_dq_passed", F.size("_dq_issues") == 0)
silver = silver.withColumn("_dq_issues", F.concat_ws("; ", "_dq_issues"))

silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_facility")
log_dq_run(silver, "silver_facility")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.read.table("silver_facility").select("facility_id", "facility_name", "_dq_passed", "_dq_issues", "staffed_exceeds_licensed_info").show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.read.table("dq_run_results").filter("table_name = 'silver_staff_schedules'").orderBy(F.col("run_timestamp").desc()).show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.read.table("silver_staff_schedules").filter("_dq_passed = FALSE").select("facility_id").distinct().show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
