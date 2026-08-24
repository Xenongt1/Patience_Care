# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "99b52282-9001-4b97-908a-7e3cb406f148",
# META       "default_lakehouse_name": "lh_gold",
# META       "default_lakehouse_workspace_id": "f86bd9be-e828-420b-b5c6-77937de2fe75",
# META       "known_lakehouses": [
# META         {
# META           "id": "99b52282-9001-4b97-908a-7e3cb406f148"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# Creating the dim_date: no source table, pure generated logic

# CELL ********************

from pyspark.sql import functions as F

# Generate one row per calendar day across your simulation window and a bit of buffer
date_range = spark.sql("""
    SELECT explode(sequence(to_date('2025-01-01'), to_date('2027-12-31'), interval 1 day)) AS full_date
""")

dim_date = (date_range
    .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("day", F.dayofmonth("full_date"))
    .withColumn("day_of_week", F.dayofweek("full_date"))  # 1=Sunday ... 7=Saturday
    .withColumn("day_name", F.date_format("full_date", "EEEE"))
    .withColumn("month_name", F.date_format("full_date", "MMMM"))
    .withColumn("iso_week", F.weekofyear("full_date"))
    .withColumn("quarter", F.quarter("full_date"))
    .withColumn("is_weekend", F.col("day_of_week").isin([1, 7]))
)

dim_date.write.format("delta").mode("overwrite").saveAsTable("dim_date")
print(f"dim_date: {dim_date.count()} rows generated")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.window import Window

# THE DQ GATE — this line is the actual gate from your architecture diagram.
# Every Gold table starts with this filter, no exceptions.
silver_facility_clean = spark.read.table("dbo_1.silver_facility").filter("_dq_passed = TRUE")

w = Window.orderBy("facility_id")
dim_facility = silver_facility_clean.withColumn("facility_key", F.row_number().over(w))

dim_facility = dim_facility.select(
    "facility_key",
    "facility_id",
    "facility_name",
    F.col("facility_type_folded").alias("facility_type"),
    "city", "state", "zip", "county",
    F.col("emergency_services_bool").alias("emergency_services"),
    F.col("licensed_beds_int").alias("licensed_beds"),
    F.col("staffed_beds_int").alias("staffed_beds"),
    "ownership"
)

dim_facility.write.format("delta").mode("overwrite").saveAsTable("dim_facility")
print(f"dim_facility: {dim_facility.count()} rows (gated from {spark.read.table('dbo_1.silver_facility').count()} total Silver rows)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Creating dim_unit

# CELL ********************

silver_unit_clean = spark.read.table("dbo_1.silver_unit").filter("_dq_passed = TRUE")

# Join to dim_facility to pick up facility_key, not just facility_id — Gold facts
# join dimension-to-dimension via surrogate keys, so dim_unit needs facility_key now
fac_key_lookup = spark.read.table("dim_facility").select("facility_key", "facility_id")

w = Window.orderBy("unit_id")
dim_unit = (silver_unit_clean
    .join(fac_key_lookup, on="facility_id", how="left")
    .withColumn("unit_key", F.row_number().over(w))
)

dim_unit = dim_unit.select(
    "unit_key",
    "unit_id",
    "facility_key",
    "facility_id",
    "unit_code", "unit_name",
    F.col("unit_type_folded").alias("unit_type"),
    "building", "floor",
    F.col("licensed_beds_int").alias("licensed_beds"),
    F.col("staffed_beds_int").alias("staffed_beds"),
    F.col("blocked_beds_int").alias("blocked_beds"),
    F.col("nurse_patient_ratio_target_float").alias("nurse_patient_ratio_target"),
    F.col("is_critical_care_bool").alias("is_critical_care"),
    F.col("is_monitored_bool").alias("is_monitored")
)

dim_unit.write.format("delta").mode("overwrite").saveAsTable("dim_unit")
print(f"dim_unit: {dim_unit.count()} rows (gated from {spark.read.table('dbo_1.silver_unit').count()} total Silver rows)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_payer_clean = spark.read.table("dbo_1.silver_payer").filter("_dq_passed = TRUE")

w = Window.orderBy("payer_id")
dim_payer = silver_payer_clean.withColumn("payer_key", F.row_number().over(w))

dim_payer = dim_payer.select(
    "payer_key", "payer_id", "payer_name",
    F.col("payer_type_folded").alias("payer_type"),
    "claim_filing_indicator_code",
    F.col("share_float").alias("share"),
    F.col("prompt_pay_days_int").alias("prompt_pay_days")
)

dim_payer.write.format("delta").mode("overwrite").saveAsTable("dim_payer")
print(f"dim_payer: {dim_payer.count()} rows (gated from {spark.read.table('dbo_1.silver_payer').count()} total Silver rows)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_drug_clean = spark.read.table("dbo_1.silver_drug").filter("_dq_passed = TRUE")

w = Window.orderBy("ndc11")
dim_drug = silver_drug_clean.withColumn("drug_key", F.row_number().over(w))

dim_drug = dim_drug.select(
    "drug_key", "ndc11", "rxcui_scd", "product_ndc",
    "proprietary_name", "non_proprietary_name",
    "dosage_form_name", "route_name", "pharm_classes",
    "dea_schedule",
    F.col("is_controlled_bool").alias("is_controlled"),
    F.col("is_shortage_prone_bool").alias("is_shortage_prone"),
    F.col("is_high_alert_bool").alias("is_high_alert"),
    F.col("unit_cost_float").alias("unit_cost"),
    "abc_class"
)

dim_drug.write.format("delta").mode("overwrite").saveAsTable("dim_drug")
print(f"dim_drug: {dim_drug.count()} rows (gated from {spark.read.table('dbo_1.silver_drug').count()} total Silver rows)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_staff_clean = spark.read.table("dbo_1.silver_staff").filter("_dq_passed = TRUE")

# Surrogate key per historical version, not per person — one staff member can
# have multiple rows here (one per valid_from/valid_to period), which is correct
w = Window.orderBy("staff_id", "valid_from")
dim_staff = silver_staff_clean.withColumn("staff_key", F.row_number().over(w))

dim_staff = dim_staff.select(
    "staff_key", "staff_id", "npi", "first_name", "last_name",
    "job_code", "job_title", "credential",
    "primary_facility_id", "primary_unit_id",
    "employment_type",
    F.col("fte_float").alias("fte"),
    "hire_date", "termination_date",
    F.col("is_active_bool").alias("is_active"),
    "valid_from", "valid_to", "is_current"
)

dim_staff.write.format("delta").mode("overwrite").saveAsTable("dim_staff")
print(f"dim_staff: {dim_staff.count()} rows (gated from {spark.read.table('dbo_1.silver_staff').count()} total Silver rows)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Creating / working on dim_patients

# CELL ********************

silver_patients_clean = spark.read.table("dbo_1.silver_patients").filter("_dq_passed = TRUE")

# One row per real person, not per facility-scoped record — dedupe on the MPI key
# your canonical_patient_key already collapses multiple Id/mrn pairs to one person,
# so pick one representative row per canonical_patient_key
w_dedupe = Window.partitionBy("canonical_patient_key").orderBy(F.col("BirthDate_date").asc())
patients_deduped = (silver_patients_clean
    .withColumn("_rn", F.row_number().over(w_dedupe))
    .filter("_rn = 1")
    .drop("_rn")
)

w = Window.orderBy("canonical_patient_key")
dim_patient = patients_deduped.withColumn("patient_key", F.row_number().over(w))

# Age band, not raw birthdate — another layer of de-identification beyond just
# dropping SSN/name, since exact birthdate combined with zip can still re-identify someone
dim_patient = dim_patient.withColumn(
    "age_band",
    F.when(F.datediff(F.current_date(), "BirthDate_date") < 365*18, "0-17")
     .when(F.datediff(F.current_date(), "BirthDate_date") < 365*35, "18-34")
     .when(F.datediff(F.current_date(), "BirthDate_date") < 365*50, "35-49")
     .when(F.datediff(F.current_date(), "BirthDate_date") < 365*65, "50-64")
     .otherwise("65+")
)

dim_patient = dim_patient.select(
    "patient_key",
    "canonical_patient_key",
    "age_band",
    "Gender",
    "Race",
    "Ethnicity",
    "Zip",
    F.col("is_high_risk_bool").alias("is_high_risk"),
    F.col("Healthcare_Coverage_float").alias("insurance_type_placeholder")  # see note below
)

dim_patient.write.format("delta").mode("overwrite").saveAsTable("dim_patient")
print(f"dim_patient: {dim_patient.count()} rows (gated from {spark.read.table('dbo_1.silver_patients').count()} total Silver rows)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Distinct people (canonical_patient_key):", spark.read.table("dim_patient").select("canonical_patient_key").distinct().count())
print("Rows in dim_patient:", spark.read.table("dim_patient").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# fact_encounters- the one everything else roughly parallels

# CELL ********************

silver_encounters_clean = spark.read.table("dbo_1.silver_encounters").filter("_dq_passed = TRUE")

fac_lookup = spark.read.table("dim_facility").select("facility_key", "facility_id")
unit_lookup = spark.read.table("dim_unit").select("unit_key", "unit_id")
patient_lookup = spark.read.table("dim_patient").select("patient_key", "canonical_patient_key")

fact_encounters = (silver_encounters_clean
    .join(fac_lookup, on="facility_id", how="left")
    .join(unit_lookup, on="unit_id", how="left")
    .join(patient_lookup, on="canonical_patient_key", how="left")  # already carried from Silver's add_patient_key
    .withColumn("encounter_date_key", F.date_format("Start_ts", "yyyyMMdd").cast("int"))
)

fact_encounters = fact_encounters.select(
    F.col("Id").alias("encounter_id"),
    "facility_key", "unit_key", "patient_key",
    "encounter_date_key",
    "Start_ts", "Stop_ts",
    F.col("EncounterClass").alias("encounter_class"),
    F.col("Base_Encounter_Cost_float").alias("base_encounter_cost"),
    F.col("Total_Claim_Cost_float").alias("total_claim_cost"),
    F.col("Payer_Coverage").alias("payer_coverage"),
    "ReasonDescription"
)

fact_encounters.write.format("delta").mode("overwrite").saveAsTable("fact_encounters")
print(f"fact_encounters: {fact_encounters.count()} rows (gated from {spark.read.table('dbo_1.silver_encounters').count()} total Silver rows)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_encounters.filter("facility_key IS NULL").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_encounters.filter("patient_key IS NULL").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

orphaned = fact_encounters.filter("patient_key IS NULL").select("encounter_id").limit(5)
orphaned.show()

# Get one canonical_patient_key from an orphaned encounter and check its patient row directly
orphaned_keys = (silver_encounters_clean
    .join(patient_lookup, on="canonical_patient_key", how="left")
    .filter("patient_key IS NULL")
    .select("canonical_patient_key")
    .distinct()
)
orphaned_keys.show(5)

spark.read.table("dbo_1.silver_patients").join(orphaned_keys, on="canonical_patient_key", how="inner") \
    .select("canonical_patient_key", "_dq_passed", "_dq_issues").show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.read.table("dbo_1.silver_patients").join(
    orphaned_keys, on="canonical_patient_key"
).select("canonical_patient_key", "Id", "BirthDate", "BirthDate_date").show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Same pattern, one addition: silver_admissions already carries canonical_patient_key (built by add_patient_key in Silver), so this join is more direct than fact_encounters' was.

# CELL ********************

silver_admissions_clean = spark.read.table("dbo_1.silver_admissions").filter("_dq_passed = TRUE")

fac_lookup = spark.read.table("dim_facility").select("facility_key", "facility_id")
patient_lookup = spark.read.table("dim_patient").select("patient_key", "canonical_patient_key")

fact_admissions = (silver_admissions_clean
    .join(fac_lookup, on="facility_id", how="left")
    .join(patient_lookup, on="canonical_patient_key", how="left")
    .withColumn("admit_date_key", F.date_format("admittime_ts", "yyyyMMdd").cast("int"))
    .withColumn("discharge_date_key", F.date_format("dischtime_ts", "yyyyMMdd").cast("int"))
    .withColumn("length_of_stay_hours", (F.unix_timestamp("dischtime_ts") - F.unix_timestamp("admittime_ts")) / 3600)
)

fact_admissions = fact_admissions.select(
    "hadm_id",
    F.col("encounter_id").alias("source_encounter_id"),  # links back to fact_encounters.encounter_id
    "facility_key", "patient_key",
    "admit_date_key", "discharge_date_key",
    "admittime_ts", "dischtime_ts", "length_of_stay_hours",
    "admission_type", "admission_location", "discharge_location",
    "insurance", "hospital_service",
    F.col("hospital_expire_flag_bool").alias("hospital_expire_flag"),
    F.col("is_readmission_bool").alias("is_readmission"),
    "is_planned_readmission",
    "drg_code",
    "index_encounter_id"
)

fact_admissions.write.format("delta").mode("overwrite").saveAsTable("fact_admissions")
print(f"fact_admissions: {fact_admissions.count()} rows (gated from {spark.read.table('dbo_1.silver_admissions').count()} total Silver rows)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_admissions.filter("facility_key IS NULL").count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_admissions.filter("patient_key IS NULL").count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_admissions.filter("length_of_stay_hours < 0").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_ed_stays_clean = spark.read.table("dbo_1.silver_ed_stays").filter("_dq_passed = TRUE")

fac_lookup = spark.read.table("dim_facility").select("facility_key", "facility_id")
patient_lookup = spark.read.table("dim_patient").select("patient_key", "canonical_patient_key")

fact_ed_stays = (silver_ed_stays_clean
    .join(fac_lookup, on="facility_id", how="left")
    .join(patient_lookup, on="canonical_patient_key", how="left")
    .withColumn("stay_date_key", F.date_format("intime_ts", "yyyyMMdd").cast("int"))
    .withColumn("wait_time_minutes", (F.unix_timestamp("triage_time") - F.unix_timestamp("intime_ts")) / 60)
    .withColumn("length_of_stay_minutes", (F.unix_timestamp("outtime_ts") - F.unix_timestamp("intime_ts")) / 60)
)

fact_ed_stays = fact_ed_stays.select(
    "stay_id",
    F.col("encounter_id").alias("source_encounter_id"),
    "hadm_id",  # nullable — an ED visit doesn't always become an admission
    "facility_key", "patient_key",
    "stay_date_key",
    "intime_ts", "outtime_ts",
    "wait_time_minutes", "length_of_stay_minutes",
    "arrival_transport", "disposition",
    F.col("acuity_int").alias("acuity"),
    "chiefcomplaint",
    "temperature", "heartrate", "resprate", "o2sat", "sbp", "dbp", "pain"
)

fact_ed_stays.write.format("delta").mode("overwrite").saveAsTable("fact_ed_stays")
print(f"fact_ed_stays: {fact_ed_stays.count()} rows (gated from {spark.read.table('dbo_1.silver_ed_stays').count()} total Silver rows)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_ed_stays.filter("facility_key IS NULL").count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_ed_stays.filter("patient_key IS NULL").count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_ed_stays.filter("wait_time_minutes < 0").count()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Sanity check the actual values, not just nulls — a wait time of 400,000 minutes
# would technically "pass" a not-null check while being obviously wrong
fact_ed_stays.selectExpr("min(wait_time_minutes)", "max(wait_time_minutes)", "avg(wait_time_minutes)").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_transfers_clean = spark.read.table("dbo_1.silver_transfers").filter("_dq_passed = TRUE")

fac_lookup = spark.read.table("dim_facility").select("facility_key", "facility_id")
unit_lookup = spark.read.table("dim_unit").select("unit_key", "unit_id")
patient_lookup = spark.read.table("dim_patient").select("patient_key", "canonical_patient_key")

fact_transfers = (silver_transfers_clean
    .join(fac_lookup, on="facility_id", how="left")
    .join(unit_lookup, on="unit_id", how="left")
    .join(patient_lookup, on="canonical_patient_key", how="left")
    .withColumn("transfer_date_key", F.date_format("intime_ts", "yyyyMMdd").cast("int"))
    .withColumn("transfer_duration_hours", (F.unix_timestamp("outtime_ts") - F.unix_timestamp("intime_ts")) / 3600)
)

fact_transfers = fact_transfers.select(
    "transfer_id",
    "hadm_id",
    "facility_key", "unit_key", "patient_key",
    "transfer_date_key",
    "intime_ts", "outtime_ts", "transfer_duration_hours",
    "eventtype", "careunit"
)

fact_transfers.write.format("delta").mode("overwrite").saveAsTable("fact_transfers")
print(f"fact_transfers: {fact_transfers.count()} rows (gated from {spark.read.table('dbo_1.silver_transfers').count()} total Silver rows)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_transfers.filter("facility_key IS NULL").count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_transfers.filter("unit_key IS NULL").count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_transfers.filter("patient_key IS NULL").count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_transfers.filter("transfer_duration_hours < 0").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_diagnoses_clean = spark.read.table("dbo_1.silver_diagnoses").filter("_dq_passed = TRUE")

fac_lookup = spark.read.table("dim_facility").select("facility_key", "facility_id")
patient_lookup = spark.read.table("dim_patient").select("patient_key", "canonical_patient_key")

fact_diagnoses = (silver_diagnoses_clean
    .join(fac_lookup, on="facility_id", how="left")
    .join(patient_lookup, on="canonical_patient_key", how="left")
)

fact_diagnoses = fact_diagnoses.select(
    "hadm_id",
    F.col("encounter_id").alias("source_encounter_id"),
    "facility_key", "patient_key",
    F.col("seq_num_int").alias("seq_num"),
    "icd_code",
    F.col("icd_version_int").alias("icd_version"),
    "icd_title",
    "hrrp_cohort"
)

fact_diagnoses.write.format("delta").mode("overwrite").saveAsTable("fact_diagnoses")
print(f"fact_diagnoses: {fact_diagnoses.count()} rows (gated from {spark.read.table('dbo_1.silver_diagnoses').count()} total Silver rows)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_diagnoses.filter("facility_key IS NULL").count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_diagnoses.filter("patient_key IS NULL").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_diagnoses.filter("seq_num = 1").filter("hrrp_cohort IS NOT NULL").count()

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
