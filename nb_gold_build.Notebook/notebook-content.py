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

# CELL ********************

spark.read.table("fact_transfers").printSchema()
spark.read.table("fact_bed_capacity").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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

# # **fact_encounters**

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

# # **fact_admissions**

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

# MARKDOWN ********************

# # **fact_ed_stays**

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

# MARKDOWN ********************

# # **fact_transfers**

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

# MARKDOWN ********************

# # **fact_diagnoses**

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

# MARKDOWN ********************

# # **fact_claims**

# CELL ********************

from pyspark.sql.window import Window 

silver_claim_header_clean = spark.read.table("dbo_1.silver_claim_header").filter("_dq_passed = TRUE")
silver_remit_clean = spark.read.table("dbo_1.silver_remit").filter("_dq_passed = TRUE")

# NEW — dedupe remit to one row per claim (most recent by remit_date) before
# joining, since some claims have multiple remit rows and joining against all
# of them fans a single claim out into duplicate fact_claims rows
remit_w = Window.partitionBy("patient_control_number").orderBy(F.col("remit_date").desc())
silver_remit_dedup = (silver_remit_clean
    .withColumn("_rn", F.row_number().over(remit_w))
    .filter("_rn = 1")
    .drop("_rn")
)

fac_lookup = spark.read.table("dim_facility").select("facility_key", "facility_id")
payer_lookup = spark.read.table("dim_payer").select("payer_key", "payer_id")
patient_lookup = spark.read.table("dim_patient").select("patient_key", "canonical_patient_key")

staff_lookup = (spark.read.table("dim_staff")
    .filter("is_current = true")
    .dropDuplicates(["npi"])
    .select(F.col("staff_key").alias("attending_staff_key"), "npi")
)

fact_claims = (silver_claim_header_clean
    .join(
        silver_remit_dedup.select(   # CHANGED — was silver_remit_clean, now the deduped version
            "patient_control_number",
            F.col("claim_status_code").alias("remit_claim_status_code"),
            F.col("claim_payment_amount_float").alias("claim_payment_amount"),
            F.col("patient_responsibility_amount").alias("patient_responsibility_amount"),
            F.col("is_appealed_bool").alias("is_appealed"),
            F.col("is_overturned_on_appeal_bool").alias("is_overturned_on_appeal"),
            F.col("remit_date").alias("remit_date")
        ),
        on="patient_control_number", how="left"
    )
    .join(fac_lookup, on="facility_id", how="left")
    .join(payer_lookup, on="payer_id", how="left")
    .join(patient_lookup, on="canonical_patient_key", how="left")
    .join(staff_lookup, F.col("attending_provider_npi") == F.col("npi"), how="left")
    .withColumn("submission_date_key", F.date_format("submission_date", "yyyyMMdd").cast("int"))
    .withColumn("is_paid", F.col("claim_payment_amount").isNotNull() & (F.col("claim_payment_amount") > 0))
    .withColumn("amount_at_risk", F.col("total_charge_amount_float") - F.coalesce(F.col("claim_payment_amount"), F.lit(0)))
)

fact_claims = fact_claims.select(
    "patient_control_number",
    F.col("encounter_id").alias("source_encounter_id"),
    "hadm_id",
    "facility_key", "payer_key", "patient_key", "attending_staff_key",
    "submission_date_key",
    F.col("total_charge_amount_float").alias("total_charge_amount"),
    "claim_payment_amount", "patient_responsibility_amount", "amount_at_risk", "is_paid",
    "drg_code", "type_of_bill", "admission_type_code",
    F.col("is_readmission_related_bool").alias("is_readmission_related"),
    "remit_claim_status_code", "is_appealed", "is_overturned_on_appeal", "remit_date"
)

fact_claims.write.format("delta").mode("overwrite").saveAsTable("fact_claims")
print(f"fact_claims: {fact_claims.count()} rows (gated from {spark.read.table('dbo_1.silver_claim_header').count()} total Silver rows)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_claims.filter("amount_at_risk < 0").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_claims.filter("facility_key IS NULL").count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_claims.filter("payer_key IS NULL").count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_claims.filter("claim_payment_amount IS NULL").count()  # claims with no remit yet — expected to be nonzero


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_claims.filter("amount_at_risk < 0").select("patient_control_number", "total_charge_amount", "claim_payment_amount").show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_claims.filter("amount_at_risk < 0").count()  # would mean paid MORE than charged — worth investigating if nonzero

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# fact_claim_lines — one row per claim line, linked to fact_claims by patient_control_number

# CELL ********************

silver_claim_line_clean = spark.read.table("dbo_1.silver_claim_line").filter("_dq_passed = TRUE")

fact_claim_lines = silver_claim_line_clean.select(
    "patient_control_number",
    "line_control_number",
    "revenue_code", "revenue_code_description",
    "procedure_code", "procedure_description",
    F.col("line_charge_amount_float").alias("line_charge_amount"),
    F.col("unit_count_int").alias("unit_count"),
    "non_covered_amount",
    "service_date_from", "service_date_to"
)

fact_claim_lines.write.format("delta").mode("overwrite").saveAsTable("fact_claim_lines")
print(f"fact_claim_lines: {fact_claim_lines.count()} rows (gated from {spark.read.table('dbo_1.silver_claim_line').count()} total Silver rows)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

valid_claim_numbers = [r["patient_control_number"] for r in fact_claims.select("patient_control_number").collect()]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_claim_lines.filter(~F.col("patient_control_number").isin(valid_claim_numbers)).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.read.table("dbo_1.silver_claim_header").filter("_dq_passed = FALSE").filter("_dq_issues LIKE '%payer_id%'").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_claims.filter("amount_at_risk < 0").select("total_charge_amount", "claim_payment_amount").show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.read.table("dbo_1.silver_remit").filter("_dq_passed = TRUE").groupBy("patient_control_number").count().filter("count > 1").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

suspects = fact_claims.filter("amount_at_risk < 0").select("patient_control_number").distinct()

fact_claims.join(suspects, on="patient_control_number").groupBy("patient_control_number").count().orderBy(F.col("count").desc()).show(20)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_claims = spark.read.table("dbo_1.silver_claim_header").filter("_dq_passed = TRUE")
raw_claims.join(suspects, on="patient_control_number").groupBy("patient_control_number", "attending_provider_npi").count().show(20, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Fact_claim_Adjustments- the last Claims fact**

# CELL ********************

silver_remit_adjustment_clean = spark.read.table("dbo_1.silver_remit_adjustment").filter("_dq_passed = TRUE")

fact_claim_adjustments = silver_remit_adjustment_clean.select(
    "patient_control_number",
    F.col("adjustment_seq").alias("adjustment_seq"),
    "group_code", "group_code_description",
    "reason_code", "reason_code_description",
    F.col("amount_float").alias("amount"),
    "quantity",
    "remark_code", "remark_code_description",
    F.col("is_denial_bool").alias("is_denial")
)

fact_claim_adjustments.write.format("delta").mode("overwrite").saveAsTable("fact_claim_adjustments")
print(f"fact_claim_adjustments: {fact_claim_adjustments.count()} rows (gated from {spark.read.table('dbo_1.silver_remit_adjustment').count()} total Silver rows)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **Operational Facts**

# CELL ********************

from pyspark.sql import functions as F

silver_bed_hourly_clean = spark.read.table("dbo_1.silver_bed_hourly").filter("_dq_passed = TRUE")

fac_lookup = spark.read.table("dim_facility").select("facility_key", "facility_id")
unit_lookup = spark.read.table("dim_unit").select("unit_key", "unit_id")

fact_bed_capacity = (silver_bed_hourly_clean
    .join(fac_lookup, on="facility_id", how="left")
    .join(unit_lookup, on="unit_id", how="left")
    .withColumn("snapshot_date_key", F.date_format("snapshot_datetime_ts", "yyyyMMdd").cast("int"))
    .withColumn("snapshot_hour", F.hour("snapshot_datetime_ts"))
)

fact_bed_capacity = fact_bed_capacity.select(
    "facility_key", "unit_key",
    "snapshot_date_key", "snapshot_hour", "snapshot_datetime_ts",
    F.col("licensed_beds").cast("int").alias("licensed_beds"),
    F.col("staffed_beds").cast("int").alias("staffed_beds"),
    F.col("blocked_beds").cast("int").alias("blocked_beds"),
    F.col("occupied_beds_int").alias("occupied_beds"),
    F.col("available_beds_int").alias("available_beds"),
    F.col("occupancy_rate_float").alias("occupancy_rate"),
    F.col("is_at_capacity_bool").alias("is_at_capacity"),
    F.col("pending_admissions").cast("int").alias("pending_admissions"),
    F.col("pending_discharges").cast("int").alias("pending_discharges")
)

fact_bed_capacity.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fact_bed_capacity")
print(f"fact_bed_capacity: {fact_bed_capacity.count()} rows (gated from {spark.read.table('dbo_1.silver_bed_hourly').count()} total Silver rows)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.read.table("fact_bed_capacity").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_bed_capacity.filter("facility_key IS NULL").count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_bed_capacity.filter("unit_key IS NULL").count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_bed_capacity.groupBy("facility_key","unit_key","snapshot_datetime_ts").count().filter("count > 1").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **fact_Pharmacy_Inventory**

# CELL ********************

silver_pharmacy_inventory_clean = spark.read.table("dbo_1.silver_pharmacy_inventory").filter("_dq_passed = TRUE")

fac_lookup = spark.read.table("dim_facility").select("facility_key", "facility_id")
drug_lookup = spark.read.table("dim_drug").select("drug_key", "ndc11")

fact_pharmacy_inventory = (silver_pharmacy_inventory_clean
    .join(fac_lookup, on="facility_id", how="left")
    .join(drug_lookup, on="ndc11", how="left")
    .withColumn("snapshot_date_key", F.date_format("snapshot_date", "yyyyMMdd").cast("int"))
)

fact_pharmacy_inventory = fact_pharmacy_inventory.select(
    "facility_key", "drug_key",
    "snapshot_date_key", "counting_datetime", "count_type", "location_id",
    F.col("qty_on_hand_int").alias("qty_on_hand"),
    "qty_on_order",
    F.col("reorder_point_int").alias("reorder_point"),
    "safety_stock", "avg_daily_usage_30d", "days_on_hand",
    F.col("is_stockout_bool").alias("is_stockout"),
    "shortage_status", "shortage_reason",
     F.col("unit_cost").cast("float").alias("unit_cost"), 
    "extended_value"
)

fact_pharmacy_inventory.write.format("delta").mode("overwrite").saveAsTable("fact_pharmacy_inventory")
print(f"fact_pharmacy_inventory: {fact_pharmacy_inventory.count()} rows (gated from {spark.read.table('dbo_1.silver_pharmacy_inventory').count()} total Silver rows)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # **fact_staffing - the last fact table**

# CELL ********************

silver_staff_schedules_clean = spark.read.table("dbo_1.silver_staff_schedules").filter("_dq_passed = TRUE")

fac_lookup = spark.read.table("dim_facility").select("facility_key", "facility_id")
unit_lookup = spark.read.table("dim_unit").select("unit_key", "unit_code", "facility_id")

staff_lookup = (spark.read.table("dim_staff")
    .filter("is_current = true")
    .dropDuplicates(["staff_id"])
    .select("staff_key", "staff_id")
)

fact_staffing = (silver_staff_schedules_clean
    .join(fac_lookup, on="facility_id", how="left")
    .join(unit_lookup, on=["unit_code", "facility_id"], how="left")   # NEW — picks up unit_key
    .join(staff_lookup, on="staff_id", how="left")
    .withColumn("work_date_key", F.date_format("work_date_parsed", "yyyyMMdd").cast("int"))
)

fact_staffing = fact_staffing.select(
    "facility_key", "staff_key", "unit_key",   # unit_key added here
    "work_date_key", "unit_code", "shift",
    "shift_start", "shift_end",
    "job_code", "employment_type",
    F.col("scheduled_hours_float").alias("scheduled_hours"),
    F.col("actual_hours_float").alias("actual_hours"),
    "status", "overtime", "called_out", "floated_in", "census"
)

fact_staffing.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("fact_staffing")
print(f"fact_staffing: {fact_staffing.count()} rows (gated from {spark.read.table('dbo_1.silver_staff_schedules').count()} total Silver rows)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_staffing.filter("unit_key IS NULL").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.read.table("dbo_1.silver_staff_schedules").filter(
    (F.col("facility_id") == 5) & (F.col("staff_id") ==
        (spark.read.table("dim_staff").filter("staff_key = 2290").select("staff_id").first()["staff_id"])
    )
).filter("work_date_parsed = '2026-08-12'").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.read.table("dim_staff").filter("staff_key = 2290").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
