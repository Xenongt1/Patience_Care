# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9197c9e0-a0c4-4048-bed4-90c28f229c4d",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "f86bd9be-e828-420b-b5c6-77937de2fe75",
# META       "known_lakehouses": [
# META         {
# META           "id": "9197c9e0-a0c4-4048-bed4-90c28f229c4d"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # **A function that helps me to check the columns of the various data**

# CELL ********************

import glob
import os
import pandas as pd


# ============================================================
# 1. Define CSV folders
# ============================================================

folders = [
    "reference/dim_facility",
    "reference/dim_unit",
    "reference/dim_payer",
    "reference/dim_drug",
    "reference/dim_staff",
    "beds/hourly_snapshot",
    "beds/nhsn_weekly",
    "claims/claim_header",
    "claims/claim_line",
    "claims/remit",
    "claims/remit_adjustment",
    "ehr/patients",
    "ehr/encounters",
    "ehr/admissions",
    "ehr/ed_stays",
    "ehr/transfers",
    "ehr/diagnoses",
    "pharmacy/inventory"
]


# ============================================================
# 2. Read staff schedule Excel files
# ============================================================

staff_schedule_path = "/lakehouse/default/Files/raw/sharepoint/staff_schedules"

# Find all Excel files in the directory
excel_files = glob.glob(
    os.path.join(staff_schedule_path, "*.xlsx")
)

print("Excel files found:")
for file in excel_files:
    print(f"  - {file}")

print(f"\nTotal Excel files found: {len(excel_files)}")
print("---")


# ============================================================
# 3. Read the staff_schedule sheet from each Excel file
# ============================================================

staff_schedule_dfs = []

for file in excel_files:
    try:
        print(f"Reading: {file}")

        # Check the sheets in the workbook
        excel_file = pd.ExcelFile(file)

        if "Roster" not in excel_file.sheet_names:
            print(
                f"  SKIPPED — 'staff_schedule' sheet not found. "
                f"Available sheets: {excel_file.sheet_names}"
            )
            print("---")
            continue

        # Read the staff_schedule sheet
        df = pd.read_excel(
            file,
            sheet_name="Roster"
        )

        # Add source file so we know where each row came from
        df["source_file"] = os.path.basename(file)

        staff_schedule_dfs.append(df)

        print(f"  Loaded {len(df)} rows")
        print(f"  Columns: {list(df.columns)}")
        print("---")

    except Exception as e:
        print(f"  FAILED — {e}")
        print("---")


# ============================================================
# 4. Combine all staff schedule Excel files
# ============================================================

if staff_schedule_dfs:

    staff_schedules = pd.concat(
        staff_schedule_dfs,
        ignore_index=True
    )

    print("Combined staff schedule:")
    print(f"Rows: {len(staff_schedules)}")
    print(f"Columns: {list(staff_schedules.columns)}")
    print("---")

    # Convert pandas DataFrame → Spark DataFrame
    staff_df = spark.createDataFrame(staff_schedules)

    print("Spark staff schedule:")
    print(staff_df.columns)
    print("---")

else:

    print("No Excel files containing the 'staff_schedule' sheet were found.")
    staff_df = None


# ============================================================
# 5. Read all CSV folders using Spark
# ============================================================

for folder in folders:

    try:

        df = spark.read.csv(
            f"Files/raw/{folder}/*.csv",
            header=True,
            inferSchema=False
        )

        print(f"{folder}:")
        print(df.columns)
        print("---")

    except Exception as e:

        print(f"{folder}: FAILED — {e}")
        print("---")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

def land_bronze(folder_path, columns, table_name):
    """
    folder_path: the raw/ subfolder this table's files live in, e.g. 'reference/dim_facility'
    columns:     list of column names, in order, as they appear in the CSV header
    table_name:  the Bronze Delta table name to write to, e.g. 'bronze_facility'
    """
    schema = StructType([StructField(c, StringType(), True) for c in columns])

    df = spark.read.csv(f"Files/raw/{folder_path}/*.csv", header=True, schema=schema)

    df = (df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_batch_id", F.regexp_extract(F.col("_source_file"), r'(\d{8})\.csv$', 1))
    )

    try:
        already_loaded = [r["_batch_id"] for r in
                           spark.read.table(table_name).select("_batch_id").distinct().collect()]
    except:
        already_loaded = []

    df_new = df.filter(~F.col("_batch_id").isin(already_loaded)) if already_loaded else df

    row_count = df_new.count()
    df_new.write.format("delta").mode("append").saveAsTable(table_name)
    print(f"{table_name}: landed {row_count} new rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

land_bronze("reference/dim_facility",
    ['facility_id', 'facility_name', 'facility_type', 'region', 'city', 'state', 'zip',
     'county', 'emergency_services', 'licensed_beds', 'staffed_beds', 'ownership'],
    "bronze_facility")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

land_bronze("reference/dim_unit",
    ['unit_id', 'facility_id', 'unit_code', 'unit_name', 'unit_type', 'building', 'floor',
     'licensed_beds', 'staffed_beds', 'blocked_beds', 'nurse_patient_ratio_target',
     'is_critical_care', 'is_monitored'],
    "bronze_unit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

land_bronze("reference/dim_payer",
    ['payer_id', 'payer_name', 'payer_type', 'claim_filing_indicator_code', 'share',
     'prompt_pay_days'],
    "bronze_payer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

land_bronze("reference/dim_payer",
    ['payer_id', 'payer_name', 'payer_type', 'claim_filing_indicator_code', 'share',
     'prompt_pay_days'],
    "bronze_payer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

land_bronze("reference/dim_drug",
    ['rxcui_scd', 'rxcui_in', 'ndc11', 'product_ndc', 'gtin14', 'proprietary_name',
     'non_proprietary_name', 'dosage_form_name', 'route_name', 'pharm_classes',
     'dea_schedule', 'dea_drug_code', 'is_controlled', 'labeler_name', 'unit_cost',
     'usage_weight', 'is_shortage_prone', 'is_high_alert', 'ndc_source', 'abc_class'],
    "bronze_drug")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

land_bronze("reference/dim_staff",
    ['staff_id', 'npi', 'first_name', 'last_name', 'job_code', 'job_title', 'credential',
     'primary_facility_id', 'primary_unit_id', 'employment_type', 'fte', 'hire_date',
     'termination_date', 'is_active'],
    "bronze_staff")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

land_bronze("beds/hourly_snapshot",
    ['snapshot_datetime', 'facility_id', 'unit_id', 'unit_code', 'licensed_beds',
     'staffed_beds', 'blocked_beds', 'occupied_beds', 'available_beds',
     'pending_admissions', 'pending_discharges', 'occupancy_rate', 'is_at_capacity'],
    "bronze_bed_hourly")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

land_bronze("beds/nhsn_weekly",
    ['nhsn_org_id', 'facility_name', 'week_ending_date', 'collection_date',
     'all_hospital_inpatient_beds', 'all_hospital_inpatient_occupancy',
     'all_adult_inpatient_beds', 'all_adult_inpatient_occupancy',
     'all_pediatric_inpatient_beds', 'all_pediatric_inpatient_occupancy',
     'all_icu_beds', 'all_icu_bed_occupancy', 'adult_icu_beds',
     'adult_icu_bed_occupancy', 'pediatric_icu_beds', 'pediatric_icu_bed_occupancy'],
    "bronze_bed_nhsn")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

land_bronze("claims/claim_header",
    ['patient_control_number', 'encounter_id', 'hadm_id', 'subject_id', 'facility_id',
     'total_charge_amount', 'claim_filing_indicator_code', 'payer_id', 'payer_name',
     'type_of_bill', 'statement_date_from', 'statement_date_to',
     'admission_date_and_hour', 'discharge_time', 'admission_type_code',
     'admission_source_code', 'patient_status_code', 'drg_code', 'principal_diagnosis',
     'admitting_diagnosis', 'other_diagnoses', 'attending_provider_npi',
     'medical_record_number', 'prior_authorization_number', 'is_readmission_related',
     'submission_date'],
    "bronze_claim_header")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

land_bronze("claims/claim_line",
    ['patient_control_number', 'line_control_number', 'revenue_code',
     'revenue_code_description', 'procedure_code', 'procedure_code_qualifier',
     'procedure_description', 'line_charge_amount', 'unit_type', 'unit_count',
     'non_covered_amount', 'service_date_from', 'service_date_to'],
    "bronze_claim_line")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

land_bronze("claims/remit",
    ['patient_control_number', 'payer_id', 'claim_status_code',
     'claim_status_description', 'total_claim_charge_amount', 'claim_payment_amount',
     'patient_responsibility_amount', 'payer_claim_control_number', 'drg_code',
     'drg_weight', 'check_eft_trace_number', 'payment_method_code', 'check_date',
     'remit_date', 'is_appealed', 'is_overturned_on_appeal'],
    "bronze_remit")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

land_bronze("claims/remit_adjustment",
    ['patient_control_number', 'adjustment_seq', 'group_code', 'group_code_description',
     'reason_code', 'reason_code_description', 'amount', 'quantity', 'remark_code',
     'remark_code_description', 'is_denial'],
    "bronze_remit_adjustment")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

land_bronze("ehr/patients",
    ['Id', 'BirthDate', 'DeathDate', 'SSN', 'Drivers', 'Passport', 'Prefix', 'First',
     'Middle', 'Last', 'Suffix', 'Maiden', 'Marital', 'Race', 'Ethnicity', 'Gender',
     'BirthPlace', 'Address', 'City', 'State', 'County', 'FIPS_County_Code', 'Zip',
     'Lat', 'Lon', 'Healthcare_Expenses', 'Healthcare_Coverage', 'Income', 'phone',
     'email', 'is_high_risk', 'mrn', 'source_facility_id'],
    "bronze_patients")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

land_bronze("ehr/encounters",
    ['Id', 'Start', 'Stop', 'Patient', 'Organization', 'Provider', 'Payer',
     'EncounterClass', 'Code', 'Description', 'Base_Encounter_Cost',
     'Total_Claim_Cost', 'Payer_Coverage', 'ReasonCode', 'ReasonDescription',
     'facility_id', 'unit_id', 'encounter_class_code', 'encounter_status',
     'patient_class', 'mrn', 'source_system'],
    "bronze_encounters")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

land_bronze("ehr/admissions",
    ['subject_id', 'hadm_id', 'admittime', 'dischtime', 'deathtime', 'admission_type',
     'admit_provider_id', 'admission_location', 'discharge_location', 'insurance',
     'language', 'marital_status', 'race', 'edregtime', 'edouttime',
     'hospital_expire_flag', 'facility_id', 'admission_type_code',
     'admit_decision_time', 'hospital_service', 'transferred_in_within_6h',
     'is_readmission', 'is_planned_readmission', 'index_encounter_id', 'encounter_id',
     'drg_code', 'source_system'],
    "bronze_admissions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

land_bronze("ehr/ed_stays",
    ['subject_id', 'stay_id', 'encounter_id', 'hadm_id', 'intime', 'outtime', 'gender',
     'race', 'arrival_transport', 'disposition', 'temperature', 'heartrate',
     'resprate', 'o2sat', 'sbp', 'dbp', 'pain', 'acuity', 'chiefcomplaint',
     'triage_time', 'provider_seen_time', 'admit_decision_time', 'facility_id',
     'source_system'],
    "bronze_ed_stays")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

land_bronze("ehr/transfers",
    ['subject_id', 'hadm_id', 'transfer_id', 'eventtype', 'careunit', 'intime',
     'outtime', 'facility_id', 'unit_id'],
    "bronze_transfers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

land_bronze("ehr/diagnoses",
    ['subject_id', 'hadm_id', 'encounter_id', 'seq_num', 'icd_code', 'icd_version',
     'icd_title', 'hrrp_cohort', 'facility_id'],
    "bronze_diagnoses")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

land_bronze("pharmacy/inventory",
    ['snapshot_date', 'counting_datetime', 'count_type', 'facility_id', 'location_id',
     'ndc11', 'product_ndc', 'gtin14', 'rxcui_scd', 'drug_name', 'dosage_form_name',
     'route_name', 'pharm_classes', 'lot_number', 'expiration_date', 'qty_on_hand',
     'base_unit', 'qty_on_order', 'par_level', 'reorder_point', 'safety_stock',
     'avg_daily_usage_30d', 'days_on_hand', 'abc_class', 'is_controlled',
     'dea_schedule', 'is_high_alert', 'shortage_status', 'shortage_reason',
     'unit_cost', 'extended_value', 'last_count_variance', 'last_restocked_at',
     'is_stockout'],
    "bronze_pharmacy_inventory")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd

sample_file = "/lakehouse/default/Files/raw/sharepoint/staff_schedules/staff_schedule_140401_20260810.xlsx"

# Read with NO header assumption — just show the raw grid, first 10 rows
raw = pd.read_excel(sample_file, sheet_name="Roster", header=None, nrows=10)
for i, row in raw.iterrows():
    print(f"Row {i}: {list(row)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import glob
import os
import pandas as pd
from pyspark.sql import functions as F

def land_bronze_staff_schedules():
    path = "/lakehouse/default/Files/raw/sharepoint/staff_schedules"
    excel_files = glob.glob(os.path.join(path, "*.xlsx"))

    dfs = []
    for file in excel_files:
        # header=4 means row index 4 (5th row) holds the real column names —
        # rows 0-3 are the title block, blank line, and confidentiality notice
        df = pd.read_excel(file, sheet_name="Roster", header=4)
        df["_source_file"] = os.path.basename(file)
        dfs.append(df)

    combined = pd.concat(dfs, ignore_index=True)

    # Bronze contract: every column is STRING, no exceptions — this also fixes
    # the Arrow conversion warning you saw, which was caused by mixed types
    # (numbers vs text) across the concatenated files
    combined = combined.astype(str)

    # Delta doesn't allow spaces or special characters in column names, so
    # rename here — this is a naming fix only, not a value transformation,
    # the actual data in every cell stays exactly as-is
    combined.columns = [
        c.strip().lower().replace(" ", "_").replace("(", "").replace(")", "")
        for c in combined.columns
    ]

    spark_df = spark.createDataFrame(combined)

    spark_df = (spark_df
        .withColumn("_ingested_at", F.current_timestamp())
        # date sits between the second underscore and .xlsx, e.g. ..._140401_20260810.xlsx
        .withColumn("_batch_id", F.regexp_extract(F.col("_source_file"), r'_(\d{8})\.xlsx$', 1))
    )

    try:
        already_loaded = [r["_batch_id"] for r in
                           spark.read.table("bronze_staff_schedules").select("_batch_id").distinct().collect()]
    except:
        already_loaded = []

    df_new = spark_df.filter(~F.col("_batch_id").isin(already_loaded)) if already_loaded else spark_df

    row_count = df_new.count()
    df_new.write.format("delta").mode("append").saveAsTable("bronze_staff_schedules")
    print(f"bronze_staff_schedules: landed {row_count} new rows")

land_bronze_staff_schedules()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
