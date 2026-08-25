# Meridian Source Data Contract

**Owner** Mubarak Tijani — data generation & source supply
**Applies to** every file and event the Meridian analytics platform ingests
**Status** Binding for the Bronze layer. Downstream layers must conform to this, not the reverse.

---

## 1. What this document is

This is the **contract for the source layer**: the exact tables, columns, types, key
relationships and value domains that will land in Bronze. It is generated from a real
generator run, not from intent — every column list, null rate and enum domain below was read
out of the emitted files.

Two things follow from that:

- **This document is the authority on what arrives.** If a downstream design assumes a
  different column name, type or grain, the downstream design is wrong. That is not a claim
  about design quality — the generator stands in for real EHR, billing, pharmacy, capacity and
  rostering systems, so its output is the source system by definition.
- **This document is not the warehouse model.** Nothing here is dimensional. Bronze lands it
  verbatim; Silver conforms it; Gold models it. Conforming, masking, surrogate keys and any
  dimensional model are deliberately left to those layers.

### How to regenerate

```bash
python run.py --days 120          # produces ./out
python validate.py out            # proves the KPIs are computable from it
```

Reproducible from `--seed` + `--start` + `--days`.

> **Choose the window to fit the KPI.** Readmission rate is right-censored by 30 days: an index
> admission discharged inside the final 30 days of the window has not yet been observed long
> enough to count. A run shorter than ~60 days leaves too few eligible index admissions for the
> rate to mean anything, and `validate.py` will correctly report it as **not computable** rather
> than print a number. Use **≥120 days** for anything readmission-related. Short runs remain fine
> for schema, DQ and staffing work.

Column lists, null rates and enum domains in this document were read out of a 21-day
`--no-streams` run; stream domains come from a separate run with streams enabled. Row counts are
deliberately not pinned — see §3.

### Reading the column tables

| Notation | Meaning |
|---|---|
| `PK` | Primary key of this table |
| `AK` | Alternate key — unique, but not the chosen PK |
| `FK <target>` | Foreign key; the target is given |
| Type | **Inferred from actual values.** Every CSV field is physically a string on arrival; the type given is what it will cast to cleanly in Silver. |
| Null % | Measured share of empty values in the run. Non-zero values are a mix of legitimate nulls and injected defects — see §8. |
| Domain | Full value set where cardinality ≤ 25, otherwise the three most frequent values. |

> **Values marked "DQ-invalid by design"** are injected data-quality defects, not schema
> members. Do not add them to a Silver enum. They exist so the DQ gate has something to
> catch — see §8.

---

## 2. Client vocabulary → actual schema names

The client request and the source systems use different words for the same things, and in a
few cases the same word for different things. **Every mapping below is a place a requirement
can be mis-implemented.** The right-hand column is what the field is actually called.

### 2.1 Entities

| Client request says | Actual name | Note |
|---|---|---|
| "patient visit/admission records" | `ehr/encounters` **+** `ehr/admissions` **+** `ehr/ed_stays` | Not one table. A visit that arrives at ED and is admitted is **three rows in three tables**, joined by `hadm_id`. |
| "patient visits" (the countable thing) | `ehr/encounters` — one row per encounter | `encounters.Id` (`ENC…`) is the visit identifier |
| "department" / "unit" | `dim_unit` · `unit_id` | The schema says **unit**, never department. `unit_id` looks like `330101-MICU`. |
| "facility" | `dim_facility` · `facility_id` | `facility_id` is a **6-digit CMS CCN-style number** (`330101`), not a UUID |
| "staff" / "nursing supervisors" | `dim_staff` · `staff_id` (`STF000001`), `npi` | |
| "patient" | `ehr/patients` · `Id` (UUID) | The **only** identifier in the platform that is genuinely a UUID |
| "diagnoses" | `ehr/diagnoses` · `icd_code`, `icd_title` | ICD-10-CM. Multiple rows per stay; `seq_num = 1` is the principal diagnosis |
| "bed capacity snapshots" | `beds/hourly_snapshot` **+** `beds/nhsn_weekly` | Two grains, deliberately. The hourly feed is per **unit-hour**, not per department-day. |
| "staff schedules" | `sharepoint/staff_schedules` — sheet `Roster` | XLSX, and the real header is on **row 5** |
| "billing and claims extracts" | `claims/claim_header`, `claim_line`, `remit`, `remit_adjustment` | Four tables: 837I header/line, 835 remittance/adjustment |
| "pharmacy inventory levels" | `pharmacy/inventory` | |
| "patient vital-sign readings" | `stream/patient-vitals` | One event **per parameter per reading** (tall/EAV), six parameters |
| "prescription-issuance events" | `stream/prescription-events` | |
| "payer" / "insurance company" | `dim_payer` · `payer_id` (`PAY001`), `payer_name` | |

### 2.2 Measures — where the naming gap is dangerous

| Client request says | Actual definition | Trap |
|---|---|---|
| "patient wait times" | Three distinct CMS measures: **OP-18** (ED arrival → departure, discharged), **ED-1** (same, admitted), **ED-2** (admit decision → departure, i.e. boarding) | "Wait time" is not one number. Reporting a single blended figure is wrong, and the three move independently — measured 149 / 394 / 181 minutes. Built from `ed_stays.intime`, `outtime`, `triage_time`, `admit_decision_time`. |
| "denied claims" | `remit.claim_status_code = 4` (X12 CLP02) | **A contractual write-off is not a denial.** `remit_adjustment` CARC `CO-45` is 50% of adjustment rows and carries `is_denial = 0`. Counting adjustments as denials overstates the rate by roughly 10×. |
| "delayed payments" | `remit.remit_date` − `claim_header.submission_date` | Days in A/R. Measured median 52 days. |
| "revenue at risk" | `claim_header.total_charge_amount` − `remit.claim_payment_amount`, restricted to `claim_status_code = 4` | Must exclude write-offs, per above |
| "readmission rate" | CMS HRRP: `admissions.is_readmission` **excluding** planned readmissions, deaths, AMA discharges and psych, one per index, with the trailing 30 days right-censored | `diagnoses.hrrp_cohort` carries the cohort (AMI / HF / PN / COPD / OTHER). A naive count over-reports. |
| "staffing ratios" | Actual patients-per-nurse vs `dim_unit.nurse_patient_ratio_target` | Targets are **per unit type** (Cal. Title 22 §70217: ICU 1:2, SDU 1:3, ED 1:4, Telemetry 1:4, Med-Surg 1:5). Collapsing unit types destroys this measure. |
| "bed occupancy" | `occupied_beds` / **`staffed_beds`** | Three bed counts exist — `licensed_beds`, `staffed_beds`, `blocked_beds`. NHSN and operational practice use **staffed**. Using licensed understates occupancy and will not reconcile against `beds/nhsn_weekly`. |
| "at risk of running out of bed capacity" | `is_at_capacity`, a **source column**, true at `occupancy_rate >= 0.85` | `occupancy_rate` is on a **0–1 scale**, not 0–100. A 0–100 range check passes silently and every figure is off by 100×. Do not re-derive this flag at a different threshold. |
| "at risk of a pharmacy stockout" | `days_on_hand` vs `reorder_point`; `is_stockout` for an actual zero | `par_level`, `reorder_point` and `safety_stock` are **our additions** — FHIR `InventoryReport` has no reorder concept, so without them "at risk" cannot be expressed |
| "critical patient-monitoring alerts" | **Derived in Silver** — NEWS2 aggregate from the six vitals parameters. Thresholds: ≥5 urgent, ≥7 emergency | **Not emitted.** The stream carries `warning` (single-parameter artifact flag) and `is_artifact`, which are *not* NEWS2. |
| "patient wait times in **outpatient** departments" | `outpatient_visits`: `arrival_time → provider_seen_time` (patient wait), or `appointment_time → provider_seen_time` (appointment adherence) | Two different questions — pick one deliberately. **Exclude `is_no_show = 1`**, whose timestamps are legitimately null; counting them as a zero wait understates the metric. Early arrival makes `arrival_time − appointment_time` negative and is not an inversion. |
| "patient wait times in **emergency** departments" | `ed_stays`: `triage_time → provider_seen_time` (door-to-doctor) | Not the same interval as the outpatient one — ED has no appointment, so there is no adherence measure. Do not average the two together. |

### 2.3 Platform vocabulary

| Client request says | Actual |
|---|---|
| "secure internal document library" | SharePoint document library — `staff_schedules` XLSX only |
| "secure cloud file storage" | OneLake Files (ADLS Gen2 API), path `<Lakehouse>.Lakehouse/Files/<prefix>/…` |
| "Kafka-compatible managed service" | Microsoft Fabric Eventstream, custom endpoint source, Kafka protocol + SASL_SSL |
| "raw / cleansed / curated" | Bronze / Silver / Gold |
| "government identifiers" | `ehr/patients`: `SSN`, `Drivers`, `Passport` |
| "contact details" | `ehr/patients`: `Address`, `City`, `State`, `Zip`, `Lat`, `Lon`, `phone`, `email` |
| "patient names, dates of birth" | `ehr/patients`: `Prefix`, `First`, `Middle`, `Last`, `Suffix`, `Maiden`, `BirthDate` |

---

## 3. Feeds, landing and cadence

| # | Feed | Landing | Cadence | Format | Tables |
|---|---|---|---|---|---|
| 1 | EHR encounters & admissions | OneLake Files | Daily | CSV | 7 <sup>†</sup> |
| 2 | Billing & claims | OneLake Files | Daily | CSV | 4 |
| 3 | Pharmacy inventory | OneLake Files | Daily snapshot | CSV | 1 |
| 4 | Bed capacity | OneLake Files | Hourly + weekly roll-up | CSV | 2 |
| 5 | Staff schedules | SharePoint | Weekly, per facility | XLSX | 1 |
| 6 | Patient vitals | Eventstream (Kafka) | Continuous, 5-min archive | JSON | 1 topic |
| 7 | Prescription issuance | Eventstream (Kafka) | Continuous | JSON | 1 topic |
| — | Reference dimensions | OneLake Files | Day 0 + Mondays, full refresh | CSV | 7 <sup>‡</sup> |

<sup>†</sup> includes `ehr/outpatient_visits`, the highest-volume feed in the contract — outpatient
is the bulk of visits, so expect it to dominate row counts and sizing.

<sup>‡</sup> includes `reference/dim_icd10` and `reference/dim_drg`. Note **`dim_staff` now
changes between snapshots** (hires, terminations, unit transfers), so successive full refreshes
are no longer byte-identical — it is SCD-2 material, and a pipeline that overwrites it loses
history.

**Batch path convention** `out/batch/<source>/<table>/<table>_YYYYMMDD.csv` — mirrors the target
OneLake layout exactly, so `--onelake-*` writes the identical paths into the lakehouse.

> **Row counts are not pinned in this document.** Transaction-feed volume scales with `--days`,
> `--seed` and `--chaos`, so any figure written here would be wrong for your run. Read the counts
> from the run you generated. Only the reference dimensions carry fixed per-snapshot sizes,
> because those do not scale with the window.

**Stream archive** `out/stream/<topic>.jsonl.gz` — gzipped by default (17× on vitals). Kept even
when publishing to Kafka, because Eventstream has no Event Hubs Capture equivalent and this is
the only Bronze replay path.

> **Cadence note.** Claims arrive **daily**, not weekly. Bed capacity is **hourly**, not
> end-of-day. Both differ from earlier batch-layer assumptions and both drive the freshness check.

> **Empty partitions are still partitions.** Claims bill discharges 2–6 days back, so the first
> two days of any window have nothing to bill — everything that far back pre-dates the extract.
> Those days land as **header-only CSVs with zero data rows**, not as absent files. A zero-row
> file that arrives on time is a healthy feed with nothing to say; an absent file is a failure.
> Your freshness check must tell them apart, and your row-count reconciliation must not read
> zero as a truncation. The only genuinely absent partitions are the injected `missing_file`
> defects listed in `out/dq_answer_key.json`.

---

## 4. Event envelope (both streams)

Every streamed event shares one envelope. Three fields carry engineering weight:

| Field | Type | Contract |
|---|---|---|
| `event_id` | UUID | **Deterministic** — `uuid5` over encounter + device + code + timestamp. A re-run of the same seed produces identical ids, which makes dedupe testable rather than assumed. |
| `event_time` | TIMESTAMP | When the event occurred. **Watermark on this.** |
| `ingest_time` | TIMESTAMP | When it was received. `ingest_time − event_time` is the arrival lag; injected late events widen it deliberately. |
| `source_system` | STRING | Emitting system, e.g. `PHILIPS_IX_MONITOR`, `PHARMACY_OMS` |
| `facility_id` | STRING | FK `dim_facility` |
| `schema_version` | STRING | Currently `1.0`. Bump on any breaking payload change. |
| `payload` | OBJECT | Feed-specific; profiled separately below |

> `event_time` is **simulation clock, not wall clock** — the generator writes 7 days of
> telemetry in ~90 seconds. Treat the archive as a replay buffer; anything that must look live
> needs a replayer pacing by `event_time`.

---

## 5. Tables


### — Reference dimensions —

*Anchored on: Conformed dimensions; CMS CCN facility numbering, NPI, DEA schedules*


### `reference/dim_facility`

**Landing** OneLake Files · **Cadence** Day 0 + every Monday (full refresh) · **Format** CSV · **Observed volume** 7 per snapshot · **Columns** 12

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `facility_id` | STRING <sup>1</sup> | PK | 0 | `030301`, `050201`, `110501`, `140401`, `330101`, `330102`, `450601` |
| `facility_name` | STRING |  | 0 | `Meridian Community Hospital – Savannah`, `Meridian General Hospital – Boston`, `Meridian General Hospital – Chicago`, `Meridian General Hospital – Oakland`, `Meridian Regional Medical Center – Phoenix`, `Meridian University Hospital`, `Meridian Urgent Care – Austin` |
| `facility_type` | STRING |  | 0 | `General Acute Care`, `Community`, `Regional`, `Teaching`, `Urgent Care` |
| `region` | STRING |  | 0 | `Northeast`, `South`, `West`, `Midwest` |
| `city` | STRING |  | 0 | `Boston`, `Austin`, `Chicago`, `Oakland`, `Phoenix`, `Savannah` |
| `state` | STRING |  | 0 | `MA`, `AZ`, `CA`, `GA`, `IL`, `TX` |
| `zip` | STRING <sup>1</sup> |  | 0 | `02118`, `02215`, `31404`, `60612`, `78702`, `85006`, `94609` |
| `county` | STRING |  | 0 | `Suffolk`, `Alameda`, `Chatham`, `Cook`, `Maricopa`, `Travis` |
| `emergency_services` | BOOLEAN |  | 0 | `1`, `0` |
| `licensed_beds` | INTEGER |  | 0 | `0`, `250`, `340`, `380`, `420`, `610`, `95` |
| `staffed_beds` | INTEGER |  | 0 | `248`, `386`, `429`, `471`, `50`, `707`, `78` |
| `ownership` | STRING |  | 0 | `Voluntary non-profit - Private`, `Proprietary`, `Government - Local`, `Voluntary non-profit - Church` |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number


### `reference/dim_unit`

**Landing** OneLake Files · **Cadence** Day 0 + every Monday (full refresh) · **Format** CSV · **Observed volume** 81 per snapshot · **Columns** 13

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `unit_id` | STRING | PK | 0 | `030301-ASC`, `030301-ED`, `030301-LD` … (81 distinct) |
| `facility_id` | STRING <sup>1</sup> | FK dim_facility | 0 | `330102`, `050201`, `140401`, `330101`, `030301`, `110501`, `450601` |
| `unit_code` | STRING |  | 0 | `ED`, `OPC`, `MICU`, `MS`, `PACU`, `TELE`, `ASC`, `LD`, `PEDS`, `PP`, `SDU`, `CVICU`, `ONC`, `PSY`, `SICU`, `NICU`, `REHAB` |
| `unit_name` | STRING |  | 0 | `Emergency Department (ED)`, `Outpatient Clinic (OPC)`, `Medical Intensive Care (MICU)`, `Medical/Surgical (MS)`, `Post-Anesthesia Care (PACU)`, `Telemetry (TELE)`, `Ambulatory Surgery (ASC)`, `Labor & Delivery (LD)`, `Pediatrics (PEDS)`, `Postpartum (PP)`, `Step-Down (SDU)`, `Cardiovascular ICU (CVICU)`, `Psychiatric (PSY)`, `Specialty Care Oncology (ONC)`, `Surgical Intensive Care (SICU)`, `Neonatal ICU (NICU)`, `Rehabilitation (REHAB)` |
| `unit_type` | STRING |  | 0 | `Emergency Department`, `Outpatient Clinic`, `Medical Intensive Care`, `Medical/Surgical`, `Post-Anesthesia Care`, `Telemetry`, `Ambulatory Surgery`, `Labor & Delivery`, `Pediatrics`, `Postpartum`, `Step-Down`, `Cardiovascular ICU`, `Psychiatric`, `Specialty Care Oncology`, `Surgical Intensive Care`, `Neonatal ICU`, `Rehabilitation` |
| `building` | STRING |  | 0 | `Main`, `North Tower` |
| `floor` | INTEGER |  | 0 | `8`, `2`, `6`, `5`, `1`, `7`, `9`, `3`, `4` |
| `licensed_beds` | INTEGER |  | 0 | `23`, `15`, `21` … (40 distinct) |
| `staffed_beds` | INTEGER |  | 0 | `13`, `16`, `20` … (42 distinct) |
| `blocked_beds` | INTEGER |  | 0 | `0`, `1`, `2`, `3`, `4`, `7` |
| `nurse_patient_ratio_target` | DECIMAL |  | 0 | `4.0`, `2.0`, `5.0`, `8.0`, `3.0`, `6.0` |
| `is_critical_care` | BOOLEAN |  | 0 | `0`, `1` |
| `is_monitored` | BOOLEAN |  | 0 | `1`, `0` |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number


### `reference/dim_staff`

**Landing** OneLake Files · **Cadence** Day 0 + every Monday (full refresh) · **Format** CSV · **Observed volume** 6,191 per snapshot · **Columns** 14

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `staff_id` | STRING | PK | 0 | `STF000001`, `STF000002`, `STF000003` … (6,213 distinct) |
| `npi` | STRING <sup>1</sup> | AK | 28.99 | `1000263212`, `1000583419`, `1000710931` … (4,410 distinct) |
| `first_name` | STRING |  | 0 | `Michael`, `Jennifer`, `Robert` … (620 distinct) |
| `last_name` | STRING |  | 0 | `Smith`, `Johnson`, `Williams` … (955 distinct) |
| `job_code` | STRING |  | 0 | `RN`, `CNA`, `MD`, `LPN`, `RT`, `UC`, `NP`, `PharmTech`, `RPh`, `PA` |
| `job_title` | STRING |  | 0 | `Registered Nurse`, `Certified Nursing Assistant`, `Physician`, `Licensed Practical Nurse`, `Respiratory Therapist`, `Unit Clerk`, `Nurse Practitioner`, `Pharmacy Technician`, `Pharmacist`, `Physician Assistant` |
| `credential` | STRING |  | 4.76 | `RN`, `CNA`, `MD`, `LPN`, `RRT`, `NP`, `CPhT`, `PharmD`, `PA-C` |
| `primary_facility_id` | STRING <sup>1</sup> | FK dim_facility | 0 | `330102`, `330101`, `050201`, `140401`, `030301`, `110501`, `450601` |
| `primary_unit_id` | STRING | FK dim_unit | 0 | `330102-ED`, `330102-PEDS`, `330102-MS` … (81 distinct) |
| `employment_type` | INTEGER |  | 0 | `1`, `2` |
| `fte` | DECIMAL |  | 0 | `1.0`, `0.6`, `0.9`, `0.8` |
| `hire_date` | DATE |  | 0 | `2024-05-14`, `2026-08-06`, `2016-10-18` … (3,224 distinct) |
| `termination_date` | DATE |  | 88.90 | `2025-10-18`, `2026-01-10`, `2026-01-20` … (334 distinct) |
| `is_active` | BOOLEAN |  | 0 | `1`, `0` |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number


### `reference/dim_payer`

**Landing** OneLake Files · **Cadence** Day 0 + every Monday (full refresh) · **Format** CSV · **Observed volume** 9 per snapshot · **Columns** 6

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `payer_id` | STRING | PK | 0 | `PAY001`, `PAY002`, `PAY003`, `PAY004`, `PAY005`, `PAY006`, `PAY007`, `PAY008`, `PAY009` |
| `payer_name` | STRING |  | 0 | `Atlas Health Commercial`, `Harborview Medicaid MC`, `Ironbridge HMO`, `Medicare Part A/B`, `Northwind PPO`, `Other / Workers Comp`, `Self-Pay`, `State Medicaid FFS`, `Vantage Medicare Advantage` |
| `payer_type` | STRING |  | 0 | `Commercial`, `Medicaid`, `Medicaid MC`, `Medicare`, `Medicare Advantage`, `Other`, `Self-Pay` |
| `claim_filing_indicator_code` | STRING |  | 0 | `CI`, `MC`, `09`, `16`, `MB`, `WC` |
| `share` | DECIMAL |  | 0 | `0.118`, `0.033`, `0.042`, `0.06`, `0.101`, `0.126`, `0.16`, `0.242` |
| `prompt_pay_days` | INTEGER |  | 0 | `40`, `30`, `45`, `0`, `60` |


### `reference/dim_drug`

**Landing** OneLake Files · **Cadence** Day 0 + every Monday (full refresh) · **Format** CSV · **Observed volume** 40 per snapshot · **Columns** 20

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `rxcui_scd` | INTEGER | PK | 0 | `1028946`, `1102306`, `1167174` … (40 distinct) |
| `rxcui_in` | INTEGER |  | 0 | `1023742`, `1042108`, `1042433` … (40 distinct) |
| `ndc11` | STRING <sup>1</sup> | AK | 0 | `10000100030`, `10037100130`, `10074100230` … (40 distinct) |
| `product_ndc` | STRING <sup>1</sup> | AK | 0 | `10000-1000`, `10037-1001`, `10074-1002` … (40 distinct) |
| `gtin14` | STRING <sup>1</sup> | AK | 0 | `00310000100030`, `00310037100137`, `00310074100234` … (40 distinct) |
| `proprietary_name` | STRING |  | 0 | `Acetaminophen`, `Albuterol`, `Alprazolam` … (40 distinct) |
| `non_proprietary_name` | STRING |  | 0 | `Acetaminophen 325 MG`, `Albuterol 2.5 MG/3ML`, `Alprazolam 0.5 MG` … (40 distinct) |
| `dosage_form_name` | STRING |  | 0 | `INJECTION, SOLUTION`, `TABLET`, `INJECTION, POWDER`, `INJECTION`, `CAPSULE`, `INHALATION SOLUTION`, `INJECTION, EMULSION`, `TABLET, DELAYED RELEASE` |
| `route_name` | STRING |  | 0 | `INTRAVENOUS`, `ORAL`, `SUBCUTANEOUS`, `RESPIRATORY` |
| `pharm_classes` | STRING |  | 0 | `Opioid Analgesic`, `Benzodiazepine`, `Cephalosporin` … (29 distinct) |
| `dea_schedule` | STRING |  | 62.50 | `CII`, `CIV`, `CIII`, `CV` |
| `dea_drug_code` | INTEGER |  | 62.50 | `2285`, `2765`, `2782`, `2882`, `2884`, `2885`, `7285`, `9064`, `9143`, `9150`, `9193`, `9250`, `9300`, `9752`, `9801` |
| `is_controlled` | BOOLEAN |  | 0 | `0`, `1` |
| `labeler_name` | STRING |  | 0 | `Adams-Scott Pharmaceuticals`, `Allen-Olson Pharmaceuticals`, `Baker, Boone and Perez Pharmaceuticals` … (40 distinct) |
| `unit_cost` | DECIMAL |  | 0 | `0.02`, `0.04`, `0.05` … (40 distinct) |
| `usage_weight` | DECIMAL |  | 0 | `1.5`, `0.5`, `0.6` … (34 distinct) |
| `is_shortage_prone` | BOOLEAN |  | 0 | `0`, `1` |
| `is_high_alert` | BOOLEAN |  | 0 | `0`, `1` |
| `ndc_source` | STRING |  | 0 | `synthetic (NDC-shaped)` |
| `abc_class` | STRING |  | 0 | `C`, `A`, `B` |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number


### `reference/dim_icd10`

**Landing** OneLake Files · **Cadence** Day 0 + every Monday (full refresh) · **Format** CSV · **Observed volume** 28 per snapshot · **Columns** 10

The code set behind `ehr/diagnoses`. Real ICD-10-CM codes and descriptions; the grouping
columns (`diagnosis_category`, `hrrp_cohort`, `readmission_risk_level`) are the generator's own
and exist so a diagnosis dimension has something to roll up to.

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `icd10_code` | STRING | PK · FK ← `ehr/diagnoses.icd_code` | 0 | `A04.72`, `A41.51`, `A41.9` … (28 distinct) |
| `icd10_description` | STRING |  | 0 | `Acute kidney failure, unspecified`, `Acute on chronic diastolic (congestive) heart failure`, `Acute on chronic systolic (congestive) heart failure` … (28 distinct) |
| `icd_version` | INTEGER |  | 0 | `10` |
| `code_chapter` | STRING |  | 0 | `I`, `J`, `R`, `A`, `E`, `N`, `K` |
| `diagnosis_category` | STRING |  | 0 | `SEPSIS`, `AMI`, `HF`, `RENAL`, `COPD`, `PN`, `ARRHYTHMIA`, `CHEST_PAIN`, `DIABETES`, `FLUID_ELECTROLYTE`, `GI_BLEED`, `GI_INFECTION`, `HYPERTENSION`, `OTHER_MEDICAL`, `RESP_FAILURE`, `STROKE`, `UTI` |
| `care_setting` | STRING |  | 0 | `IP`, `BOTH`, `ED` |
| `hrrp_cohort` | STRING |  | 0 | `OTHER`, `AMI`, `HF`, `COPD`, `PN` |
| `is_chronic` | BOOLEAN |  | 0 | `0`, `1` |
| `readmission_risk_level` | STRING |  | 0 | `low`, `high`, `medium` |
| `relative_frequency` | DECIMAL |  | 0 | `0.8`, `0.9`, `1.1`, `1.6`, `0.35`, `0.55`, `0.65`, `0.7`, `0.85`, `1.2`, `1.3`, `1.4`, `1.5`, `1.7`, `1.8`, `1.9`, `2.0`, `2.1`, `2.2`, `2.4`, `2.7`, `5.8`, `6.5`, `8.0` |

<sup>1</sup> the CMS Hospital Readmissions Reduction Program cohorts. Join on the **principal**
diagnosis (`seq_num = 1`) only — a secondary HF code does not make a stay an HF-cohort case.


### `reference/dim_drg`

**Landing** OneLake Files · **Cadence** Day 0 + every Monday (full refresh) · **Format** CSV · **Observed volume** 42 per snapshot · **Columns** 7

Resolves `ehr/admissions.drg_code`. Enables case-mix adjustment — without it, comparing raw
length-of-stay or cost across facilities compares patient mix, not performance.

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `drg_code` | STRING <sup>1</sup> | PK · FK ← `ehr/admissions.drg_code` | 0 | `064`, `065`, `066` … (42 distinct) |
| `drg_description` | STRING |  | 0 | `Acute myocardial infarction, discharged alive with CC`, `Acute myocardial infarction, discharged alive with MCC`, `Acute myocardial infarction, discharged alive without CC/MCC` … (42 distinct) |
| `relative_weight` | DECIMAL |  | 0 | `0.65`, `0.75`, `1.0` … (29 distinct) |
| `severity_tier` | STRING |  | 0 | `MCC`, `NONE`, `CC` |
| `drg_family` | STRING |  | 0 | `AMI`, `ARRHYTHMIA`, `COPD`, `DIABETES`, `GI_BLEED`, `GI_INFECTION`, `HF`, `PN`, `RENAL`, `STROKE`, `FLUID_ELECTROLYTE`, `HYPERTENSION`, `OTHER_MEDICAL`, `SEPSIS`, `UTI`, `CHEST_PAIN`, `RESP_FAILURE` |
| `drg_type` | STRING |  | 0 | `MED` |
| `weight_source` | STRING |  | 0 | `synthetic (approximate) — replace with the CMS FY relative weight file` |

<sup>1</sup> identifier — `drg_code` is zero-padded three characters, never cast to a number

<sup>2</sup> MS-DRG severity: **MCC** major complication/comorbidity, **CC** complication/
comorbidity, **NONE** neither. The triplet is why one clinical condition maps to several codes.

<sup>3</sup> **medical DRGs only.** No surgical DRGs are emitted, because no procedure feed
exists to justify one. `relative_weight` is approximate and **must not be used for
reimbursement modelling**; replace it with the CMS FY relative weight file first.


### — EHR — clinical core —

*Anchored on: Synthea CSV column idiom + MIMIC-IV `admissions` / `transfers` / `edstays`*


### `ehr/patients`

**Landing** OneLake Files · **Cadence** Daily · **Format** CSV · **Columns** 33

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `Id` | UUID | PK | 0 | `30cf14dd-460c-48f3-816f-66d2ef7b7512`, `0b152dd3-5dee-46a0-9b78-713458372dee`, `1b0d1428-42e4-411a-be53-98285e1d6fea` … (32,534 distinct) |
| `BirthDate` | DATE |  | 0.14 | `1962-09-21`, `1972-05-27`, `1924-12-07` … (22,606 distinct) |
| `DeathDate` | STRING |  | 100.00 | *(all null in this run)* |
| `SSN` | STRING <sup>1</sup> |  | 0 | `515-82-3263`, `019-25-4066`, `090-43-1109` … (32,534 distinct) |
| `Drivers` | STRING |  | 2.83 | `S30836803`, `S65407583`, `S13029445` … (31,627 distinct) |
| `Passport` | STRING |  | 65.20 | `X87596897`, `X89964186`, `X96643874` … (11,298 distinct) |
| `Prefix` | STRING |  | 3.45 | `Mr.`, `Ms.` |
| `First` | STRING |  | 0 | `Michael`, `David`, `Jennifer` … (689 distinct) |
| `Middle` | STRING |  | 29.99 | `Michael`, `David`, `Jennifer` … (685 distinct) |
| `Last` | STRING |  | 0.14 | `Smith`, `Johnson`, `Williams` … (1,000 distinct) |
| `Suffix` | STRING |  | 100.00 | *(all null in this run)* |
| `Maiden` | STRING |  | 82.28 | `Smith`, `Johnson`, `Williams` … (946 distinct) |
| `Marital` | STRING |  | 19.61 | `W`, `S`, `D`, `M` |
| `Race` | STRING |  | 0 | `white`, `black`, `other`, `asian`, `native`, `-`, `999`, `ZZZ`, `N/A`, `UNKNOWN_CODE` |
| `Ethnicity` | STRING |  | 0 | `nonhispanic`, `hispanic`, `999`, `ZZZ`, `-`, `N/A`, `UNKNOWN_CODE` |
| `Gender` | STRING |  | 0.11 | `male`, `female`, `unknown`, `other`, `ZZZ`, `-`, `999`, `N/A`, `UNKNOWN_CODE` |
| `BirthPlace` | STRING |  | 0 | `South Michael MA US`, `East John MA US`, `New Jessica MA US` … (27,364 distinct) |
| `Address` | STRING |  | 0 | `5691 Maddox Track Apt. 669`, `057 Mata Common`, `1813 Carpenter Trail Apt. 724` … (32,534 distinct) |
| `City` | STRING |  | 0 | `Boston`, `Oakland`, `Savannah`, `Phoenix`, `Chicago`, `Austin` |
| `State` | STRING |  | 0 | `MA`, `CA`, `GA`, `AZ`, `IL`, `TX` |
| `County` | STRING |  | 0 | `Suffolk`, `Alameda`, `Chatham`, `Maricopa`, `Cook`, `Travis` |
| `FIPS County Code` | INTEGER |  | 0 | `27187`, `22274`, `37565` … (24,683 distinct) |
| `Zip` | STRING <sup>1</sup> |  | 0 | `02215`, `94609`, `31404`, `02118`, `85006`, `60612`, `78702` |
| `Lat` | DECIMAL |  | 0 | `1.884`, `-29.519647`, `-3.100543` … (32,531 distinct) |
| `Lon` | DECIMAL |  | 0 | `96.5557`, `-116.740721`, `-163.794607` … (32,533 distinct) |
| `Healthcare_Expenses` | DECIMAL |  | 0 | `20508.61`, `10521.25`, `114313.1` … (32,471 distinct) |
| `Healthcare_Coverage` | DECIMAL |  | 0 | `4668.48`, `5933.54`, `10085.78` … (32,410 distinct) |
| `Income` | STRING |  | 0 | `13938`, `28135`, `34774` … (27,836 distinct) |
| `phone` | STRING |  | 0 | `(488)990-8889x3362`, `(251)459-1442`, `(527)525-4062x669` … (32,534 distinct) |
| `email` | STRING |  | 0 | `hsmith@example.com`, `qsmith@example.net`, `djohnson@example.net` … (30,548 distinct) |
| `mrn` | STRING <sup>1</sup> | AK per facility | 0 | `330225113`, `330398915`, `330428846` … (41,230 distinct) |
| `source_facility_id` | STRING <sup>1</sup> | FK dim_facility | 0 | `330102`, `330101`, `050201`, `140401`, `030301`, `450601`, `110501` |
| `is_high_risk` | BOOLEAN |  | 0 | `0`, `1` |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number

<sup>2</sup> a **source** flag, rising with age (~3% at 45, capped at 42%). It is the patient's
standing clinical risk, not a readmission prediction and not derived from anything else in this
contract. Do not re-derive it, and do not treat it as a modelling target — nothing downstream
depends on it, so a model trained on it is fitting the generator, not clinical reality.


### `ehr/encounters`

**Landing** OneLake Files · **Cadence** Daily — encounters closed on run_date − 1 · **Format** CSV · **Columns** 22

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `Id` | STRING | PK | 0 | `ENC000064021`, `ENC000073208`, `ENC000075799` … (46,324 distinct) |
| `Start` | TIMESTAMP |  | 6.90 | `2026-08-10 09:32:00`, `2026-08-10 10:02:00`, `2026-08-05 10:22:00` … (8,050 distinct) |
| `Stop` | TIMESTAMP |  | 0 | `2026-08-05 14:30:00`, `2026-08-11 11:15:00`, `2026-08-12 15:15:00` … (40,779 distinct) |
| `Patient` | STRING | FK patients.Id | 0.13 | `30cf14dd-460c-48f3-816f-66d2ef7b7512`, `0b152dd3-5dee-46a0-9b78-713458372dee`, `1d4fd905-358a-4668-8bb3-fc651ea23e3f` … (32,529 distinct) |
| `Organization` | STRING <sup>1</sup> | FK dim_facility | 0 | `330102`, `330101`, `050201`, `140401`, `030301`, `450601`, `110501` |
| `Provider` | STRING | FK dim_staff | 0 | `STF001309`, `STF000170`, `STF003640` … (6,209 distinct) |
| `Payer` | STRING | FK dim_payer | 0 | `PAY001`, `PAY002`, `PAY005`, `PAY004`, `PAY003`, `PAY006`, `PAY007`, `PAY008`, `PAY009` |
| `EncounterClass` | STRING |  | 0.11 | `outpatient`, `OUTPATIENT`, `emergency`, `Outpatient`, `inpatient`, `ambulatory`, `EMERGENCY`, `urgentcare`, `Emergency`, `AMBULATORY`, `INPATIENT`, `Inpatient`, `UNKNOWN_CODE`, `N/A`, `-`, `ZZZ`, `999` |
| `Code` | STRING |  | 0 | `R07.9`, `R10.9`, `E11.65` … (28 distinct) |
| `Description` | STRING |  | 0 | `Chest pain, unspecified`, `Unspecified abdominal pain`, `Type 2 diabetes mellitus with hyperglycemia` … (28 distinct) |
| `Base_Encounter_Cost` | DECIMAL |  | 0 | `223.53`, `831.6`, `568.54` … (34,993 distinct) |
| `Total_Claim_Cost` | STRING |  | 100.00 | *(all null in this run)* |
| `Payer_Coverage` | STRING |  | 100.00 | *(all null in this run)* |
| `ReasonCode` | STRING |  | 0 | `R07.9`, `R10.9`, `E11.65` … (28 distinct) |
| `ReasonDescription` | STRING |  | 1.06 | `Chest pain, unspecified`, `Unspecified abdominal pain`, `Type 2 diabetes mellitus with hyperglycemia` … (28 distinct) |
| `facility_id` | STRING <sup>1</sup> | FK dim_facility | 0 | `330102`, `330101`, `050201`, `140401`, `030301`, `450601`, `110501` |
| `unit_id` | STRING | FK dim_unit | 0 | `330102-OPC`, `330101-OPC`, `050201-OPC` … (81 distinct) |
| `encounter_class_code` | STRING |  | 0 | `AMB`, `EMER`, `IMP`, `ZZZ`, `999`, `N/A`, `-`, `UNKNOWN_CODE` |
| `encounter_status` | STRING |  | 0 | `finished` |
| `patient_class` | STRING |  | 0 | `O`, `E`, `I`, `999`, `-`, `ZZZ`, `N/A`, `UNKNOWN_CODE` |
| `mrn` | STRING <sup>1</sup> | FK patients.mrn | 0 | `030712589`, `050151402`, `330225113` … (43,192 distinct) |
| `source_system` | STRING |  | 0 | `MERIDIAN_EHR_CORE`, `ACADEMIC_CIS`, `REGIONAL_HIS`, `URGENTCARE_CLOUD`, `COMMUNITY_CARE_EHR` |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number

<sup>2</sup> **not canonical.** The same class arrives as `outpatient`, `OUTPATIENT` and
`Outpatient` depending on which EHR emitted the row. Case-fold before any enum check or
grouping; one that does not will reject or split roughly half of all encounters. `AMB` /
`ambulatory` and `outpatient` are both outpatient-class — `urgentcare` is its own class,
emitted only by `450601`. See §8.

<sup>3</sup> **`unit_id` reflects where the encounter actually happened**, including `OPC` and
`ASC` for outpatient. Outpatient encounters previously carried an `-ED` unit, which inflated ED
encounter volume against `dim_unit` by roughly 8×. If you have a saved extract from before this
correction, re-pull it — any per-unit or ED-volume figure taken from it is wrong.


### `ehr/admissions`

**Landing** OneLake Files · **Cadence** Daily · **Format** CSV · **Columns** 27

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `subject_id` | UUID | FK patients.Id | 0 | `0885af47-70d1-4fe7-8260-71bd96a57d45`, `0b75809e-1a39-4fef-b46a-276d9d0d088d`, `0f5daba7-10d1-4aed-9cae-e76327bb349f` … (2,061 distinct) |
| `hadm_id` | STRING | PK | 0 | `HADM000064271`, `HADM000075306`, `HADM000097595` … (2,125 distinct) |
| `admittime` | TIMESTAMP |  | 0.33 | `2026-08-06 07:00:00`, `2026-08-07 11:46:00`, `2026-07-23 11:06:00` … (2,074 distinct) |
| `dischtime` | TIMESTAMP |  | 0 | `2026-08-11 15:21:42`, `2026-08-12 03:22:29`, `2026-08-12 22:29:18` … (2,125 distinct) |
| `deathtime` | TIMESTAMP |  | 97.65 | `2026-08-12 22:29:18`, `2026-08-05 00:23:46`, `2026-08-05 06:04:46` … (49 distinct) |
| `admission_type` | STRING |  | 0.23 | `EW EMER.`, `URGENT`, `ELECTIVE`, `EU OBSERVATION`, `SURGICAL SAME DAY ADMISSION`, `OBSERVATION ADMIT`, `DIRECT EMER.`, `DIRECT OBSERVATION`, `N/A`, `UNKNOWN_CODE`, `ZZZ` |
| `admit_provider_id` | STRING | FK dim_staff | 0 | `STF003811`, `STF000138`, `STF000176` … (1,797 distinct) |
| `admission_location` | STRING |  | 0 | `EMERGENCY ROOM`, `PHYSICIAN REFERRAL`, `CLINIC REFERRAL`, `TRANSFER FROM HOSPITAL`, `AMBULATORY SURGERY TRANSFER`, `TRANSFER FROM SKILLED NURSING FACILITY`, `-`, `N/A` |
| `discharge_location` | STRING |  | 0 | `HOME`, `SKILLED NURSING FACILITY`, `HOME HEALTH CARE`, `REHAB`, `ACUTE HOSPITAL`, `DIED`, `HOSPICE`, `ASSISTED LIVING`, `PSYCH FACILITY`, `HEALTHCARE FACILITY`, `CHRONIC/LONG TERM ACUTE CARE`, `OTHER FACILITY`, `AGAINST ADVICE`, `-`, `ZZZ` |
| `insurance` | STRING |  | 0 | `Commercial`, `Medicare`, `Medicare Advantage`, `Medicaid MC`, `Medicaid`, `Self-Pay`, `Other` |
| `language` | STRING |  | 0 | `ENGLISH`, `SPANISH`, `OTHER`, `?` |
| `marital_status` | STRING |  | 19.41 | `D`, `W`, `S`, `M` |
| `race` | STRING |  | 0 | `white`, `black`, `other`, `asian`, `native` |
| `edregtime` | TIMESTAMP |  | 23.07 | `2026-08-03 17:45:00`, `2026-08-04 09:36:00`, `2026-08-05 09:02:00` … (1,533 distinct) |
| `edouttime` | TIMESTAMP |  | 23.07 | `2026-07-31 17:59:01`, `2026-08-03 16:20:30`, `2026-08-03 20:31:05` … (1,631 distinct) |
| `hospital_expire_flag` | BOOLEAN |  | 0 | `0`, `1` |
| `facility_id` | STRING <sup>1</sup> | FK dim_facility | 0 | `330102`, `330101`, `050201`, `140401`, `030301`, `110501` |
| `admission_type_code` | STRING |  | 0 | `E`, `C`, `U` |
| `admit_decision_time` | TIMESTAMP |  | 23.07 | `2026-08-03 16:05:52`, `2026-08-04 20:41:48`, `2026-08-08 04:22:16` … (1,634 distinct) |
| `hospital_service` | STRING |  | 0 | `MED`, `PSYCH`, `OMED`, `ORTHO`, `SURG`, `NSURG`, `VSURG`, `CMED`, `OBS`, `TRAUM`, `CSURG`, `NB`, `TSURG`, `GU`, `PSURG`, `NBB`, `NMED` |
| `transferred_in_within_6h` | BOOLEAN |  | 0 | `0`, `1` |
| `is_readmission` | BOOLEAN |  | 0 | `0`, `1` |
| `is_planned_readmission` | BOOLEAN |  | 0 | `0`, `1` |
| `index_encounter_id` | STRING | FK encounters.Id | 93.80 | `ENC000000237`, `ENC000000306`, `ENC000000528` … (132 distinct) |
| `encounter_id` | STRING | FK encounters.Id | 0 | `ENC000064270`, `ENC000075305`, `ENC000097594` … (2,125 distinct) |
| `drg_code` | STRING <sup>1</sup> | FK dim_drg.drg_code | 0 | `872`, `871`, `293` … (38 distinct) |
| `source_system` | STRING |  | 0 | `MERIDIAN_EHR_CORE`, `ACADEMIC_CIS`, `REGIONAL_HIS`, `COMMUNITY_CARE_EHR` |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number

<sup>2</sup> populated only on rows where `is_readmission = 1`, and **resolves only within the
window** — a readmission whose index stay pre-dates the extract points at an encounter that was
never emitted. That is faithful to a real extract, not an orphan defect. Any readmission-rate
measure must restrict its numerator to readmissions whose index stay is in the eligible
denominator, or numerator and denominator are drawn from different populations.

<sup>3</sup> which EHR the row came from. **`admittime` / `dischtime` date format and enum casing
vary by this column** — see §8. Note `URGENTCARE_CLOUD` does not appear here: the urgent care
facility does not admit.


### `ehr/ed_stays`

**Landing** OneLake Files · **Cadence** Daily · **Format** CSV · **Columns** 24

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `subject_id` | UUID | FK patients.Id | 0 | `1231e0ef-c779-4927-8fd1-1320674e270a`, `1881d6e7-ff97-4fbe-8989-d7e1f6345370`, `1b4872dd-df65-4b27-ad7f-a72e234a0d47` … (6,607 distinct) |
| `stay_id` | STRING | PK | 0 | `ENC000080949`, `ENC000083314`, `ENC000091110` … (7,008 distinct) |
| `encounter_id` | STRING | FK encounters.Id | 0 | `ENC000080949`, `ENC000083314`, `ENC000091110` … (7,008 distinct) |
| `hadm_id` | STRING | FK admissions.hadm_id | 77.07 | `HADM000004541`, `HADM000005040`, `HADM000008490` … (1,608 distinct) |
| `intime` | TIMESTAMP |  | 0.09 | `2026-08-05 09:36:00`, `2026-08-05 12:15:00`, `2026-08-05 14:38:00` … (5,348 distinct) |
| `outtime` | TIMESTAMP |  | 0 | `2026-08-05 17:42:36`, `2026-08-03 16:20:30`, `2026-08-04 19:55:11` … (6,983 distinct) |
| `gender` | STRING |  | 0 | `M`, `F`, `U` |
| `race` | STRING |  | 0 | `white`, `black`, `other`, `asian`, `native` |
| `arrival_transport` | STRING |  | 0 | `WALK IN`, `AMBULANCE`, `UNKNOWN`, `HELICOPTER`, `UNKNOWN_CODE`, `-`, `999`, `N/A`, `ZZZ` |
| `disposition` | STRING |  | 0.11 | `home`, `admitted`, `HOME`, `Home`, `ADMITTED`, `left against medical advice`, `Admitted`, `transfer`, `Transfer`, `TRANSFER`, `LEFT AGAINST MEDICAL ADVICE`, `Left Against Medical Advice`, `expired`, `left without being seen`, `EXPIRED`, `LEFT WITHOUT BEING SEEN`, `ZZZ`, `N/A`, `-`, `UNKNOWN_CODE` |
| `temperature` | DECIMAL |  | 0 | `99.3`, `98.8`, `98.9` … (69 distinct) |
| `heartrate` | STRING |  | 0 | `105`, `103`, `98` … (99 distinct) |
| `resprate` | INTEGER |  | 0 | `21`, `22`, `23` … (26 distinct) |
| `o2sat` | STRING |  | 0 | `93`, `94`, `92`, `95`, `91`, `96`, `90`, `97`, `89`, `98`, `88`, `87`, `99`, `86`, `100`, `93 mg`, `94 mg`, `85`, `91 mg`, `92 mg`, `92000`, `94000`, `95 mg`, `96 mg`, `980000` |
| `sbp` | STRING |  | 0 | `114`, `125`, `121` … (129 distinct) |
| `dbp` | INTEGER |  | 0 | `73`, `68`, `71` … (77 distinct) |
| `pain` | INTEGER |  | 0 | `5`, `8`, `10`, `7`, `3`, `1`, `6`, `0`, `9`, `2`, `4` |
| `acuity` | INTEGER |  | 0.19 | `3`, `4`, `2`, `5`, `1` |
| `chiefcomplaint` | STRING |  | 0 | `Chest pain, unspecified`, `Unspecified abdominal pain`, `Type 2 diabetes mellitus with hyperglycemia`, `Essential (primary) hypertension`, `Unspecified atrial fibrillation`, `Urinary tract infection, site not specified`, `Dehydration`, `Chronic obstructive pulmonary disease with (acute) lower res`, `Unspecified bacterial pneumonia`, `Heart failure, unspecified`, `Acute on chronic systolic (congestive) heart failure`, `Pneumonia, unspecified organism`, `Acute on chronic diastolic (congestive) heart failure`, `Myocardial infarction type 2`, `Chronic obstructive pulmonary disease with (acute) exacerbat`, `ST elevation (STEMI) myocardial infarction of unspecified si`, `Non-ST elevation (NSTEMI) myocardial infarction` |
| `triage_time` | TIMESTAMP |  | 0 | `2026-08-05 08:30:29`, `2026-08-05 12:34:56`, `2026-08-05 15:01:21` … (6,972 distinct) |
| `provider_seen_time` | TIMESTAMP |  | 0.14 | `2026-08-03 20:26:02`, `2026-08-05 10:00:28`, `2026-08-05 14:26:19` … (6,961 distinct) |
| `admit_decision_time` | TIMESTAMP |  | 76.48 | `2026-07-23 13:41:43`, `2026-07-23 16:06:42`, `2026-07-24 07:47:18` … (1,650 distinct) |
| `facility_id` | STRING <sup>1</sup> | FK dim_facility | 0 | `330102`, `330101`, `050201`, `140401`, `030301`, `450601`, `110501` |
| `source_system` | STRING |  | 0 | `MERIDIAN_EHR_CORE`, `ACADEMIC_CIS`, `REGIONAL_HIS`, `URGENTCARE_CLOUD`, `COMMUNITY_CARE_EHR` |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number

<sup>2</sup> the join key that makes a single visit fact possible. MIMIC-IV's `edstays` has no
encounter concept, so this column is the generator's addition: without it the 77% of ED stays
with a null `hadm_id` had **no join path to `encounters` at all**, and they are most of ED volume
and the whole OP-18 denominator.

<sup>3</sup> **legitimate null.** `hadm_id` is null for patients not admitted from the ED. This
is faithful to MIMIC-IV and is not a defect — use `encounter_id` to join these rows.

<sup>4</sup> door-to-doctor. `triage_time → provider_seen_time` is the interval; the residual
nulls are injected defects, not patients who were never seen.


### `ehr/outpatient_visits`

**Landing** OneLake Files · **Cadence** Daily · **Format** CSV · **Columns** 18

Outpatient clinic (`OPC`) and ambulatory surgery (`ASC`) visits. This feed exists because the
client names the problem directly — *"patient wait times in emergency **and outpatient**
departments are trending in the wrong direction"* — and it is the larger half by volume:
outpatient is normally the bulk of hospital visits, so it is also what brings total encounter
volume in line with the *"several million patient visits a year"* in the brief.

The wait-time timeline is `appointment_time → arrival_time → provider_seen_time →
departure_time`. Note this is a **four**-point timeline against the ED's three: outpatient has a
scheduled appointment, so it supports two distinct waits — *appointment adherence*
(`appointment_time → provider_seen_time`, did the clinic run late) and *patient wait*
(`arrival_time → provider_seen_time`, how long the patient sat there). They are different
questions and a dashboard that conflates them will mislead. Patients also legitimately arrive
before their appointment, so `arrival_time − appointment_time` is often negative — that is early
arrival, **not** a temporal inversion.

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `visit_id` | STRING | PK | 0 | `ENC000076755`, `ENC000077429`, `ENC000078597` … (38,825 distinct) |
| `encounter_id` | STRING | FK encounters.Id | 0 | `ENC000076755`, `ENC000077429`, `ENC000078597` … (38,825 distinct) |
| `subject_id` | UUID | FK patients.Id | 0 | `0b152dd3-5dee-46a0-9b78-713458372dee`, `30cf14dd-460c-48f3-816f-66d2ef7b7512`, `3878fe2f-14d1-43d3-8019-88dd22c87927` … (28,786 distinct) |
| `facility_id` | STRING <sup>1</sup> | FK dim_facility | 0 | `330102`, `330101`, `050201`, `140401`, `030301`, `450601`, `110501` |
| `unit_id` | STRING | FK dim_unit | 0 | `330102-OPC`, `330101-OPC`, `050201-OPC`, `140401-OPC`, `030301-OPC`, `450601-OPC`, `110501-OPC`, `330102-ASC`, `330101-ASC`, `050201-ASC`, `140401-ASC`, `030301-ASC` |
| `clinic_type` | STRING |  | 0.17 | `OPC`, `ASC`, `N/A`, `ZZZ`, `-`, `UNKNOWN_CODE`, `999` |
| `appointment_time` | TIMESTAMP |  | 0.25 | `2026-08-06 10:00:00`, `2026-08-10 10:15:00`, `2026-08-12 10:20:00` … (808 distinct) |
| `arrival_time` | TIMESTAMP |  | 8.10 | `2026-08-10 09:32:00`, `2026-08-10 10:02:00`, `2026-08-05 10:22:00` … (5,061 distinct) |
| `provider_seen_time` | TIMESTAMP |  | 8.10 | `2026-08-05 10:15:57`, `2026-08-05 16:40:02`, `2026-08-06 09:16:53` … (33,204 distinct) |
| `departure_time` | TIMESTAMP |  | 0 | `2026-08-12 15:15:00`, `2026-08-05 09:50:00`, `2026-08-05 14:30:00` … (33,728 distinct) |
| `seen_by_provider_id` | STRING | FK dim_staff.staff_id | 0 | `STF000642`, `STF000676`, `STF000644` … (6,197 distinct) |
| `visit_status` | STRING |  | 0 | `completed`, `COMPLETED`, `no show`, `Completed`, `NO SHOW`, `No Show`, `admitted`, `UNKNOWN_CODE`, `999`, `ZZZ`, `-`, `N/A`, `ADMITTED` |
| `is_no_show` | BOOLEAN |  | 0 | `0`, `1` |
| `escalated_to_inpatient` | BOOLEAN |  | 0 | `0`, `1` |
| `primary_diagnosis_code` | STRING | FK dim_icd10.icd10_code | 0 | `R07.9`, `R10.9`, `E11.65` … (28 distinct) |
| `payer_id` | STRING | FK dim_payer.payer_id | 0 | `PAY001`, `PAY002`, `PAY005`, `PAY004`, `PAY003`, `PAY006`, `PAY007`, `PAY008`, `PAY009` |
| `mrn` | STRING <sup>1</sup> |  | 0 | `050151402`, `330201078`, `330225113` … (36,638 distinct) |
| `source_system` | STRING |  | 0 | `MERIDIAN_EHR_CORE`, `ACADEMIC_CIS`, `REGIONAL_HIS`, `URGENTCARE_CLOUD`, `COMMUNITY_CARE_EHR` |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number

<sup>2</sup> **legitimate null, not a defect.** A no-show never arrives and is never seen, so
`arrival_time` and `provider_seen_time` are null together on those rows (`is_no_show = 1`).
**Exclude no-shows from wait-time averages** — counting them as a zero wait understates the
metric, and counting them as missing data hides a real operational signal. They belong in their
own no-show rate.

<sup>3</sup> the same status arrives as `completed`, `COMPLETED` and `Completed` depending on the
emitting EHR. This is the standardisation problem the client describes, not a defect — see §8.

<sup>4</sup> an ASC visit that became an inpatient admission. Where true, the corresponding
`ehr/admissions` row exists and shares `subject_id`.


### `ehr/transfers`

**Landing** OneLake Files · **Cadence** Daily · **Format** CSV · **Columns** 9

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `subject_id` | UUID | FK patients.Id | 0 | `26ccb245-a3cb-4aed-9655-cf3ac5fc9ea2`, `56dcc453-b644-485a-a66a-abff16dba963`, `7709ed02-df90-46d7-a29c-7ccbebb2002e` … (2,061 distinct) |
| `hadm_id` | STRING | FK admissions.hadm_id | 0 | `HADM000018936`, `HADM000004251`, `HADM000017844` … (2,125 distinct) |
| `transfer_id` | STRING | PK | 0 | `HADM000000363-1`, `HADM000000363-2`, `HADM000000363-3` … (6,809 distinct) |
| `eventtype` | STRING |  | 0 | `admit`, `discharge`, `ed`, `transfer` |
| `careunit` | STRING |  | 31.21 | `Emergency Department`, `Medical/Surgical (MS)`, `Telemetry (TELE)`, `Step-Down (SDU)`, `Medical Intensive Care (MICU)`, `Postpartum (PP)`, `Pediatrics (PEDS)`, `Labor & Delivery (LD)`, `Surgical Intensive Care (SICU)`, `Psychiatric (PSY)`, `Post-Anesthesia Care (PACU)`, `Specialty Care Oncology (ONC)`, `Cardiovascular ICU (CVICU)`, `Neonatal ICU (NICU)`, `Rehabilitation (REHAB)` |
| `intime` | TIMESTAMP |  | 0 | `2026-08-04 17:00:00`, `2026-08-06 18:00:00`, `2026-08-04 21:00:00` … (5,975 distinct) |
| `outtime` | TIMESTAMP |  | 31.21 | `2026-08-04 17:00:00`, `2026-08-06 18:00:00`, `2026-08-04 21:00:00` … (4,065 distinct) |
| `facility_id` | STRING <sup>1</sup> | FK dim_facility | 0 | `330102`, `330101`, `050201`, `140401`, `030301`, `110501` |
| `unit_id` | STRING | FK dim_unit | 0 | `330102-ED`, `330102-MS`, `330101-MS` … (68 distinct) |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number


### `ehr/diagnoses`

**Landing** OneLake Files · **Cadence** Daily · **Format** CSV · **Columns** 9

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `subject_id` | UUID | FK patients.Id | 0 | `0b75809e-1a39-4fef-b46a-276d9d0d088d`, `6f1ce111-3b28-444f-8b18-6624023eb72e`, `8a59c720-c874-4b7f-8b22-d511052e67fb` … (29,108 distinct) |
| `hadm_id` | STRING | FK admissions.hadm_id <sup>2</sup> | 82.49 | `HADM000005547`, `HADM000008490`, `HADM000035031` … (1,850 distinct) |
| `encounter_id` | STRING | FK encounters.Id | 0 | `ENC000005546`, `ENC000008489`, `ENC000035030` … (39,272 distinct) |
| `seq_num` | INTEGER |  | 0 | `1`, `2`, `3`, `4`, `5`, `6`, `7`, `8` |
| `icd_code` | STRING |  | 0 | `R07.9`, `R10.9`, `E11.65` … (28 distinct) |
| `icd_version` | INTEGER |  | 0 | `10` |
| `icd_title` | STRING |  | 0 | `Chest pain, unspecified`, `Unspecified abdominal pain`, `Type 2 diabetes mellitus with hyperglycemia` … (28 distinct) |
| `hrrp_cohort` | STRING |  | 0 | `OTHER`, `HF`, `PN`, `COPD`, `AMI` |
| `facility_id` | STRING <sup>1</sup> | FK dim_facility | 0 | `330102`, `330101`, `050201`, `140401`, `030301`, `450601`, `110501` |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number

<sup>2</sup> **Nullable by design** — the same rule as `ed_stays.hadm_id`. A diagnosis is
recorded against every encounter, but only an *admitted* encounter has an `hadm_id`; outpatient
and ED-discharged encounters have none, which is the 82% here. **`encounter_id` is the join key
for this table** — it is populated on every row. Use `hadm_id` only when you deliberately want
the inpatient subset. Filtering `hadm_id IS NOT NULL` and calling the result "all diagnoses" is
the mistake this column invites.


### — Billing & claims —

*Anchored on: Flattened X12 837I (claim) and 835 (remittance)*


### `claims/claim_header`

**Landing** OneLake Files · **Cadence** Daily — discharges 2–6 days prior · **Format** CSV · **Columns** 26

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `patient_control_number` | STRING | PK | 0 | `PCN000004076`, `PCN000054753`, `PCN000058991` … (27,723 distinct) |
| `encounter_id` | STRING | FK encounters.Id | 0 | `ENC000004076`, `ENC000054753`, `ENC000058991` … (27,723 distinct) |
| `hadm_id` | STRING | FK admissions.hadm_id | 95.21 | `HADM000004077`, `HADM000054754`, `HADM000058992` … (1,324 distinct) |
| `subject_id` | UUID | FK patients.Id | 0 | `1d4fd905-358a-4668-8bb3-fc651ea23e3f`, `1a476b1e-7cda-4243-b272-04e810b22f44`, `9bc7b8bb-3d5f-4249-9bda-491c8960f963` … (21,739 distinct) |
| `facility_id` | STRING <sup>1</sup> | FK dim_facility | 0 | `330102`, `330101`, `050201`, `140401`, `030301`, `450601`, `110501` |
| `total_charge_amount` | STRING |  | 0.15 | `1148.22`, `1464.84`, `1710.18` … (26,733 distinct) |
| `claim_filing_indicator_code` | STRING |  | 0 | `CI`, `MB`, `MC`, `16`, `09`, `WC` |
| `payer_id` | STRING | FK dim_payer | 0.18 | `PAY001`, `PAY002`, `PAY005`, `PAY004`, `PAY003`, `PAY006`, `PAY007`, `PAY008`, `PAY009` |
| `payer_name` | STRING |  | 0 | `MEDICARE PART A/B`, `Medicare Part A/B`, `Medicare A/B` … (29 distinct) |
| `type_of_bill` | STRING <sup>1</sup> |  | 0 | `0131`, `0111`, `ZZZ`, `UNKNOWN_CODE`, `999`, `N/A`, `-` |
| `statement_date_from` | TIMESTAMP |  | 6.65 | `2026-08-05 10:22:00`, `2026-08-06 10:37:00`, `2026-08-06 15:02:00` … (6,330 distinct) |
| `statement_date_to` | TIMESTAMP |  | 0 | `2026-08-05 14:30:00`, `2026-08-05 09:50:00`, `2026-08-05 11:00:00` … (24,785 distinct) |
| `admission_date_and_hour` | TIMESTAMP |  | 95.21 | `2026-07-23 11:06:00`, `2026-07-23 11:46:00`, `2026-07-23 11:55:00` … (1,300 distinct) |
| `discharge_time` | TIMESTAMP |  | 95.21 | `2026-08-05 10:29:47`, `2026-08-06 00:27:51`, `2026-08-07 16:56:11` … (1,324 distinct) |
| `admission_type_code` | STRING |  | 95.21 | `E`, `C`, `U` |
| `admission_source_code` | STRING |  | 0 | `7`, `4`, `E`, `D`, `2`, `1`, `999`, `UNKNOWN_CODE`, `-`, `N/A`, `ZZZ` |
| `patient_status_code` | STRING |  | 0 | `01`, `03`, `06`, `62`, `02`, `04`, `20`, `51`, `63`, `65`, `43`, `07`, `-`, `ZZZ`, `999`, `N/A`, `UNKNOWN_CODE` |
| `drg_code` | STRING |  | 95.21 | `872`, `871`, `293` … (38 distinct) |
| `principal_diagnosis` | STRING |  | 0.13 | `R07.9`, `R10.9`, `E11.65` … (28 distinct) |
| `admitting_diagnosis` | STRING |  | 0 | `R07.9`, `R10.9`, `E11.65` … (28 distinct) |
| `other_diagnoses` | STRING |  | 95.26 | `A41.9`, `E11.65`, `N39.0` … (1,035 distinct) |
| `attending_provider_npi` | STRING <sup>1</sup> | FK dim_staff.npi | 0 | `1833180480`, `1611437475`, `1879475685` … (4,404 distinct) |
| `medical_record_number` | STRING <sup>1</sup> | FK patients.mrn | 0 | `030712589`, `050151402`, `330262317` … (26,423 distinct) |
| `prior_authorization_number` | STRING |  | 58.60 | `AUTH10340752`, `AUTH12928083`, `AUTH13785974` … (11,475 distinct) |
| `submission_date` | DATE |  | 0 | `2026-08-13`, `2026-08-12`, `2026-08-11`, `2026-08-10`, `2026-08-09`, `2026-08-08`, `2026-08-07` |
| `is_readmission_related` | BOOLEAN |  | 0 | `0`, `1` |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number

<sup>2</sup> the claim bills a stay flagged `admissions.is_readmission`. It is **HRRP penalty
exposure on the billing side**, and it is *not* the readmission-rate numerator: it carries no
index link, applies no HRRP exclusion, and includes planned readmissions. Use
`ehr/admissions` for the rate; use this only to price it.


### `claims/claim_line`

**Landing** OneLake Files · **Cadence** Daily · **Format** CSV · **Columns** 13

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `patient_control_number` | STRING | FK claim_header | 0 | `PCN000068244`, `PCN000102225`, `PCN000000486` … (28,049 distinct) |
| `line_control_number` | STRING | PK (with PCN) | 0 | `PCN000068125-002`, `PCN000068244-003`, `PCN000071734-003` … (101,132 distinct) |
| `revenue_code` | STRING <sup>1</sup> |  | 0.23 | `0450`, `0250`, `0320`, `0300`, `0730`, `0460`, `0121`, `0258`, `0360`, `0410`, `0200`, `-`, `N/A`, `ZZZ`, `UNKNOWN_CODE`, `999` |
| `revenue_code_description` | STRING |  | 0 | `Emergency Room - General Classification`, `Pharmacy - General Classification`, `Radiology Diagnostic - General Classification`, `Laboratory - General Classification`, `EKG/ECG - General Classification`, `Pulmonary Function - General Classification`, `Room and Board Semi-private (two beds) - Medical/Surgical/Gyn`, `Pharmacy - IV Solutions`, `Operating Room Services - General Classification`, `Respiratory Services - General Classification`, `Intensive Care Unit - General Classification` |
| `procedure_code` | STRING |  | 84.77 | `J1200`, `G0378`, `J1100`, `J2270`, `J2405`, `J3010`, `J1644`, `A4216`, `J0690`, `J3370`, `J2250`, `Q9967`, `J2543`, `J1170` |
| `procedure_code_qualifier` | STRING |  | 84.77 | `HC` |
| `procedure_description` | STRING |  | 84.77 | `Injection, diphenhydramine hcl, up to 50 mg`, `Hospital observation service, per hour`, `Injection, dexamethasone sodium phosphate, 1 mg`, `Injection, morphine sulfate, up to 10 mg`, `Injection, ondansetron hydrochloride, per 1 mg`, `Injection, fentanyl citrate, 0.1 mg`, `Injection, heparin sodium, per 1000 units`, `Sterile water, saline and/or dextrose, diluent/flush, 10 ml`, `Injection, cefazolin sodium, 500 mg`, `Injection, vancomycin hcl, 500 mg`, `Injection, midazolam hydrochloride, per 1 mg`, `Low osmolar contrast material, 300-399 mg/ml iodine, per ml`, `Injection, piperacillin/tazobactam, 1.125 g`, `Injection, hydromorphone, up to 4 mg` |
| `line_charge_amount` | STRING |  | 0.16 | `4260.0`, `2840.0`, `5680.0` … (72,220 distinct) |
| `unit_type` | STRING |  | 0 | `UN`, `DA` |
| `unit_count` | STRING |  | 0 | `1`, `4`, `2` … (40 distinct) |
| `non_covered_amount` | DECIMAL |  | 0 | `0.0` |
| `service_date_from` | TIMESTAMP |  | 6.38 | `2026-08-05 10:58:00`, `2026-08-05 15:12:00`, `2026-08-06 14:26:00` … (6,324 distinct) |
| `service_date_to` | TIMESTAMP |  | 0 | `2026-08-05 09:50:00`, `2026-08-05 11:00:00`, `2026-08-06 09:30:00` … (25,041 distinct) |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number


### `claims/remit`

**Landing** OneLake Files · **Cadence** Daily · **Format** CSV · **Columns** 16

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `patient_control_number` | STRING | FK claim_header | 0 | `PCN000049532`, `PCN000076119`, `PCN000076530` … (26,861 distinct) |
| `payer_id` | STRING | FK dim_payer | 0 | `PAY001`, `PAY002`, `PAY005`, `PAY004`, `PAY003`, `PAY006`, `PAY007`, `PAY009` |
| `claim_status_code` | STRING <sup>1</sup> |  | 0.20 | `1`, `4`, `UNKNOWN_CODE`, `ZZZ`, `-`, `999`, `N/A` |
| `claim_status_description` | STRING |  | 0 | `Processed as Primary`, `Denied` |
| `total_claim_charge_amount` | DECIMAL |  | 0 | `1148.22`, `1464.84`, `1710.18` … (25,971 distinct) |
| `claim_payment_amount` | STRING |  | 0.20 | `0.0`, `0.0 mg`, `1722.29` … (22,406 distinct) |
| `patient_responsibility_amount` | STRING |  | 0 | `0.0`, `180.94`, `27.91` … (17,922 distinct) |
| `payer_claim_control_number` | STRING |  | 0 | `ICN143726492520`, `ICN167967614754`, `ICN183088521608` … (26,861 distinct) |
| `drg_code` | STRING |  | 95.29 | `872`, `871`, `293` … (38 distinct) |
| `drg_weight` | STRING |  | 95.29 | `1.05`, `0.62`, `1.85` … (29 distinct) |
| `check_eft_trace_number` | STRING |  | 0 | `EFT100043457`, `EFT100618421`, `EFT101851170` … (26,861 distinct) |
| `payment_method_code` | STRING |  | 0 | `CHK`, `ACH` |
| `check_date` | DATE |  | 0 | `2026-09-16`, `2026-09-14`, `2026-09-15` … (325 distinct) |
| `remit_date` | DATE |  | 0 | `2026-09-16`, `2026-09-14`, `2026-09-15` … (325 distinct) |
| `is_appealed` | BOOLEAN |  | 88.56 | `1`, `0` |
| `is_overturned_on_appeal` | BOOLEAN |  | 92.84 | `0`, `1` |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number


### `claims/remit_adjustment`

**Landing** OneLake Files · **Cadence** Daily · **Format** CSV · **Columns** 11

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `patient_control_number` | STRING | FK claim_header | 0 | `PCN000000444`, `PCN000000486`, `PCN000000574` … (22,140 distinct) |
| `adjustment_seq` | INTEGER | PK (with PCN) | 0 | `1`, `2` |
| `group_code` | STRING |  | 0 | `CO`, `PR`, `OA` |
| `group_code_description` | STRING |  | 0 | `Contractual Obligation`, `Patient Responsibility`, `Other Adjustment` |
| `reason_code` | STRING <sup>1</sup> |  | 0 | `45`, `2`, `1`, `3`, `16`, `50`, `197`, `31`, `252`, `96`, `109`, `204`, `18`, `227`, `11`, `29`, `27`, `198`, `146`, `181`, `39` |
| `reason_code_description` | STRING |  | 0 | `Charge exceeds fee schedule/maximum allowable or contracted/legislated fee arrangement.`, `Coinsurance Amount`, `Deductible Amount`, `Co-payment Amount`, `Claim/service lacks information or has submission/billing error(s).`, `These are non-covered services because this is not deemed a "medical necessity" by the payer.`, `Precertification/authorization/notification/pre-treatment absent.`, `Patient cannot be identified as our insured.`, `An attachment/other documentation is required to adjudicate this claim/service.`, `Non-covered charge(s).`, `Claim/service not covered by this payer/contractor. You must send the claim/service to the correct payer/contractor.`, `This service/equipment/drug is not covered under the patient's current benefit plan`, `Exact duplicate claim/service`, `Information requested from the patient/insured/responsible party was not provided or was insufficient/incomplete.`, `The diagnosis is inconsistent with the procedure.`, `The time limit for filing has expired.`, `Expenses incurred after coverage terminated.`, `Precertification/notification/authorization/pre-treatment exceeded.`, `Diagnosis was invalid for the date(s) of service reported.`, `Procedure code was invalid on the date of service.`, `Services denied at the time authorization/pre-certification was requested.` |
| `amount` | DECIMAL |  | 0 | `98.99`, `102.08`, `102.14` … (38,497 distinct) |
| `quantity` | STRING |  | 100.00 | *(all null in this run)* |
| `remark_code` | STRING <sup>1</sup> |  | 98.59 | `MA130` |
| `remark_code_description` | STRING |  | 98.59 | `Your claim contains incomplete and/or invalid information, and no appeal rights are afforded because the claim is unprocessable.` |
| `is_denial` | BOOLEAN |  | 0 | `0`, `1` |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number


### — Bed capacity —

*Anchored on: CDC NHSN Hospital Respiratory Data*


### `beds/hourly_snapshot`

**Landing** OneLake Files · **Cadence** Hourly, batched into one daily file · **Format** CSV · **Columns** 13

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `snapshot_datetime` | TIMESTAMP | PK (with unit_id) | 0 | `2026-08-08 07:00:00`, `2026-08-05 10:00:00`, `2026-08-05 11:00:00` … (192 distinct) |
| `facility_id` | STRING <sup>1</sup> | FK dim_facility | 0 | `330102`, `330101`, `050201`, `140401`, `030301`, `110501` |
| `unit_id` | STRING | FK dim_unit | 0 | `140401-PACU`, `330101-LD`, `330101-ONC` … (62 distinct) |
| `unit_code` | STRING | FK dim_unit.unit_code | 0 | `PACU`, `MICU`, `MS`, `TELE`, `LD`, `PEDS`, `SDU`, `PP`, `ONC`, `CVICU`, `PSY`, `SICU`, `NICU`, `REHAB` |
| `licensed_beds` | INTEGER |  | 0 | `23`, `21`, `15` … (30 distinct) |
| `staffed_beds` | STRING |  | 0.13 | `13`, `20`, `16` … (54 distinct) |
| `blocked_beds` | INTEGER |  | 0 | `0`, `1`, `2`, `4`, `7` |
| `occupied_beds` | STRING |  | 0.15 | `12`, `13`, `11` … (150 distinct) |
| `available_beds` | STRING |  | 0 | `0`, `1`, `2` … (47 distinct) |
| `pending_admissions` | INTEGER |  | 0 | `6`, `8`, `7` … (32 distinct) |
| `pending_discharges` | BOOLEAN |  | 0 | `0`, `1`, `2`, `3`, `4` |
| `occupancy_rate` | DECIMAL |  | 0 | `1.0`, `0.7`, `0.6667` … (251 distinct) |
| `is_at_capacity` | BOOLEAN |  | 0 | `1`, `0` |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number


### `beds/nhsn_weekly`

**Landing** OneLake Files · **Cadence** Weekly — week ending Sunday, measured Wednesday · **Format** CSV · **Columns** 16

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `nhsn_org_id` | STRING <sup>1</sup> | FK dim_facility.facility_id | 0 | `030301`, `050201`, `110501`, `140401`, `330101`, `330102` |
| `facility_name` | STRING |  | 0 | `Meridian Community Hospital – Savannah`, `Meridian General Hospital – Boston`, `Meridian General Hospital – Chicago`, `Meridian General Hospital – Oakland`, `Meridian Regional Medical Center – Phoenix`, `Meridian University Hospital` |
| `week_ending_date` | DATE | PK (with org) | 0 | `2026-08-09` |
| `collection_date` | DATE |  | 0 | `2026-08-05` |
| `all_hospital_inpatient_beds` | INTEGER |  | 0 | `163`, `281`, `315`, `353`, `45`, `533` |
| `all_hospital_inpatient_occupancy` | INTEGER |  | 0 | `139`, `231`, `263`, `274`, `41`, `446` |
| `all_adult_inpatient_beds` | INTEGER |  | 0 | `152`, `266`, `297`, `333`, `45`, `488` |
| `all_adult_inpatient_occupancy` | INTEGER |  | 0 | `132`, `223`, `250`, `260`, `408`, `41` |
| `all_pediatric_inpatient_beds` | INTEGER |  | 0 | `0`, `11`, `15`, `18`, `20`, `45` |
| `all_pediatric_inpatient_occupancy` | INTEGER |  | 0 | `7`, `0`, `12`, `14`, `38` |
| `all_icu_beds` | INTEGER |  | 0 | `14`, `42`, `45`, `51`, `6`, `90` |
| `all_icu_bed_occupancy` | INTEGER |  | 0 | `13`, `36`, `37`, `40`, `5`, `81` |
| `adult_icu_beds` | INTEGER |  | 0 | `14`, `42`, `45`, `51`, `6`, `74` |
| `adult_icu_bed_occupancy` | INTEGER |  | 0 | `13`, `36`, `37`, `40`, `5`, `67` |
| `pediatric_icu_beds` | INTEGER |  | 0 | `0`, `16` |
| `pediatric_icu_bed_occupancy` | INTEGER |  | 0 | `0`, `14` |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number


### — Pharmacy —

*Anchored on: FHIR R5 `InventoryReport` semantics + FDA NDC + RxNorm*


### `pharmacy/inventory`

**Landing** OneLake Files · **Cadence** Daily snapshot · **Format** CSV · **Columns** 34

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `snapshot_date` | DATE | PK (with facility+ndc11) | 0 | `2026-08-06`, `2026-08-07`, `2026-08-08`, `2026-08-10`, `2026-08-09`, `2026-08-11`, `2026-08-12`, `2026-08-13` |
| `counting_datetime` | TIMESTAMP |  | 0 | `2026-08-06 02:15:00`, `2026-08-07 02:15:00`, `2026-08-08 02:15:00`, `2026-08-10 02:15:00`, `2026-08-09 02:15:00`, `2026-08-11 02:15:00`, `2026-08-12 02:15:00`, `2026-08-13 02:15:00` |
| `count_type` | STRING |  | 0 | `snapshot` |
| `facility_id` | STRING <sup>1</sup> | FK dim_facility | 0 | `140401`, `330102`, `330101`, `050201`, `030301`, `110501`, `450601` |
| `location_id` | STRING |  | 0 | `450601-PHARM-MAIN`, `030301-PHARM-MAIN`, `050201-PHARM-MAIN` … (40 distinct) |
| `ndc11` | STRING <sup>1</sup> | FK dim_drug.ndc11 | 0 | `10481101330`, `11110103030`, `10666101830` … (40 distinct) |
| `product_ndc` | STRING <sup>1</sup> |  | 0 | `10481-1013`, `11110-1030`, `10666-1018` … (40 distinct) |
| `gtin14` | STRING <sup>1</sup> |  | 0 | `00310481101331`, `00311110103030`, `00310666101836` … (40 distinct) |
| `rxcui_scd` | INTEGER | FK dim_drug.rxcui_scd | 0 | `1533243`, `595794`, `1178750` … (40 distinct) |
| `drug_name` | STRING |  | 0 | `Levetiracetam 500 MG/5ML`, `Methadone 10 MG`, `Atorvastatin 40 MG` … (40 distinct) |
| `dosage_form_name` | STRING |  | 0 | `INJECTION, SOLUTION`, `TABLET`, `INJECTION, POWDER`, `INJECTION`, `INJECTION, EMULSION`, `CAPSULE`, `INHALATION SOLUTION`, `TABLET, DELAYED RELEASE` |
| `route_name` | STRING |  | 0 | `INTRAVENOUS`, `ORAL`, `SUBCUTANEOUS`, `RESPIRATORY` |
| `pharm_classes` | STRING |  | 0 | `Opioid Analgesic`, `Benzodiazepine`, `Electrolyte` … (29 distinct) |
| `lot_number` | STRING |  | 0.13 | `L288931`, `L511576`, `L791257` … (874 distinct) |
| `expiration_date` | DATE |  | 0.12 | `2027-03-10`, `2026-11-21`, `2026-12-14` … (543 distinct) |
| `qty_on_hand` | STRING |  | 0.16 | `7`, `6`, `8` … (176 distinct) |
| `base_unit` | STRING |  | 0 | `EA` |
| `qty_on_order` | INTEGER |  | 0 | `0`, `11`, `18`, `26`, `30`, `41`, `59`, `63`, `14`, `29`, `31`, `36`, `38`, `91`, `10`, `21`, `25`, `80` |
| `par_level` | STRING |  | 0 | `11`, `12`, `10` … (123 distinct) |
| `reorder_point` | INTEGER |  | 0 | `2`, `3`, `4` … (51 distinct) |
| `safety_stock` | INTEGER |  | 0 | `1`, `2`, `3` … (36 distinct) |
| `avg_daily_usage_30d` | DECIMAL |  | 0 | `0.4`, `0.47`, `0.53` … (197 distinct) |
| `days_on_hand` | DECIMAL |  | 0 | `15.0`, `17.5`, `12.5` … (1,134 distinct) |
| `abc_class` | STRING |  | 0 | `C`, `A`, `B`, `-`, `999`, `ZZZ` |
| `is_controlled` | BOOLEAN |  | 0 | `0`, `1` |
| `dea_schedule` | STRING |  | 63.34 | `CII`, `CIV`, `CIII`, `CV`, `ZZZ` |
| `is_high_alert` | BOOLEAN |  | 0 | `0`, `1` |
| `shortage_status` | STRING |  | 0 | `Available`, `ZZZ`, `-`, `999`, `N/A`, `UNKNOWN_CODE` |
| `shortage_reason` | STRING |  | 100.00 | *(all null in this run)* |
| `unit_cost` | STRING |  | 0 | `6.8`, `0.3`, `0.09` … (48 distinct) |
| `extended_value` | DECIMAL |  | 0 | `2.1`, `0.9`, `50.4` … (1,132 distinct) |
| `last_count_variance` | INTEGER |  | 0 | `0`, `2`, `1`, `-2`, `-1` |
| `is_stockout` | BOOLEAN |  | 0 | `0` |
| `last_restocked_at` | DATE |  | 0 | `2026-08-03`, `2026-07-23`, `2026-07-24` … (29 distinct) |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number

<sup>2</sup> a **date, not a timestamp**, despite the `_at` suffix — there is no restock time of
day. Paired with `days_on_hand` it gives replenishment lead time; on its own it does not indicate
whether stock is currently adequate.


### — Staff rostering —

*Anchored on: CMS PBJ column idiom + California Title 22 §70217 ratios*


### `sharepoint/staff_schedules (XLSX, header row 5)`

**Landing** SharePoint document library · **Cadence** Weekly — Monday, one workbook per facility · **Format** XLSX · **Columns** 19

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `Facility ID` | STRING <sup>1</sup> | FK dim_facility | 0 | `330102`, `330101`, `050201`, `140401`, `030301`, `110501`, `450601` |
| `Unit` | STRING |  | 0 | `Medical/Surgical (MS)`, `Emergency Department (ED)`, `Medical Intensive Care (MICU)`, `Telemetry (TELE)`, `Step-Down (SDU)`, `Labor & Delivery (LD)`, `Surgical Intensive Care (SICU)`, `Cardiovascular ICU (CVICU)`, `Post-Anesthesia Care (PACU)`, `Pediatrics (PEDS)`, `Specialty Care Oncology (ONC)`, `Postpartum (PP)`, `Psychiatric (PSY)`, `Neonatal ICU (NICU)`, `Rehabilitation (REHAB)` |
| `Unit Code` | STRING | FK dim_unit.unit_code | 0 | `MS`, `ED`, `MICU`, `TELE`, `SDU`, `LD`, `SICU`, `CVICU`, `PACU`, `PEDS`, `ONC`, `PP`, `PSY`, `NICU`, `REHAB` |
| `Work Date` | STRING |  | 0 | `2026-08-11`, `2026-08-10`, `2026-08-12`, `08/11/2026`, `08/12/2026`, `08/10/2026`, `2026-08-14`, `2026-08-13`, `2026-08-15`, `2026-08-16`, `08/16/2026`, `08/15/2026`, `08/13/2026`, `08/14/2026` |
| `Shift` | STRING |  | 0 | `D`, `E`, `N`, `OC` |
| `Shift Start` | STRING |  | 0 | `07:00`, `15:00`, `23:00`, `19:00` |
| `Shift End` | STRING |  | 0 | `07:00`, `15:00`, `23:00` |
| `Staff ID` | STRING | FK dim_staff.staff_id | 0 | `STF003476`, `STF003613`, `STF003883` … (2,115 distinct) |
| `Name` | STRING |  | 0 | `Nelson, Michael`, `Cook, Jessica`, `Gaines, Anna` … (2,095 distinct) |
| `Job Code` | STRING |  | 0 | `RN`, `LPN`, `NP` |
| `Employment Type` | INTEGER |  | 0 | `1`, `2` |
| `Scheduled Hours` | INTEGER |  | 0 | `8`, `12` |
| `Actual Hours` | DECIMAL |  | 4.10 | `8`, `12`, `0`, `10`, `14`, `16` |
| `Status` | STRING |  | 0 | `completed`, `absent`, `scheduled`, `cancelled`, `swapped` |
| `Overtime` | STRING |  | 90.38 | `Y` |
| `Called Out` | STRING |  | 95.28 | `Y` |
| `Floated In` | STRING |  | 94.75 | `Y` |
| `Census` | INTEGER |  | 0 | `19`, `10`, `13` … (47 distinct) |
| `Notes` | STRING |  | 49.02 | `agency`, `float pool`, `double shift`, `orientee paired` |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number

<sup>2</sup> `D` 07:00–15:00, `E` 15:00–23:00, `N` 23:00–07:00 — three 8-hour shifts covering the
day with no overlap, each staffed against the unit's census ratio. `OC` (on-call, 19:00, 12h) is
**cover, not rostered presence**: it is a fixed small team, not scaled from census. A
nurse-to-patient ratio computed against `OC` is meaningless — it yields one nurse "covering" a
50-bed unit. **Exclude `OC` from any mandated-ratio or staffing-adequacy measure** and report it
separately.

<sup>3</sup> **legitimate null, not a defect.** `Actual Hours` is null while `Status` is
`scheduled`, `swapped` or `cancelled` — the shift has not been worked yet. These rows are
deliberately *not* in the answer key: they are the material that proves a completeness gate does
not false-positive on a null that is supposed to be there.


### — Streaming — patient vitals —

*Anchored on: eICU `vitalPeriodic` + LOINC (vitals); MIMIC-IV `hosp.pharmacy` + FHIR `MedicationRequest`*


### `stream/patient-vitals — envelope`

**Landing** Eventstream (Kafka) + gzipped JSONL archive · **Cadence** Continuous · **Format** JSON · **Observed volume** 7,465,314 events, 257,425 sampled · **Columns** 7

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `event_id` | UUID | PK (deterministic uuid5) | 0 | `00001b66-8122-5e75-a680-7cfaa6110649`, `000051b6-16dc-5d42-954e-e0bc7c592ac4`, `00006f8e-7ad6-529c-b23d-80bc3cdca050` … (257,425 distinct) |
| `event_type` | STRING |  | 0 | `vitals.reading` |
| `event_time` | TIMESTAMP |  | 0 | `2026-08-05T04:50:00`, `2026-08-12T02:00:00`, `2026-08-05T04:25:00` … (9,073 distinct) |
| `ingest_time` | TIMESTAMP |  | 0 | `2026-08-05T07:15:01`, `2026-08-09T17:05:05`, `2026-08-07T00:35:04` … (36,603 distinct) |
| `source_system` | STRING |  | 0 | `PHILIPS_IX_MONITOR` |
| `facility_id` | STRING <sup>1</sup> | FK dim_facility | 0 | `330102`, `330101`, `050201`, `140401`, `030301`, `110501` |
| `schema_version` | DECIMAL |  | 0 | `1.0` |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number


### `stream/patient-vitals — payload`

**Landing** Eventstream (Kafka) + gzipped JSONL archive · **Cadence** Continuous · **Format** JSON · **Observed volume** 7,465,314 events, 257,425 sampled · **Columns** 13

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `patient_id` | UUID | FK patients.Id | 0 | `1de14133-fc15-442b-b919-c4eab89f7daf`, `a43abf05-35f3-4762-a039-b15694dc2f4a`, `0f5daba7-10d1-4aed-9cae-e76327bb349f` … (1,708 distinct) |
| `encounter_id` | STRING | FK encounters.Id | 0 | `ENC000000702`, `ENC000025824`, `ENC000057727` … (1,774 distinct) |
| `hadm_id` | STRING | FK admissions.hadm_id | 0 | `HADM000000703`, `HADM000025825`, `HADM000057728` … (1,774 distinct) |
| `unit_id` | STRING | FK dim_unit | 0 | `330102-TELE`, `330101-TELE`, `330102-MICU` … (37 distinct) |
| `bed_id` | STRING |  | 0 | `330102-TELE-B40`, `030301-SDU-B12`, `330101-CVICU-B10` … (664 distinct) |
| `device_id` | STRING |  | 0 | `MON-TELE-024`, `MON-TELE-035`, `MON-TELE-033` … (319 distinct) |
| `charttime` | TIMESTAMP |  | 0 | `2026-08-05T04:50:00`, `2026-08-12T02:00:00`, `2026-08-05T04:25:00` … (9,073 distinct) |
| `loinc_code` | STRING |  | 0 | `8867-4`, `9279-1`, `8310-5`, `2708-6`, `8462-4`, `8480-6` |
| `parameter_name` | STRING |  | 0 | `Heart rate`, `Respiratory Rate`, `Body temperature`, `Oxygen saturation in Arterial blood`, `Diastolic arterial blood pressure`, `Systolic arterial blood pressure` |
| `value_num` | DECIMAL |  | 0.38 | `91`, `90`, `92` … (342 distinct) |
| `value_uom` | STRING |  | 0 | `/min`, `mm[Hg]`, `Cel`, `%` |
| `warning` | BOOLEAN |  | 0 | `0`, `1` |
| `is_artifact` | BOOLEAN |  | 0 | `False`, `True` |


### — Streaming — prescription events —

*Anchored on: eICU `vitalPeriodic` + LOINC (vitals); MIMIC-IV `hosp.pharmacy` + FHIR `MedicationRequest`*


### `stream/prescription-events — envelope`

**Landing** Eventstream (Kafka) + gzipped JSONL archive · **Cadence** Continuous · **Format** JSON · **Observed volume** 97,576 events, 97,576 sampled · **Columns** 7

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `event_id` | UUID | PK (deterministic uuid5) | 0 | `01bf5fe5-73cd-5027-b07a-32ac9b90332c`, `02249489-c13a-516b-b46d-c82cd2930753`, `0500d5cd-570f-5bec-9172-f006fbfe3f9f` … (97,377 distinct) |
| `event_type` | STRING |  | 0 | `prescription.issued` |
| `event_time` | TIMESTAMP |  | 0 | `2026-08-08T15:53:54`, `2026-08-09T00:23:31`, `2026-08-09T16:03:14` … (92,424 distinct) |
| `ingest_time` | TIMESTAMP |  | 0 | `2026-08-06T02:10:20`, `2026-08-07T02:33:46`, `2026-08-07T02:41:42` … (92,319 distinct) |
| `source_system` | STRING |  | 0 | `PHARMACY_OMS` |
| `facility_id` | STRING <sup>1</sup> | FK dim_facility | 0 | `330102`, `330101`, `050201`, `140401`, `030301`, `110501` |
| `schema_version` | DECIMAL |  | 0 | `1.0` |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number


### `stream/prescription-events — payload`

**Landing** Eventstream (Kafka) + gzipped JSONL archive · **Cadence** Continuous · **Format** JSON · **Observed volume** 97,576 events, 97,576 sampled · **Columns** 36

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `subject_id` | UUID | FK patients.Id | 0 | `1de14133-fc15-442b-b919-c4eab89f7daf`, `2efd542f-5745-4279-b6ff-550699a6449f`, `a43abf05-35f3-4762-a039-b15694dc2f4a` … (3,292 distinct) |
| `hadm_id` | STRING | FK admissions.hadm_id | 0 | `HADM000060031`, `HADM000049462`, `HADM000047483` … (3,482 distinct) |
| `encounter_id` | STRING | FK encounters.Id | 0 | `ENC000060030`, `ENC000049461`, `ENC000047482` … (3,482 distinct) |
| `pharmacy_id` | INTEGER |  | 0 | `1974512`, `2562809`, `2757062` … (96,847 distinct) |
| `poe_id` | STRING |  | 0 | `HADM000000811-572`, `HADM000008910-271`, `HADM000012022-219` … (95,638 distinct) |
| `poe_seq` | INTEGER |  | 0 | `825`, `499`, `878` … (999 distinct) |
| `order_provider_id` | STRING | FK dim_staff.staff_id | 0 | `STF004493`, `STF005462`, `STF003525` … (6,213 distinct) |
| `drug` | STRING |  | 0 | `Sodium Chloride 0.9%`, `Acetaminophen 325 MG`, `Ondansetron 4 MG/2ML` … (40 distinct) |
| `drug_type` | STRING |  | 0 | `MAIN`, `BASE`, `ADDITIVE` |
| `formulary_drug_cd` | STRING |  | 0 | `SODIUMCHLOR`, `ACETAMINOPHE`, `ONDANSETRON` … (40 distinct) |
| `gsn` | INTEGER |  | 0 | `95634`, `21445`, `43050` … (61,994 distinct) |
| `ndc` | STRING <sup>1</sup> | FK dim_drug.ndc11 | 0 | `10000100030`, `10037100130`, `10074100230` … (40 distinct) |
| `rxcui_scd` | INTEGER | FK dim_drug.rxcui_scd | 0 | `1799706`, `721063`, `328689` … (40 distinct) |
| `prod_strength` | STRING |  | 0 | `Sodium Chloride 0.9%`, `Acetaminophen 325 MG`, `Ondansetron 4 MG/2ML` … (40 distinct) |
| `dose_val_rx` | DECIMAL |  | 0 | `0.5`, `25`, `12.5`, `2`, `50`, `100`, `5`, `4`, `1000`, `40`, `500`, `10`, `20`, `1` |
| `dose_unit_rx` | STRING |  | 0 | `mL`, `UNT`, `mg`, `mEq`, `mcg` |
| `form_val_disp` | INTEGER |  | 0 | `2`, `4`, `1`, `3` |
| `form_unit_disp` | STRING |  | 0 | `BAG`, `TAB`, `VIAL`, `SYR` |
| `doses_per_24_hrs` | DECIMAL |  | 0 | `3.0`, `1.0`, `4.0`, `6.0`, `2.0` |
| `route` | STRING |  | 0 | `INTRAVENOUS`, `ORAL`, `SUBCUTANEOUS`, `RESPIRATORY` |
| `frequency` | STRING |  | 0 | `ONCE`, `BID`, `Q8H`, `Q6H`, `Q24H`, `PRN`, `TID`, `Q12H` |
| `proc_type` | STRING |  | 0 | `Unit Dose`, `IV Piggyback`, `Non-formulary`, `Large Volume` |
| `status` | STRING |  | 0 | `active`, `inactive`, `discontinued` |
| `fhir_status` | STRING |  | 0 | `active`, `completed`, `stopped` |
| `fhir_intent` | STRING |  | 0 | `order` |
| `fhir_priority` | STRING |  | 0 | `routine`, `stat` |
| `entertime` | TIMESTAMP |  | 0 | `2026-08-08T15:53:54`, `2026-08-09T00:23:31`, `2026-08-09T16:03:14` … (92,424 distinct) |
| `verifiedtime` | TIMESTAMP |  | 0 | `2026-08-05T03:08:17`, `2026-08-05T07:05:04`, `2026-08-07T05:26:56` … (92,407 distinct) |
| `starttime` | TIMESTAMP |  | 0 | `2026-08-12T05:45:36`, `2026-08-11T07:11:32`, `2026-08-12T02:34:33` … (92,427 distinct) |
| `stoptime` | TIMESTAMP |  | 0 | `2026-08-08T01:35:19`, `2026-08-08T05:45:31`, `2026-08-09T11:07:52` … (93,117 distinct) |
| `dispensation` | STRING |  | 0 | `ADC`, `Main Pharmacy`, `Satellite` |
| `fill_quantity` | INTEGER |  | 0 | `4`, `3`, `5`, `2`, `6`, `1` |
| `is_controlled` | BOOLEAN |  | 0 | `False`, `True` |
| `dea_schedule` | STRING |  | 84.57 | `CII`, `CIV`, `CIII`, `CV` |
| `unit_id` | STRING | FK dim_unit | 0 | `330102-MS`, `330101-MS`, `050201-MS` … (62 distinct) |
| `event_subtype` | STRING |  | 0 | `ordered` |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number


---

## 6. Referential integrity rules

These are the joins Silver must validate. Anything that fails is an injected orphan or a real
defect — there is no third case.

| From | To | On | Rule |
|---|---|---|---|
| `encounters.Patient` | `patients.Id` | patient UUID | Must resolve |
| `admissions.encounter_id` | `encounters.Id` | `ENC…` | Must resolve; 1:1 |
| `admissions.subject_id` | `patients.Id` | patient UUID | Must resolve |
| `transfers.hadm_id` | `admissions.hadm_id` | `HADM…` | Must resolve; 1:N |
| `diagnoses.hadm_id` | `admissions.hadm_id` | `HADM…` | **Nullable by design** — populated only for admitted encounters (≈18% of rows); null for outpatient and ED-discharged. Where present it must resolve, 1:N. Join via `encounter_id` for all diagnoses. |
| `diagnoses.encounter_id` | `encounters.Id` | `ENC…` | Must resolve; 1:N. The join path that covers every diagnosis row |
| `ed_stays.hadm_id` | `admissions.hadm_id` | `HADM…` | **Nullable by design** — empty for patients discharged from ED (77% of ED stays). Join via `encounter_id` instead. |
| `ed_stays.encounter_id` | `encounters.Id` | `ENC…` | Must resolve; 1:1. The join path for ED-discharged patients |
| `outpatient_visits.encounter_id` | `encounters.Id` | `ENC…` | Must resolve; 1:1 |
| `outpatient_visits.subject_id` | `patients.Id` | patient UUID | Must resolve |
| `outpatient_visits.unit_id` | `dim_unit.unit_id` | `330101-OPC` | Must resolve; `OPC`/`ASC` units only |
| `outpatient_visits.payer_id` | `dim_payer.payer_id` | `PAY…` | Must resolve |
| `outpatient_visits.primary_diagnosis_code` | `dim_icd10.icd10_code` | ICD-10-CM | Must resolve |
| `outpatient_visits.seen_by_provider_id` | `dim_staff.staff_id` | `STF…` | Must resolve |
| `admissions.drg_code` | `dim_drg.drg_code` | MS-DRG | Must resolve |
| `diagnoses.icd_code` | `dim_icd10.icd10_code` | ICD-10-CM | Must resolve |
| `admissions.index_encounter_id` | `encounters.Id` | `ENC…` | **Nullable, and resolves only within the window** — a readmission whose index stay pre-dates the extract points at nothing. Not an orphan defect — restrict any readmission-rate numerator to readmissions whose index stay is in the eligible denominator. |
| `claim_header.encounter_id` | `encounters.Id` | `ENC…` | Must resolve |
| `claim_line.patient_control_number` | `claim_header.patient_control_number` | `PCN…` | Must resolve; 1:N |
| `remit.patient_control_number` | `claim_header.patient_control_number` | `PCN…` | Must resolve; 1:1 |
| `remit_adjustment.patient_control_number` | `claim_header.patient_control_number` | `PCN…` | Must resolve; 1:N |
| `hourly_snapshot.unit_id` | `dim_unit.unit_id` | `330101-MICU` | Must resolve |
| `nhsn_weekly.nhsn_org_id` | `dim_facility.facility_id` | CCN | Must resolve |
| `inventory.ndc11` | `dim_drug.ndc11` | NDC-11 | Must resolve |
| `staff_schedules.Staff ID` | `dim_staff.staff_id` | `STF…` | Must resolve |
| `staff_schedules.Unit Code` | `dim_unit.unit_code` | e.g. `MICU` | Must resolve |
| both streams `.encounter_id` | `encounters.Id` | `ENC…` | **Resolves only within the window** — telemetry flows while a patient is in a bed, but `ehr/encounters` only emits an encounter once it *closes*. A stay still open at the end of the extract streams events whose encounter row has not been written yet, so roughly a third of distinct streamed `encounter_id`s have no EHR match. Not an orphan defect: treat unmatched stream events as in-flight, not as broken keys. |
| both streams `.unit_id` | `dim_unit.unit_id` | | Must resolve |

**Reconciliation rule.** `beds/nhsn_weekly` must reconcile against `beds/hourly_snapshot`
measured as of the Wednesday of the reporting week. Store the variance; do not discard it.

**Identity rule.** `mrn` is **per facility, not per patient**. 7,932 of 32,534 patients (24%)
carry more than one MRN — the same person has a different MRN at each facility they attend.
Silver needs a master patient index on (`mrn`, `facility_id`) → `patients.Id`. Hashing MRN
row-by-row produces several unlinkable tokens for one person.

---

## 7. Canonical value domains

Full domains appear in the per-table sections. These are the ones most often mis-specified:

| Field | Canonical values | Count |
|---|---|---|
| `dim_payer.payer_type` | Medicare, Medicare Advantage, Medicaid, Medicaid MC, Commercial, Self-Pay, Other | **7** |
| `dim_facility.facility_type` | General Acute Care, Teaching, Regional, Community, Urgent Care | 5 |
| `dim_facility.region` | Northeast, West, Midwest, South | 4 |
| `dim_unit.unit_type` | 17 values — see `dim_unit`, now including `Outpatient Clinic` (`OPC`) and `Ambulatory Surgery` (`ASC`) | **17** |
| `encounters.EncounterClass` | outpatient, emergency, inpatient, ambulatory, urgentcare | **5** <sup>†</sup> |
| `staff_schedules.Shift` | D, E, N, OC | **4** — `OC` is cover, exclude from ratio measures |
| `remit.claim_status_code` | `1` processed as primary, `4` denied | X12 CLP02 |
| `ed_stays.acuity` | 1–5 ESI, 1 = most acute | MIMIC-IV-ED |
| `diagnoses.hrrp_cohort` | AMI, HF, PN, COPD, OTHER | CMS HRRP |
| `dim_icd10.hrrp_cohort` | AMI, HF, PN, COPD, OTHER | CMS HRRP |
| `dim_drg.severity_tier` | MCC, CC, NONE | MS-DRG |
| `outpatient_visits.visit_status` | completed, no show, admitted | case varies by source system |

<sup>†</sup> **case is not canonical.** `EncounterClass` arrives as `outpatient`, `OUTPATIENT`
and `Outpatient` — and likewise for the others — because facilities run different EHRs. The
five values above are the domain *after* case-folding. A Silver enum check that does not fold
case first will reject roughly half of all encounters. Same applies to
`outpatient_visits.visit_status`. See §8.

> Two of these are commonly truncated. `payer_type` **must** keep Medicare Advantage separate —
> its denial rate is calibrated to roughly 2× traditional Medicare, which is the most
> significant payer finding in the data. `unit_type` **must** keep all 17 — the Title 22 nurse
> ratios are per unit type.

---

## 8. Deliberate imperfections

Every run injects data-quality defects at a configurable rate **and records each one** in
`out/dq_answer_key.json`. That file is the marking scheme. It must never land in Bronze
alongside the data it grades.

Defects are injected at two levels.

**Row-level** — the value is wrong, but the file arrives intact:

| Defect class | Check that should catch it |
|---|---|
| late event | late-data handling / watermarks |
| null required field | completeness / `null_check` |
| duplicate `event_id` | uniqueness / dedupe |
| type non-conformance | type conformance — e.g. `"12.5 mg"` in a numeric column |
| invalid code value | validity / domain (`enum_check`) |
| numeric outlier | plausibility range |
| duplicate row | uniqueness |
| temporal inversion | temporal validity — discharge before admit |
| orphan foreign key | cross-source referential integrity |

**File-level** — the file itself is wrong or absent. These exist because the client success
criteria require pipelines that *"can be demonstrated to recover from a failed run without
manual data-fixing"*, and no row-level defect can produce that failure:

| Defect class | Check that should catch it |
|---|---|
| `missing_file` | expected-arrival / freshness monitoring — the partition never lands |
| `truncated_file` | row-count reconciliation against expected volume |

> Counts vary by run length, seed and `--chaos`; read them from `by_type` in the answer key for
> the run you actually generated rather than from a number pinned in this document. A 21-day
> `--no-streams` run injects ~12,000; stream defects (late event, duplicate `event_id`) only
> appear when streams are generated.

`--chaos` multiplies the rates for the break-it test; `--no-defects` produces a clean baseline.
**Always generate both.** When something looks wrong, the baseline tells you in one step whether
it is a generator bug or an injected defect doing its job.

> **Consequence for the contract.** Non-zero null rates and out-of-domain values in the tables
> below are largely intentional. Do not "correct" the contract to match them, and do not admit
> them to a Silver enum.


---


*Synthetic data throughout. The mechanics and schemas are real and citable; the numbers are
invented. No figure from this generator is a finding about a real hospital, and none may be
presented as one.*
