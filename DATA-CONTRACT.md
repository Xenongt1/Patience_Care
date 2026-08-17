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
| `facility_id` | STRING <sup>1</sup> | PK | 0 | `330101`, `330102`, `050201`, `030301`, `140401`, `110501`, `450601` |
| `facility_name` | STRING |  | 0 | `Meridian General Hospital – Boston`, `Meridian University Hospital`, `Meridian General Hospital – Oakland`, `Meridian Regional Medical Center – Phoenix`, `Meridian General Hospital – Chicago`, `Meridian Community Hospital – Savannah`, `Meridian Urgent Care – Austin` |
| `facility_type` | STRING |  | 0 | `General Acute Care`, `Teaching`, `Regional`, `Community`, `Urgent Care` |
| `region` | STRING |  | 0 | `Northeast`, `West`, `South`, `Midwest` |
| `city` | STRING |  | 0 | `Boston`, `Oakland`, `Phoenix`, `Chicago`, `Savannah`, `Austin` |
| `state` | STRING |  | 0 | `MA`, `CA`, `AZ`, `IL`, `GA`, `TX` |
| `zip` | STRING <sup>1</sup> |  | 0 | `02118`, `02215`, `94609`, `85006`, `60612`, `31404`, `78702` |
| `county` | STRING |  | 0 | `Suffolk`, `Alameda`, `Maricopa`, `Cook`, `Chatham`, `Travis` |
| `emergency_services` | BOOLEAN |  | 0 | `1`, `0` |
| `licensed_beds` | INTEGER |  | 0 | `420`, `610`, `380`, `250`, `340`, `95`, `0` |
| `staffed_beds` | INTEGER |  | 0 | `389`, `584`, `350`, `186`, `319`, `57`, `17` |
| `ownership` | STRING |  | 0 | `Voluntary non-profit - Private`, `Proprietary`, `Voluntary non-profit - Church`, `Government - Local` |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number


### `reference/dim_unit`

**Landing** OneLake Files · **Cadence** Day 0 + every Monday (full refresh) · **Format** CSV · **Observed volume** 69 per snapshot · **Columns** 13

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `unit_id` | STRING | PK | 0 | `330101-ED`, `330101-MICU`, `330101-SICU` … (69 distinct) |
| `facility_id` | STRING <sup>1</sup> | FK dim_facility | 0 | `330102`, `330101`, `050201`, `140401`, `030301`, `110501`, `450601` |
| `unit_code` | STRING |  | 0 | `ED`, `MICU`, `TELE`, `PACU`, `MS`, `SDU`, `PEDS`, `LD`, `PP`, `SICU`, `CVICU`, `ONC`, `PSY`, `NICU` … (+1) |
| `unit_name` | STRING |  | 0 | `Emergency Department (ED)`, `Medical Intensive Care (MICU)`, `Telemetry (TELE)`, `Post-Anesthesia Care (PACU)`, `Medical/Surgical (MS)`, `Step-Down (SDU)`, `Pediatrics (PEDS)`, `Labor & Delivery (LD)`, `Postpartum (PP)`, `Surgical Intensive Care (SICU)`, `Cardiovascular ICU (CVICU)`, `Specialty Care Oncology (ONC)`, `Psychiatric (PSY)`, `Neonatal ICU (NICU)` … (+1) |
| `unit_type` | STRING |  | 0 | `Emergency Department`, `Medical Intensive Care`, `Telemetry`, `Post-Anesthesia Care`, `Medical/Surgical`, `Step-Down`, `Pediatrics`, `Labor & Delivery`, `Postpartum`, `Surgical Intensive Care`, `Cardiovascular ICU`, `Specialty Care Oncology`, `Psychiatric`, `Neonatal ICU` … (+1) |
| `building` | STRING |  | 0 | `Main`, `North Tower` |
| `floor` | INTEGER |  | 0 | `8`, `6`, `2`, `5`, `1`, `7`, `9`, `3`, `4` |
| `licensed_beds` | INTEGER |  | 0 | `23`, `21`, `15` … (34 distinct) |
| `staffed_beds` | INTEGER |  | 0 | `16`, `13`, `36` … (34 distinct) |
| `blocked_beds` | INTEGER |  | 0 | `0`, `1`, `2`, `3` |
| `nurse_patient_ratio_target` | DECIMAL |  | 0 | `4.0`, `2.0`, `5.0`, `3.0`, `6.0` |
| `is_critical_care` | BOOLEAN |  | 0 | `0`, `1` |
| `is_monitored` | BOOLEAN |  | 0 | `1`, `0` |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number


### `reference/dim_staff`

**Landing** OneLake Files · **Cadence** Day 0 + every Monday (full refresh) · **Format** CSV · **Observed volume** 4,963 per snapshot · **Columns** 14

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `staff_id` | STRING | PK | 0 | `STF000001`, `STF000002`, `STF000003` … (4,963 distinct) |
| `npi` | STRING <sup>1</sup> | AK | 29.26 | `1162645417`, `1384193213`, `1919917997` … (3,510 distinct) |
| `first_name` | STRING |  | 0 | `Michael`, `Robert`, `Jennifer` … (592 distinct) |
| `last_name` | STRING |  | 0 | `Smith`, `Johnson`, `Williams` … (921 distinct) |
| `job_code` | STRING |  | 0 | `RN`, `CNA`, `MD`, `LPN`, `RT`, `UC`, `RPh`, `NP`, `PharmTech`, `PA` |
| `job_title` | STRING |  | 0 | `Registered Nurse`, `Certified Nursing Assistant`, `Physician`, `Licensed Practical Nurse`, `Respiratory Therapist`, `Unit Clerk`, `Pharmacist`, `Nurse Practitioner`, `Pharmacy Technician`, `Physician Assistant` |
| `credential` | STRING |  | 4.61 | `RN`, `CNA`, `MD`, `LPN`, `RRT`, `PharmD`, `NP`, `CPhT`, `PA-C` |
| `primary_facility_id` | STRING <sup>1</sup> | FK dim_facility | 0 | `330102`, `330101`, `050201`, `140401`, `030301`, `110501`, `450601` |
| `primary_unit_id` | STRING | FK dim_unit | 0 | `330102-PEDS`, `330102-MS`, `330102-SICU` … (69 distinct) |
| `employment_type` | INTEGER |  | 0 | `1`, `2` |
| `fte` | DECIMAL |  | 0 | `1.0`, `0.6`, `0.8`, `0.9` |
| `hire_date` | DATE |  | 0 | `2022-01-05`, `2016-02-27`, `2025-09-07` … (2,885 distinct) |
| `termination_date` | DATE |  | 88.7 | `2026-03-21`, `2026-07-25`, `2026-01-04` … (311 distinct) |
| `is_active` | BOOLEAN |  | 0 | `1`, `0` |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number


### `reference/dim_payer`

**Landing** OneLake Files · **Cadence** Day 0 + every Monday (full refresh) · **Format** CSV · **Observed volume** 9 per snapshot · **Columns** 6

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `payer_id` | STRING | PK | 0 | `PAY001`, `PAY002`, `PAY003`, `PAY004`, `PAY005`, `PAY006`, `PAY007`, `PAY008`, `PAY009` |
| `payer_name` | STRING |  | 0 | `Medicare Part A/B`, `Vantage Medicare Advantage`, `State Medicaid FFS`, `Harborview Medicaid MC`, `Atlas Health Commercial`, `Northwind PPO`, `Ironbridge HMO`, `Self-Pay`, `Other / Workers Comp` |
| `payer_type` | STRING |  | 0 | `Commercial`, `Medicare`, `Medicare Advantage`, `Medicaid`, `Medicaid MC`, `Self-Pay`, `Other` |
| `claim_filing_indicator_code` | STRING |  | 0 | `CI`, `MC`, `MB`, `16`, `09`, `WC` |
| `share` | DECIMAL |  | 0 | `0.118`, `0.242`, `0.16`, `0.126`, `0.101`, `0.06`, `0.042`, `0.033` |
| `prompt_pay_days` | INTEGER |  | 0 | `40`, `30`, `45`, `60`, `0` |


### `reference/dim_drug`

**Landing** OneLake Files · **Cadence** Day 0 + every Monday (full refresh) · **Format** CSV · **Observed volume** 40 per snapshot · **Columns** 20

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `rxcui_scd` | INTEGER | PK | 0 | `1799706`, `721063`, `328689` … (40 distinct) |
| `rxcui_in` | INTEGER |  | 0 | `341992`, `1139960`, `1302788` … (40 distinct) |
| `ndc11` | STRING <sup>1</sup> | AK | 0 | `10000100030`, `10037100130`, `10074100230` … (40 distinct) |
| `product_ndc` | STRING <sup>1</sup> | AK | 0 | `10000-1000`, `10037-1001`, `10074-1002` … (40 distinct) |
| `gtin14` | STRING <sup>1</sup> | AK | 0 | `00310000100030`, `00310037100137`, `00310074100234` … (40 distinct) |
| `proprietary_name` | STRING |  | 0 | `Sodium`, `Acetaminophen`, `Ondansetron` … (40 distinct) |
| `non_proprietary_name` | STRING |  | 0 | `Sodium Chloride 0.9%`, `Acetaminophen 325 MG`, `Ondansetron 4 MG/2ML` … (40 distinct) |
| `dosage_form_name` | STRING |  | 0 | `INJECTION, SOLUTION`, `TABLET`, `INJECTION, POWDER`, `INJECTION`, `INHALATION SOLUTION`, `TABLET, DELAYED RELEASE`, `INJECTION, EMULSION`, `CAPSULE` |
| `route_name` | STRING |  | 0 | `INTRAVENOUS`, `ORAL`, `SUBCUTANEOUS`, `RESPIRATORY` |
| `pharm_classes` | STRING |  | 0 | `Opioid Analgesic`, `Benzodiazepine`, `Electrolyte` … (29 distinct) |
| `dea_schedule` | STRING |  | 62.5 | `CII`, `CIV`, `CIII`, `CV` |
| `dea_drug_code` | INTEGER |  | 62.5 | `9801`, `9150`, `9300`, `9143`, `9193`, `9250`, `7285`, `9064`, `2884`, `2885`, `2765`, `2882`, `2285`, `9752` … (+1) |
| `is_controlled` | BOOLEAN |  | 0 | `0`, `1` |
| `labeler_name` | STRING |  | 0 | `Jones Ltd Pharmaceuticals`, `Mccarthy, Hoover and Rosario Pharmaceuticals`, `Allen-Olson Pharmaceuticals` … (40 distinct) |
| `unit_cost` | DECIMAL |  | 0 | `1.85`, `0.04`, `1.4` … (40 distinct) |
| `usage_weight` | DECIMAL |  | 0 | `1.5`, `1.6`, `1.4` … (34 distinct) |
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
| `icd10_code` | STRING | PK · FK ← `ehr/diagnoses.icd_code` | 0 | `A41.9`, `A41.51`, `R65.20` … (28 distinct) |
| `icd10_description` | STRING |  | 0 | `Sepsis, unspecified organism`, `Sepsis due to Escherichia coli [E. coli]`, `Severe sepsis without septic shock` … (28 distinct) |
| `icd_version` | INTEGER |  | 0 | `10` |
| `code_chapter` | STRING |  | 0 | `A`, `R`, `I`, `J`, `E`, `N`, `K` |
| `diagnosis_category` | STRING |  | 0 | `SEPSIS`, `HF`, `COPD` … (17 distinct) |
| `care_setting` | STRING |  | 0 | `IP`, `BOTH`, `ED` |
| `hrrp_cohort` | STRING |  | 0 | `AMI`, `HF`, `COPD`, `PN`, `OTHER` <sup>1</sup> |
| `is_chronic` | BOOLEAN |  | 0 | `0`, `1` |
| `readmission_risk_level` | STRING |  | 0 | `low`, `medium`, `high` |
| `relative_frequency` | DECIMAL |  | 0 | `8.0`, `1.6`, `1.2` … (24 distinct) |

<sup>1</sup> the CMS Hospital Readmissions Reduction Program cohorts. Join on the **principal**
diagnosis (`seq_num = 1`) only — a secondary HF code does not make a stay an HF-cohort case.


### `reference/dim_drg`

**Landing** OneLake Files · **Cadence** Day 0 + every Monday (full refresh) · **Format** CSV · **Observed volume** 42 per snapshot · **Columns** 7

Resolves `ehr/admissions.drg_code`. Enables case-mix adjustment — without it, comparing raw
length-of-stay or cost across facilities compares patient mix, not performance.

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `drg_code` | STRING <sup>1</sup> | PK · FK ← `ehr/admissions.drg_code` | 0 | `871`, `872`, `291` … (42 distinct) |
| `drg_description` | STRING |  | 0 | `Septicemia or severe sepsis without MV >96 hours with MCC`, `Heart failure and shock with MCC` … (42 distinct) |
| `relative_weight` | DECIMAL |  | 0 | `1.85`, `1.05`, `1.4` … (29 distinct) |
| `severity_tier` | STRING |  | 0 | `MCC`, `CC`, `NONE` <sup>2</sup> |
| `drg_family` | STRING |  | 0 | `SEPSIS`, `HF`, `COPD` … (17 distinct) |
| `drg_type` | STRING |  | 0 | `MED` <sup>3</sup> |
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
| `Id` | UUID | PK | 0 | `8ee6ac36-be45-4265-9abe-8a6cabc2647b`, `61f8d2e4-4e4b-4886-80ea-b3b159a05b79`, `0e40771f-70d7-42a4-ae12-0098a1e6a32d` … (4,958 distinct) |
| `BirthDate` | DATE |  | 0.16 | `1968-08-19`, `1924-09-11`, `1990-12-19` … (5,907 distinct) |
| `DeathDate` | STRING |  | 100 | — |
| `SSN` | STRING <sup>1</sup> |  | 0 | `075-12-0352`, `313-31-9262`, `800-64-0095` … (4,958 distinct) |
| `Drivers` | STRING |  | 2.5 | `S19951226`, `S48319176`, `S62323054` … (4,833 distinct) |
| `Passport` | STRING |  | 64.22 | `X90087772`, `X61779822`, `X41093101` … (1,768 distinct) |
| `Prefix` | STRING |  | 3.1 | `Ms.`, `Mr.` |
| `First` | STRING |  | 0 | `Michael`, `James`, `David` … (586 distinct) |
| `Middle` | STRING |  | 28.61 | `Michael`, `David`, `John` … (564 distinct) |
| `Last` | STRING |  | 0.15 | `Smith`, `Johnson`, `Williams` … (906 distinct) |
| `Suffix` | STRING |  | 100 | — |
| `Maiden` | STRING |  | 83.34 | `Smith`, `Johnson`, `Miller` … (429 distinct) |
| `Marital` | STRING |  | 20.54 | `S`, `D`, `W`, `M` |
| `Race` | STRING |  | 0 | `white`, `black`, `other`, `asian`, `native` — **plus 3 DQ-invalid value(s) by design** |
| `Ethnicity` | STRING |  | 0 | `nonhispanic`, `hispanic` — **plus 3 DQ-invalid value(s) by design** |
| `Gender` | STRING |  | 0.16 | `female`, `male`, `unknown`, `other` — **plus 4 DQ-invalid value(s) by design** |
| `BirthPlace` | STRING |  | 0 | `East Michael MA US`, `Andrewshire MA US`, `Heatherbury AZ US` … (4,782 distinct) |
| `Address` | STRING |  | 0 | `850 Williams Keys Apt. 022`, `564 Christine Land Suite 851`, `181 Joshua Groves` … (4,958 distinct) |
| `City` | STRING |  | 0 | `Boston`, `Oakland`, `Austin`, `Phoenix`, `Savannah`, `Chicago` |
| `State` | STRING |  | 0 | `MA`, `CA`, `TX`, `AZ`, `GA`, `IL` |
| `County` | STRING |  | 0 | `Suffolk`, `Alameda`, `Travis`, `Maricopa`, `Chatham`, `Cook` |
| `FIPS County Code` | INTEGER |  | 0 | `52727`, `47432`, `48374` … (4,754 distinct) |
| `Zip` | STRING <sup>1</sup> |  | 0 | `02118`, `94609`, `78702`, `85006`, `31404`, `02215`, `60612` |
| `Lat` | DECIMAL |  | 0 | `18.583309`, `85.645594`, `-55.049926` … (4,958 distinct) |
| `Lon` | DECIMAL |  | 0 | `80.917507`, `83.322615`, `87.942295` … (4,958 distinct) |
| `Healthcare_Expenses` | DECIMAL |  | 0 | `25506.07`, `91284.59`, `30005.85` … (4,958 distinct) |
| `Healthcare_Coverage` | DECIMAL |  | 0 | `11850.47`, `2761.5`, `10761.45` … (4,955 distinct) |
| `Income` | STRING |  | 0 | `41183`, `37805`, `16000` … (4,860 distinct) |
| `phone` | STRING |  | 0 | `663.856.5165x75692`, `001-405-408-8423x04220`, `001-711-572-2216x384` … (4,958 distinct) |
| `email` | STRING |  | 0 | `snyderpamela@example.com`, `lmartin@example.org`, `psmith@example.com` … (4,898 distinct) |
| `mrn` | STRING <sup>1</sup> | AK per facility | 0 | `330837549`, `330239746`, `140632059` … (high cardinality) |
| `source_facility_id` | STRING <sup>1</sup> | FK dim_facility | 0 | `330102`, `330101`, `050201`, `140401`, `030301`, `110501`, `450601` |
| `is_high_risk` | BOOLEAN |  | 0 | `0`, `1` <sup>2</sup> |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number

<sup>2</sup> a **source** flag, rising with age (~3% at 45, capped at 42%). It is the patient's
standing clinical risk, not a readmission prediction and not derived from anything else in this
contract. Do not re-derive it, and do not treat it as a modelling target — nothing downstream
depends on it, so a model trained on it is fitting the generator, not clinical reality.


### `ehr/encounters`

**Landing** OneLake Files · **Cadence** Daily — encounters closed on run_date − 1 · **Format** CSV · **Columns** 22

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `Id` | STRING | PK | 0 | `ENC000007809`, `ENC000016637`, `ENC000016803` … (6,978 distinct) |
| `Start` | TIMESTAMP |  | 0.17 | `2026-08-05 14:31:00`, `2026-08-05 09:43:00`, `2026-08-05 09:08:00` … (5,418 distinct) |
| `Stop` | TIMESTAMP |  | 0 | `2026-08-05 18:47:02`, `2026-08-05 21:06:44`, `2026-08-05 12:25:53` … (6,939 distinct) |
| `Patient` | STRING | FK patients.Id | 0.1 | `39e8234f-d349-405a-a7ad-78a744bcdcc3`, `e8a0c589-dd5e-46c1-bb6b-7a73f6add04e`, `31cde0a2-2d3c-4c12-b086-d5cd29ce8e81` … (4,955 distinct) |
| `Organization` | STRING <sup>1</sup> | FK dim_facility | 0 | `330102`, `330101`, `050201`, `140401`, `030301`, `110501`, `450601` |
| `Provider` | STRING | FK dim_staff | 0 | `STF002949`, `STF000875`, `STF000153` … (3,751 distinct) |
| `Payer` | STRING | FK dim_payer | 0 | `PAY001`, `PAY002`, `PAY005`, `PAY004`, `PAY003`, `PAY006`, `PAY007`, `PAY008`, `PAY009` |
| `EncounterClass` | STRING |  | 0.15 | `outpatient`, `emergency`, `inpatient`, `ambulatory`, `urgentcare` — **case varies by `source_system`** <sup>2</sup> — plus 4 DQ-invalid value(s) by design |
| `Code` | STRING |  | 0 | `R07.9`, `R10.9`, `I10` … (28 distinct) |
| `Description` | STRING |  | 0 | `Chest pain, unspecified`, `Unspecified abdominal pain`, `Essential (primary) hypertension` … (28 distinct) |
| `Base_Encounter_Cost` | DECIMAL |  | 0 | `594.99`, `852.84`, `640.53` … (6,677 distinct) |
| `Total_Claim_Cost` | STRING |  | 100 | — |
| `Payer_Coverage` | STRING |  | 100 | — |
| `ReasonCode` | STRING |  | 0 | `R07.9`, `R10.9`, `I10` … (28 distinct) |
| `ReasonDescription` | STRING |  | 7.45 | `Chest pain, unspecified`, `Unspecified abdominal pain`, `Essential (primary) hypertension`, `Type 2 diabetes mellitus with hyperglycemia`, `Urinary tract infection, site not specified`, `Unspecified atrial fibrillation`, `Dehydration`, `Chronic obstructive pulmonary disease with (acute) exacerbat`, `Heart failure, unspecified`, `Acute on chronic diastolic (congestive) heart failure`, `Chronic obstructive pulmonary disease with (acute) lower res`, `Myocardial infarction type 2`, `Pneumonia, unspecified organism`, `Unspecified bacterial pneumonia` … (+3) |
| `facility_id` | STRING <sup>1</sup> | FK dim_facility | 0 | `330102`, `330101`, `050201`, `140401`, `030301`, `110501`, `450601` |
| `unit_id` | STRING | FK dim_unit | 0 | `330102-MS`, `330101-OPC`, `050201-ED` … (81 distinct) <sup>3</sup> |
| `encounter_class_code` | STRING |  | 0.01 | `AMB`, `EMER`, `IMP` — **plus 4 DQ-invalid value(s) by design** |
| `encounter_status` | STRING |  | 0 | `finished` |
| `patient_class` | STRING |  | 0.02 | `O`, `E`, `I` — **plus 4 DQ-invalid value(s) by design** |
| `mrn` | STRING <sup>1</sup> | FK patients.mrn | 0 | `330631380`, `330837549`, `330239746` … (high cardinality) |
| `source_system` | STRING |  | 0 | `MERIDIAN_EHR_CORE`, `REGIONAL_HIS`, `COMMUNITY_CARE_EHR`, `ACADEMIC_CIS`, `URGENTCARE_CLOUD` |

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
| `subject_id` | UUID | FK patients.Id | 0 | `31cde0a2-2d3c-4c12-b086-d5cd29ce8e81`, `8ee6ac36-be45-4265-9abe-8a6cabc2647b`, `15fc1d2d-a1a3-4279-933e-904cc9497460` … (1,885 distinct) |
| `hadm_id` | STRING | PK | 0 | `HADM000013582`, `HADM000000264`, `HADM000000318` … (2,188 distinct) |
| `admittime` | TIMESTAMP |  | 0.14 | `2026-07-23 07:23:00`, `2026-07-31 11:59:00`, `2026-08-03 07:51:00` … (2,129 distinct) |
| `dischtime` | TIMESTAMP |  | 0 | `2026-08-05 19:13:30`, `2026-08-07 16:54:09`, `2026-08-07 16:36:37` … (2,182 distinct) |
| `deathtime` | TIMESTAMP |  | 96.76 | `2026-08-05 15:16:09`, `2026-08-05 07:24:34`, `2026-08-05 02:16:47` … (71 distinct) |
| `admission_type` | STRING |  | 0.27 | `EW EMER.`, `URGENT`, `ELECTIVE`, `EU OBSERVATION`, `SURGICAL SAME DAY ADMISSION`, `OBSERVATION ADMIT`, `DIRECT EMER.`, `DIRECT OBSERVATION` — **plus 2 DQ-invalid value(s) by design** |
| `admit_provider_id` | STRING | FK dim_staff | 0 | `STF003561`, `STF002244`, `STF001654` … (1,778 distinct) |
| `admission_location` | STRING |  | 0 | `EMERGENCY ROOM`, `PHYSICIAN REFERRAL`, `CLINIC REFERRAL`, `TRANSFER FROM HOSPITAL`, `AMBULATORY SURGERY TRANSFER`, `TRANSFER FROM SKILLED NURSING FACILITY` — **plus 3 DQ-invalid value(s) by design** |
| `discharge_location` | STRING |  | 0 | `HOME`, `SKILLED NURSING FACILITY`, `HOME HEALTH CARE`, `REHAB`, `DIED`, `ACUTE HOSPITAL`, `ASSISTED LIVING`, `PSYCH FACILITY`, `HEALTHCARE FACILITY`, `HOSPICE`, `AGAINST ADVICE`, `CHRONIC/LONG TERM ACUTE CARE`, `OTHER FACILITY` — **plus 1 DQ-invalid value(s) by design** |
| `insurance` | STRING |  | 0 | `Commercial`, `Medicare`, `Medicare Advantage`, `Medicaid`, `Medicaid MC`, `Self-Pay`, `Other` |
| `language` | STRING |  | 0 | `ENGLISH`, `SPANISH`, `OTHER` — **plus 1 DQ-invalid value(s) by design** |
| `marital_status` | STRING |  | 20.42 | `D`, `S`, `W`, `M` |
| `race` | STRING |  | 0 | `white`, `black`, `other`, `asian`, `native` |
| `edregtime` | TIMESTAMP |  | 23.76 | `2026-07-30 18:02:00`, `2026-08-04 12:35:00`, `2026-08-05 09:43:00` … (1,580 distinct) |
| `edouttime` | TIMESTAMP |  | 23.76 | `2026-08-02 16:19:50`, `2026-07-24 10:49:55`, `2026-07-25 07:35:12` … (1,668 distinct) |
| `hospital_expire_flag` | BOOLEAN |  | 0 | `0`, `1` |
| `facility_id` | STRING <sup>1</sup> | FK dim_facility | 0 | `330102`, `330101`, `050201`, `140401`, `030301`, `110501` |
| `admission_type_code` | STRING |  | 0 | `E`, `C`, `U` |
| `admit_decision_time` | TIMESTAMP |  | 23.76 | `2026-08-02 15:34:41`, `2026-08-04 23:33:16`, `2026-08-06 11:50:36` … (1,665 distinct) |
| `hospital_service` | STRING |  | 0 | `MED`, `ORTHO`, `OMED`, `SURG`, `PSYCH`, `VSURG`, `CMED`, `CSURG`, `OBS`, `TRAUM`, `NSURG`, `NB`, `GU`, `NMED` … (+3) |
| `transferred_in_within_6h` | BOOLEAN |  | 0 | `0`, `1` |
| `is_readmission` | BOOLEAN |  | 0 | `0`, `1` |
| `is_planned_readmission` | BOOLEAN |  | 0 | `0`, `1` |
| `index_encounter_id` | STRING | FK encounters.Id | 93.01 <sup>2</sup> | `ENC000002164`, `ENC000003402`, `ENC000004057` … (153 distinct) |
| `encounter_id` | STRING | FK encounters.Id | 0 | `ENC000013581`, `ENC000000263`, `ENC000000317` … (2,188 distinct) |
| `drg_code` | STRING <sup>1</sup> | FK dim_drg.drg_code | 0 | `291`, `871`, `194`, `689`, `292`, `309` … (39 distinct) |
| `source_system` | STRING |  | 0 | `MERIDIAN_EHR_CORE`, `REGIONAL_HIS`, `ACADEMIC_CIS`, `COMMUNITY_CARE_EHR` <sup>3</sup> |

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
| `subject_id` | UUID | FK patients.Id | 0 | `39e8234f-d349-405a-a7ad-78a744bcdcc3`, `e8a0c589-dd5e-46c1-bb6b-7a73f6add04e`, `ffc26862-5bae-4ffa-88eb-0c2f92b1c347` … (4,714 distinct) |
| `stay_id` | STRING | PK | 0 | `ENC000016597`, `ENC000018383`, `ENC000017085` … (6,458 distinct) |
| `encounter_id` | STRING | FK encounters.Id | 0 <sup>2</sup> | `ENC000016597`, `ENC000018383`, `ENC000017085` … (6,458 distinct) |
| `hadm_id` | STRING | FK admissions.hadm_id | 74.18 <sup>3</sup> | `HADM000017086`, `HADM000020866`, `HADM000002656` … (1,668 distinct) |
| `intime` | TIMESTAMP |  | 0.19 | `2026-08-05 14:31:00`, `2026-08-05 09:43:00`, `2026-08-05 13:59:00` … (5,072 distinct) |
| `outtime` | TIMESTAMP |  | 0 | `2026-08-12 12:44:27`, `2026-08-05 00:36:25`, `2026-08-05 02:32:58` … (6,429 distinct) |
| `gender` | STRING |  | 0 | `F`, `M`, `U` |
| `race` | STRING |  | 0 | `white`, `black`, `other`, `asian`, `native` |
| `arrival_transport` | STRING |  | 0 | `WALK IN`, `AMBULANCE`, `UNKNOWN`, `HELICOPTER` — **plus 4 DQ-invalid value(s) by design** |
| `disposition` | STRING |  | 0.11 | `HOME`, `ADMITTED`, `TRANSFER`, `LEFT AGAINST MEDICAL ADVICE`, `LEFT WITHOUT BEING SEEN`, `EXPIRED` — **plus 5 DQ-invalid value(s) by design** |
| `temperature` | DECIMAL |  | 0 | `99.1`, `99.4`, `99.0` … (66 distinct) |
| `heartrate` | STRING |  | 0 | `102`, `100`, `106` … (103 distinct) |
| `resprate` | INTEGER |  | 0 | `21`, `22`, `20` … (27 distinct) |
| `o2sat` | STRING |  | 0 | `93`, `94`, `92`, `95`, `91`, `96`, `90`, `97`, `89`, `98`, `88`, `99`, `87`, `86` … (+8) |
| `sbp` | STRING |  | 0 | `115`, `125`, `120` … (127 distinct) |
| `dbp` | INTEGER |  | 0 | `69`, `71`, `70` … (75 distinct) |
| `pain` | INTEGER |  | 0 | `3`, `2`, `5`, `0`, `8`, `9`, `10`, `4`, `7`, `6`, `1` |
| `acuity` | INTEGER |  | 0.23 | `3`, `4`, `2`, `5`, `1` |
| `chiefcomplaint` | STRING |  | 0 | `Chest pain, unspecified`, `Unspecified abdominal pain`, `Essential (primary) hypertension`, `Type 2 diabetes mellitus with hyperglycemia`, `Urinary tract infection, site not specified`, `Unspecified atrial fibrillation`, `Dehydration`, `Chronic obstructive pulmonary disease with (acute) exacerbat`, `Heart failure, unspecified`, `Acute on chronic diastolic (congestive) heart failure`, `Chronic obstructive pulmonary disease with (acute) lower res`, `Myocardial infarction type 2`, `Pneumonia, unspecified organism`, `Unspecified bacterial pneumonia` … (+3) |
| `triage_time` | TIMESTAMP |  | 0 | `2026-08-08 12:04:42`, `2026-08-04 21:59:38`, `2026-08-04 22:16:26` … (6,424 distinct) |
| `provider_seen_time` | TIMESTAMP |  | 0.2 <sup>4</sup> | `2026-08-05 13:31:07`, `2026-08-04 22:18:55` … (high cardinality) |
| `admit_decision_time` | TIMESTAMP |  | 73.69 | `2026-08-05 13:48:12`, `2026-08-04 23:33:16`, `2026-08-06 11:50:36` … (1,697 distinct) |
| `facility_id` | STRING <sup>1</sup> | FK dim_facility | 0 | `330102`, `330101`, `050201`, `140401`, `030301`, `110501`, `450601` |
| `source_system` | STRING |  | 0 | `MERIDIAN_EHR_CORE`, `REGIONAL_HIS`, `COMMUNITY_CARE_EHR`, `ACADEMIC_CIS`, `URGENTCARE_CLOUD` |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number

<sup>2</sup> the join key that makes a single visit fact possible. MIMIC-IV's `edstays` has no
encounter concept, so this column is the generator's addition: without it the 74% of ED stays
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
| `visit_id` | STRING | PK | 0 | `ENC000036013`, `ENC000081587`, `ENC000081588` … (high cardinality) |
| `encounter_id` | STRING | FK encounters.Id | 0 | `ENC000036013`, `ENC000081587`, `ENC000081588` … (high cardinality) |
| `subject_id` | UUID | FK patients.Id | 0 | `dac34977-d9ec-49e4-8928-0c4a764d9d99`, `45488135-6529-41c7-8143-9adf1fc6ec3e` … (high cardinality) |
| `facility_id` | STRING <sup>1</sup> | FK dim_facility | 0 | `050201`, `330101`, `450601`, `330102`, `030301`, `140401`, `110501` |
| `unit_id` | STRING | FK dim_unit | 0 | `050201-ASC`, `330101-OPC`, `050201-OPC`, `450601-OPC` … (12 distinct) |
| `clinic_type` | STRING |  | 0.26 | `OPC`, `ASC` — **plus DQ-invalid value(s) by design** |
| `appointment_time` | TIMESTAMP |  | 0.2 | `2026-07-05 07:20:00`, `2026-07-05 07:30:00` … (slot-aligned) |
| `arrival_time` | TIMESTAMP |  | 8.04 <sup>2</sup> | `2026-07-05 07:30:00`, `2026-07-05 07:13:00` … (high cardinality) |
| `provider_seen_time` | TIMESTAMP |  | 8.04 <sup>2</sup> | `2026-07-05 07:42:06`, `2026-07-05 07:46:13` … (high cardinality) |
| `departure_time` | TIMESTAMP |  | 0 | `2026-07-05 07:53:18`, `2026-07-05 08:39:25` … (high cardinality) |
| `seen_by_provider_id` | STRING | FK dim_staff.staff_id | 0 | `STF000040`, `STF004249`, `STF001741` … (high cardinality) |
| `visit_status` | STRING |  | 0.02 | `completed`, `no show`, `admitted` — **case varies by `source_system`** <sup>3</sup> |
| `is_no_show` | BOOLEAN |  | 0 | `0`, `1` |
| `escalated_to_inpatient` | BOOLEAN |  | 0 | `0`, `1` <sup>4</sup> |
| `primary_diagnosis_code` | STRING | FK dim_icd10.icd10_code | 0 | `N39.0`, `E11.65`, `I10` … (28 distinct) |
| `payer_id` | STRING | FK dim_payer.payer_id | 0 | `PAY001` … `PAY009` |
| `mrn` | STRING <sup>1</sup> |  | 0 | `050629790`, `330740755`, `330513123` … (high cardinality) |
| `source_system` | STRING |  | 0 | `MERIDIAN_EHR_CORE`, `URGENTCARE_CLOUD`, `ACADEMIC_CIS`, `REGIONAL_HIS`, `COMMUNITY_CARE_EHR` |

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
| `subject_id` | UUID | FK patients.Id | 0 | `8ee6ac36-be45-4265-9abe-8a6cabc2647b`, `31cde0a2-2d3c-4c12-b086-d5cd29ce8e81`, `508b7f4a-97a7-4d53-9b5f-a1d11403bc60` … (1,885 distinct) |
| `hadm_id` | STRING | FK admissions.hadm_id | 0 | `HADM000000226`, `HADM000004125`, `HADM000009913` … (2,188 distinct) |
| `transfer_id` | STRING | PK | 0 | `HADM000000264-1`, `HADM000000264-2`, `HADM000000264-3` … (7,039 distinct) |
| `eventtype` | STRING |  | 0 | `admit`, `discharge`, `ed`, `transfer` |
| `careunit` | STRING |  | 31.08 | `Emergency Department`, `Medical/Surgical (MS)`, `Telemetry (TELE)`, `Step-Down (SDU)`, `Medical Intensive Care (MICU)`, `Postpartum (PP)`, `Pediatrics (PEDS)`, `Labor & Delivery (LD)`, `Post-Anesthesia Care (PACU)`, `Psychiatric (PSY)`, `Specialty Care Oncology (ONC)`, `Surgical Intensive Care (SICU)`, `Cardiovascular ICU (CVICU)`, `Neonatal ICU (NICU)` … (+1) |
| `intime` | TIMESTAMP |  | 0 | `2026-08-05 14:00:00`, `2026-08-06 13:00:00`, `2026-08-05 02:00:00` … (6,166 distinct) |
| `outtime` | TIMESTAMP |  | 31.08 | `2026-08-05 14:00:00`, `2026-08-05 02:00:00`, `2026-08-06 13:00:00` … (4,202 distinct) |
| `facility_id` | STRING <sup>1</sup> | FK dim_facility | 0 | `330102`, `330101`, `050201`, `140401`, `030301`, `110501` |
| `unit_id` | STRING | FK dim_unit | 0 | `330102-MS`, `330102-ED`, `330101-MS` … (68 distinct) |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number


### `ehr/diagnoses`

**Landing** OneLake Files · **Cadence** Daily · **Format** CSV · **Columns** 9

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `subject_id` | UUID | FK patients.Id | 0 | `15fc1d2d-a1a3-4279-933e-904cc9497460`, `31cde0a2-2d3c-4c12-b086-d5cd29ce8e81`, `8ee6ac36-be45-4265-9abe-8a6cabc2647b` … (4,958 distinct) |
| `hadm_id` | STRING | FK admissions.hadm_id | 0 | `HADM000005809`, `HADM000008503`, `HADM000008775` … (6,978 distinct) |
| `encounter_id` | STRING | FK encounters.Id | 0 | `ENC000005808`, `ENC000008502`, `ENC000008774` … (6,978 distinct) |
| `seq_num` | INTEGER |  | 0 | `1`, `2`, `3`, `4`, `5`, `6`, `7`, `8` |
| `icd_code` | STRING |  | 0 | `R07.9`, `A41.9`, `R10.9` … (28 distinct) |
| `icd_version` | INTEGER |  | 0 | `10` |
| `icd_title` | STRING |  | 0 | `Chest pain, unspecified`, `Sepsis, unspecified organism`, `Unspecified abdominal pain` … (28 distinct) |
| `hrrp_cohort` | STRING |  | 0 | `OTHER`, `HF`, `PN`, `COPD`, `AMI` |
| `facility_id` | STRING <sup>1</sup> | FK dim_facility | 0 | `330102`, `330101`, `050201`, `140401`, `030301`, `110501` |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number


### — Billing & claims —

*Anchored on: Flattened X12 837I (claim) and 835 (remittance)*


### `claims/claim_header`

**Landing** OneLake Files · **Cadence** Daily — discharges 2–6 days prior · **Format** CSV · **Columns** 26

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `patient_control_number` | STRING | PK | 0 | `PCN000017068`, `PCN000017714`, `PCN000014662` … (4,328 distinct) |
| `encounter_id` | STRING | FK encounters.Id | 0 | `ENC000017068`, `ENC000017714`, `ENC000014662` … (4,328 distinct) |
| `hadm_id` | STRING | FK admissions.hadm_id | 68.2 | `HADM000014663`, `HADM000017130`, `HADM000000264` … (1,376 distinct) |
| `subject_id` | UUID | FK patients.Id | 0 | `5d01ae6a-4fb5-4868-b6fe-32d808977586`, `bc6471b3-2f54-4be2-b300-456f8340d8fb`, `616e1b3b-f4fd-4f66-9d37-6ff09d529c0e` … (3,418 distinct) |
| `facility_id` | STRING <sup>1</sup> | FK dim_facility | 0 | `330102`, `330101`, `050201`, `140401`, `030301`, `110501` |
| `total_charge_amount` | STRING |  | 0.07 | `1975.69`, `2399.85`, `1562.57` … (4,309 distinct) |
| `claim_filing_indicator_code` | STRING |  | 0 | `CI`, `MC`, `MB`, `16`, `09`, `WC` |
| `payer_id` | STRING | FK dim_payer | 0.12 | `PAY001`, `PAY002`, `PAY005`, `PAY004`, `PAY003`, `PAY006`, `PAY007`, `PAY008`, `PAY009` |
| `payer_name` | STRING |  | 0 | `Medicare Part A/B`, `Vantage Medicare Advantage`, `Atlas Health Commercial`, `Harborview Medicaid MC`, `State Medicaid FFS`, `Northwind PPO`, `Ironbridge HMO`, `Self-Pay`, `Other / Workers Comp` |
| `type_of_bill` | STRING <sup>1</sup> |  | 0 | `0131`, `0111` — **plus 1 DQ-invalid value(s) by design** |
| `statement_date_from` | TIMESTAMP |  | 0 | `2026-08-05 14:31:00`, `2026-08-07 20:30:00`, `2026-08-05 11:02:00` … (3,740 distinct) |
| `statement_date_to` | TIMESTAMP |  | 0 | `2026-08-05 05:14:25`, `2026-08-05 10:43:22`, `2026-08-05 14:06:10` … (4,310 distinct) |
| `admission_date_and_hour` | TIMESTAMP |  | 68.2 | `2026-07-31 11:59:00`, `2026-08-03 14:03:00`, `2026-08-04 07:49:00` … (1,344 distinct) |
| `discharge_time` | TIMESTAMP |  | 68.2 | `2026-08-07 21:56:04`, `2026-08-07 16:54:09`, `2026-08-08 17:23:14` … (1,373 distinct) |
| `admission_type_code` | STRING |  | 68.2 | `E`, `C`, `U` |
| `admission_source_code` | STRING |  | 0 | `7`, `D`, `1`, `4`, `E`, `2` — **plus 3 DQ-invalid value(s) by design** |
| `patient_status_code` | STRING |  | 0 | `01`, `03`, `06`, `62`, `04`, `20`, `51`, `65`, `02`, `07`, `63`, `43` — **plus 1 DQ-invalid value(s) by design** |
| `drg_code` | STRING |  | 100 | — |
| `principal_diagnosis` | STRING |  | 0.14 | `R07.9`, `R10.9`, `I10` … (28 distinct) |
| `admitting_diagnosis` | STRING |  | 0 | `R07.9`, `R10.9`, `I10` … (28 distinct) |
| `other_diagnoses` | STRING |  | 68.77 | `A41.9`, `E11.65`, `N39.0` … (1,055 distinct) |
| `attending_provider_npi` | STRING <sup>1</sup> | FK dim_staff.npi | 0 | `1892828089`, `1552010479`, `1550831754` … (2,484 distinct) |
| `medical_record_number` | STRING <sup>1</sup> | FK patients.mrn | 0 | `330239746`, `330967638`, `330617124` … (4,126 distinct) |
| `prior_authorization_number` | STRING |  | 58.23 | `AUTH41830870`, `AUTH86434287`, `AUTH13676334` … (1,809 distinct) |
| `submission_date` | DATE |  | 0 | `2026-08-11`, `2026-08-12`, `2026-08-13`, `2026-08-10`, `2026-08-09`, `2026-08-08`, `2026-08-07` |
| `is_readmission_related` | BOOLEAN |  | 0 | `0`, `1` <sup>2</sup> |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number

<sup>2</sup> the claim bills a stay flagged `admissions.is_readmission`. It is **HRRP penalty
exposure on the billing side**, and it is *not* the readmission-rate numerator: it carries no
index link, applies no HRRP exclusion, and includes planned readmissions. Use
`ehr/admissions` for the rate; use this only to price it.


### `claims/claim_line`

**Landing** OneLake Files · **Cadence** Daily · **Format** CSV · **Columns** 13

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `patient_control_number` | STRING | FK claim_header | 0 | `PCN000000563`, `PCN000013718`, `PCN000017474` … (4,328 distinct) |
| `line_control_number` | STRING | PK (with PCN) | 0 | `PCN000000627-002`, `PCN000007908-001`, `PCN000017095-004` … (17,996 distinct) |
| `revenue_code` | STRING <sup>1</sup> |  | 0.24 | `0450`, `0250`, `0320`, `0300`, `0730`, `0460`, `0121`, `0410`, `0360`, `0258`, `0200` — **plus 5 DQ-invalid value(s) by design** |
| `revenue_code_description` | STRING |  | 0 | `Emergency Room - General Classification`, `Pharmacy - General Classification`, `Radiology Diagnostic - General Classification`, `Laboratory - General Classification`, `EKG/ECG - General Classification`, `Pulmonary Function - General Classification`, `Room and Board Semi-private (two beds) - Medical/Surgical/Gyn`, `Respiratory Services - General Classification`, `Operating Room Services - General Classification`, `Pharmacy - IV Solutions`, `Intensive Care Unit - General Classification` |
| `procedure_code` | STRING |  | 81.62 | `J2270`, `J2543`, `J3370`, `Q9967`, `J0690`, `J1170`, `J3010`, `J2405`, `A4216`, `J2250`, `J1200`, `J1644`, `J1100`, `G0378` |
| `procedure_code_qualifier` | STRING |  | 81.62 | `HC` |
| `procedure_description` | STRING |  | 81.62 | `Injection, morphine sulfate, up to 10 mg`, `Injection, piperacillin/tazobactam, 1.125 g`, `Injection, vancomycin hcl, 500 mg`, `Low osmolar contrast material, 300-399 mg/ml iodine, per ml`, `Injection, cefazolin sodium, 500 mg`, `Injection, hydromorphone, up to 4 mg`, `Injection, fentanyl citrate, 0.1 mg`, `Injection, ondansetron hydrochloride, per 1 mg`, `Sterile water, saline and/or dextrose, diluent/flush, 10 ml`, `Injection, midazolam hydrochloride, per 1 mg`, `Injection, diphenhydramine hcl, up to 50 mg`, `Injection, heparin sodium, per 1000 units`, `Injection, dexamethasone sodium phosphate, 1 mg`, `Hospital observation service, per hour` |
| `line_charge_amount` | STRING |  | 0.2 | `4260.0`, `2840.0`, `5680.0` … (15,891 distinct) |
| `unit_type` | STRING |  | 0 | `UN`, `DA` |
| `unit_count` | STRING |  | 0 | `1`, `3`, `2` … (36 distinct) |
| `non_covered_amount` | DECIMAL |  | 0 | `0.0` |
| `service_date_from` | TIMESTAMP |  | 0 | `2026-08-05 14:31:00`, `2026-08-07 20:30:00`, `2026-07-31 11:59:00` … (3,739 distinct) |
| `service_date_to` | TIMESTAMP |  | 0 | `2026-08-07 16:54:09`, `2026-08-07 16:36:37`, `2026-08-09 08:50:16` … (4,310 distinct) |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number


### `claims/remit`

**Landing** OneLake Files · **Cadence** Daily · **Format** CSV · **Columns** 16

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `patient_control_number` | STRING | FK claim_header | 0 | `PCN000019342`, `PCN000017977`, `PCN000020723` … (4,153 distinct) |
| `payer_id` | STRING | FK dim_payer | 0 | `PAY001`, `PAY002`, `PAY005`, `PAY004`, `PAY003`, `PAY006`, `PAY007`, `PAY009` |
| `claim_status_code` | STRING <sup>1</sup> |  | 0.12 | `1`, `4` — **plus 5 DQ-invalid value(s) by design** |
| `claim_status_description` | STRING |  | 0 | `Processed as Primary`, `Denied` |
| `total_claim_charge_amount` | DECIMAL |  | 0 | `3509.38`, `1975.69`, `1562.57` … (4,138 distinct) |
| `claim_payment_amount` | STRING |  | 0.17 | `0.0`, `1039.78`, `886.59` … (3,660 distinct) |
| `patient_responsibility_amount` | STRING |  | 0 | `0.0`, `75.75`, `18.25` … (3,569 distinct) |
| `payer_claim_control_number` | STRING |  | 0 | `ICN709084810649`, `ICN215817877487`, `ICN812903307131` … (4,153 distinct) |
| `drg_code` | STRING |  | 100 | — |
| `drg_weight` | STRING |  | 100 | — |
| `check_eft_trace_number` | STRING |  | 0 | `EFT644461113`, `EFT201339959`, `EFT720288748` … (4,153 distinct) |
| `payment_method_code` | STRING |  | 0 | `CHK`, `ACH` |
| `check_date` | DATE |  | 0 | `2026-09-17`, `2026-09-14`, `2026-09-18` … (230 distinct) |
| `remit_date` | DATE |  | 0 | `2026-09-17`, `2026-09-14`, `2026-09-18` … (230 distinct) |
| `is_appealed` | BOOLEAN |  | 88.55 | `1`, `0` |
| `is_overturned_on_appeal` | BOOLEAN |  | 92.33 | `0`, `1` |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number


### `claims/remit_adjustment`

**Landing** OneLake Files · **Cadence** Daily · **Format** CSV · **Columns** 11

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `patient_control_number` | STRING | FK claim_header | 0 | `PCN000000263`, `PCN000000549`, `PCN000000627` … (4,153 distinct) |
| `adjustment_seq` | INTEGER | PK (with PCN) | 0 | `1`, `2` |
| `group_code` | STRING |  | 0 | `CO`, `PR`, `OA` |
| `group_code_description` | STRING |  | 0 | `Contractual Obligation`, `Patient Responsibility`, `Other Adjustment` |
| `reason_code` | STRING <sup>1</sup> |  | 0 | `45`, `1`, `2`, `3`, `16`, `50`, `197`, `96`, `18`, `29`, `27`, `204`, `109`, `252` … (+7) |
| `reason_code_description` | STRING |  | 0 | `Charge exceeds fee schedule/maximum allowable or contracted/legislated fee arrangement.`, `Deductible Amount`, `Coinsurance Amount`, `Co-payment Amount`, `Claim/service lacks information or has submission/billing error(s).`, `These are non-covered services because this is not deemed a "medical necessity" by the payer.`, `Precertification/authorization/notification/pre-treatment absent.`, `Non-covered charge(s).`, `Exact duplicate claim/service`, `The time limit for filing has expired.`, `Expenses incurred after coverage terminated.`, `This service/equipment/drug is not covered under the patient's current benefit plan`, `Claim/service not covered by this payer/contractor. You must send the claim/service to the correct payer/contractor.`, `An attachment/other documentation is required to adjudicate this claim/service.` … (+7) |
| `amount` | DECIMAL |  | 0 | `75.75`, `18.25`, `271.43` … (8,154 distinct) |
| `quantity` | STRING |  | 100 | — |
| `remark_code` | STRING <sup>1</sup> |  | 98.5 | `MA130` |
| `remark_code_description` | STRING |  | 98.5 | `Your claim contains incomplete and/or invalid information, and no appeal rights are afforded because the claim is unprocessable.` |
| `is_denial` | BOOLEAN |  | 0 | `0`, `1` |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number


### — Bed capacity —

*Anchored on: CDC NHSN Hospital Respiratory Data*


### `beds/hourly_snapshot`

**Landing** OneLake Files · **Cadence** Hourly, batched into one daily file · **Format** CSV · **Columns** 13

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `snapshot_datetime` | TIMESTAMP | PK (with unit_id) | 0 | `2026-08-05 04:00:00`, `2026-08-05 15:00:00`, `2026-08-05 20:00:00` … (192 distinct) |
| `facility_id` | STRING <sup>1</sup> | FK dim_facility | 0 | `330102`, `330101`, `140401`, `050201`, `030301`, `110501` |
| `unit_id` | STRING | FK dim_unit | 0 | `330101-LD`, `140401-PP`, `330101-CVICU` … (62 distinct) |
| `unit_code` | STRING | FK dim_unit.unit_code | 0 | `PACU`, `MS`, `TELE`, `MICU`, `LD`, `PP`, `PEDS`, `SDU`, `CVICU`, `ONC`, `PSY`, `SICU`, `NICU`, `REHAB` |
| `licensed_beds` | INTEGER |  | 0 | `23`, `21`, `19` … (30 distinct) |
| `staffed_beds` | STRING |  | 0.18 | `16`, `13`, `21` … (44 distinct) |
| `blocked_beds` | INTEGER |  | 0 | `0`, `1`, `2`, `3` |
| `occupied_beds` | STRING |  | 0.18 | `13`, `11`, `8` … (163 distinct) |
| `available_beds` | STRING |  | 0 | `0`, `1`, `2` … (57 distinct) |
| `pending_admissions` | INTEGER |  | 0 | `6`, `5`, `8` … (36 distinct) |
| `pending_discharges` | BOOLEAN |  | 0 | `0` |
| `occupancy_rate` | DECIMAL |  | 0 | `1.0`, `0.6667`, `0.75` … (284 distinct) |
| `is_at_capacity` | BOOLEAN |  | 0 | `0`, `1` |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number


### `beds/nhsn_weekly`

**Landing** OneLake Files · **Cadence** Weekly — week ending Sunday, measured Wednesday · **Format** CSV · **Columns** 16

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `nhsn_org_id` | STRING <sup>1</sup> | FK dim_facility.facility_id | 0 | `330101`, `330102`, `050201`, `030301`, `140401`, `110501` |
| `facility_name` | STRING |  | 0 | `Meridian General Hospital – Boston`, `Meridian University Hospital`, `Meridian General Hospital – Oakland`, `Meridian Regional Medical Center – Phoenix`, `Meridian General Hospital – Chicago`, `Meridian Community Hospital – Savannah` |
| `week_ending_date` | DATE | PK (with org) | 0 | `2026-08-09` |
| `collection_date` | DATE |  | 0 | `2026-08-05` |
| `all_hospital_inpatient_beds` | INTEGER |  | 0 | `353`, `532`, `314`, `159`, `287`, `45` |
| `all_hospital_inpatient_occupancy` | INTEGER |  | 0 | `293`, `463`, `255`, `145`, `236`, `39` |
| `all_adult_inpatient_beds` | INTEGER |  | 0 | `333`, `489`, `297`, `148`, `271`, `45` |
| `all_adult_inpatient_occupancy` | INTEGER |  | 0 | `279`, `428`, `243`, `134`, `224`, `39` |
| `all_pediatric_inpatient_beds` | INTEGER |  | 0 | `20`, `43`, `17`, `11`, `16`, `0` |
| `all_pediatric_inpatient_occupancy` | INTEGER |  | 0 | `13`, `34`, `11`, `10`, `12`, `0` |
| `all_icu_beds` | INTEGER |  | 0 | `51`, `88`, `46`, `13`, `41`, `5` |
| `all_icu_bed_occupancy` | INTEGER |  | 0 | `42`, `81`, `39`, `12`, `34`, `4` |
| `adult_icu_beds` | INTEGER |  | 0 | `51`, `72`, `46`, `13`, `41`, `5` |
| `adult_icu_bed_occupancy` | INTEGER |  | 0 | `42`, `67`, `39`, `12`, `34`, `4` |
| `pediatric_icu_beds` | INTEGER |  | 0 | `0`, `16` |
| `pediatric_icu_bed_occupancy` | INTEGER |  | 0 | `0`, `13` |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number


### — Pharmacy —

*Anchored on: FHIR R5 `InventoryReport` semantics + FDA NDC + RxNorm*


### `pharmacy/inventory`

**Landing** OneLake Files · **Cadence** Daily snapshot · **Format** CSV · **Columns** 34

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `snapshot_date` | DATE | PK (with facility+ndc11) | 0 | `2026-08-12`, `2026-08-09`, `2026-08-10`, `2026-08-06`, `2026-08-07`, `2026-08-08`, `2026-08-11`, `2026-08-13` |
| `counting_datetime` | TIMESTAMP |  | 0 | `2026-08-12 02:15:00`, `2026-08-09 02:15:00`, `2026-08-10 02:15:00`, `2026-08-06 02:15:00`, `2026-08-07 02:15:00`, `2026-08-08 02:15:00`, `2026-08-11 02:15:00`, `2026-08-13 02:15:00` |
| `count_type` | STRING |  | 0 | `snapshot` |
| `facility_id` | STRING <sup>1</sup> | FK dim_facility | 0 | `140401`, `330102`, `050201`, `330101`, `110501`, `030301`, `450601` |
| `location_id` | STRING |  | 0 | `330101-PHARM-MAIN`, `030301-PHARM-MAIN`, `330102-PHARM-MAIN` … (40 distinct) |
| `ndc11` | STRING <sup>1</sup> | FK dim_drug.ndc11 | 0 | `11221103330`, `11258103430`, `10444101230` … (40 distinct) |
| `product_ndc` | STRING <sup>1</sup> |  | 0 | `11221-1033`, `11258-1034`, `10444-1012` … (40 distinct) |
| `gtin14` | STRING <sup>1</sup> |  | 0 | `00311221103331`, `00311258103438`, `00310444101234` … (40 distinct) |
| `rxcui_scd` | INTEGER | FK dim_drug.rxcui_scd | 0 | `389776`, `553043`, `1375608` … (40 distinct) |
| `drug_name` | STRING |  | 0 | `Midazolam 2 MG/2ML`, `Lorazepam 2 MG/ML`, `Insulin Aspart 100 UNT/ML` … (40 distinct) |
| `dosage_form_name` | STRING |  | 0 | `INJECTION, SOLUTION`, `TABLET`, `INJECTION, POWDER`, `INJECTION`, `INJECTION, EMULSION`, `INHALATION SOLUTION`, `TABLET, DELAYED RELEASE`, `CAPSULE` |
| `route_name` | STRING |  | 0 | `INTRAVENOUS`, `ORAL`, `SUBCUTANEOUS`, `RESPIRATORY` |
| `pharm_classes` | STRING |  | 0 | `Opioid Analgesic`, `Benzodiazepine`, `General Anesthetic` … (29 distinct) |
| `lot_number` | STRING |  | 0.14 | `L891093`, `L622575`, `L505147` … (895 distinct) |
| `expiration_date` | DATE |  | 0.13 | `2027-11-15`, `2028-05-12`, `2028-03-05` … (560 distinct) |
| `qty_on_hand` | STRING |  | 0.2 | `7`, `6`, `8` … (186 distinct) |
| `base_unit` | STRING |  | 0 | `EA` |
| `qty_on_order` | INTEGER |  | 0 | `0`, `30`, `16`, `18`, `25`, `19`, `23`, `61`, `32`, `72`, `29`, `62`, `31`, `60` … (+5) |
| `par_level` | STRING |  | 0 | `12`, `10`, `11` … (131 distinct) |
| `reorder_point` | INTEGER |  | 0 | `2`, `3`, `4` … (51 distinct) |
| `safety_stock` | INTEGER |  | 0 | `1`, `2`, `3` … (36 distinct) |
| `avg_daily_usage_30d` | DECIMAL |  | 0 | `0.4`, `0.53`, `0.57` … (197 distinct) |
| `days_on_hand` | DECIMAL |  | 0 | `15.0`, `17.5`, `20.0` … (1,114 distinct) |
| `abc_class` | STRING |  | 0 | `C`, `A`, `B` — **plus 3 DQ-invalid value(s) by design** |
| `is_controlled` | BOOLEAN |  | 0 | `0`, `1` |
| `dea_schedule` | STRING |  | 61.49 | `CIV`, `CII`, `CIII`, `CV` — **plus 2 DQ-invalid value(s) by design** |
| `is_high_alert` | BOOLEAN |  | 0 | `0`, `1` |
| `shortage_status` | STRING |  | 0 | `Available`, `Currently in Shortage` — **plus 4 DQ-invalid value(s) by design** |
| `shortage_reason` | STRING |  | 98.25 | `Discontinuation of the manufacture of the drug` |
| `unit_cost` | STRING |  | 0 | `2.8`, `3.1`, `26.4` … (56 distinct) |
| `extended_value` | DECIMAL |  | 0 | `58.8`, `1.8`, `2.1` … (1,145 distinct) |
| `last_count_variance` | INTEGER |  | 0 | `0`, `1`, `-1`, `2`, `-2` |
| `is_stockout` | BOOLEAN |  | 0 | `0` |
| `last_restocked_at` | DATE |  | 0 | `2026-07-08`, `2026-07-04`, `2026-06-25` … (29 distinct) <sup>2</sup> |

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
| `Unit` | STRING |  | 0 | `Medical/Surgical (MS)`, `Emergency Department (ED)`, `Medical Intensive Care (MICU)`, `Telemetry (TELE)`, `Step-Down (SDU)`, `Surgical Intensive Care (SICU)`, `Labor & Delivery (LD)`, `Cardiovascular ICU (CVICU)`, `Post-Anesthesia Care (PACU)`, `Postpartum (PP)`, `Specialty Care Oncology (ONC)`, `Pediatrics (PEDS)`, `Psychiatric (PSY)`, `Neonatal ICU (NICU)` … (+1) |
| `Unit Code` | STRING | FK dim_unit.unit_code | 0 | `MS`, `ED`, `MICU`, `TELE`, `SDU`, `SICU`, `LD`, `CVICU`, `PACU`, `PP`, `ONC`, `PEDS`, `PSY`, `NICU` … (+1) |
| `Work Date` | STRING |  | 0 | `2026-08-12`, `2026-08-11`, `2026-08-10`, `08/12/2026`, `08/11/2026`, `08/10/2026`, `2026-08-14`, `2026-08-15`, `2026-08-13`, `2026-08-16`, `08/16/2026`, `08/13/2026`, `08/14/2026`, `08/15/2026` |
| `Shift` | STRING |  | 0 | `D`, `E`, `N`, `OC` <sup>2</sup> |
| `Shift Start` | STRING |  | 0 | `07:00`, `15:00`, `23:00`, `19:00` |
| `Shift End` | STRING |  | 0 | `15:00`, `23:00`, `07:00` |
| `Staff ID` | STRING | FK dim_staff.staff_id | 0 | `STF002180`, `STF003512`, `STF002694` … (1,633 distinct) |
| `Name` | STRING |  | 0 | `Johnson, Anthony`, `Rogers, Michelle`, `Wilson, Shannon` … (1,615 distinct) |
| `Job Code` | STRING |  | 0 | `RN`, `LPN`, `NP` |
| `Employment Type` | INTEGER |  | 0 | `1`, `2` |
| `Scheduled Hours` | INTEGER |  | 0 | `8` (D/E/N), `12` (OC) |
| `Actual Hours` | DECIMAL |  | 31.70 <sup>3</sup> | `8`, `10`, `12`, `14`, `16`, `0` |
| `Status` | STRING |  | 0 | `completed`, `absent`, `scheduled`, `swapped`, `cancelled` |
| `Overtime` | STRING |  | 89.73 | `Y` |
| `Called Out` | STRING |  | 95.45 | `Y` |
| `Floated In` | STRING |  | 94.81 | `Y` |
| `Census` | INTEGER |  | 0 | `13`, `11`, `19` … (50 distinct) |
| `Notes` | STRING |  | 50.52 | `orientee paired`, `float pool`, `double shift`, `agency` |

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

**Landing** Eventstream (Kafka) + gzipped JSONL archive · **Cadence** Continuous · **Format** JSON · **Observed volume** 6000 sampled · **Columns** 7

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `event_id` | UUID | PK (deterministic uuid5) | 0 | `3f0745e1-e091-51e5-accc-763bbc23ba14`, `984c6b81-ee4d-52a8-b3f4-7a0df51863d5`, `1486fe4b-a0d7-5362-aa0b-b24fd581d109` … (5,991 distinct) |
| `event_type` | STRING |  | 0 | `vitals.reading` |
| `event_time` | TIMESTAMP |  | 0 | `2026-08-05T01:15:00`, `2026-08-05T02:25:00`, `2026-08-05T03:00:00` … (288 distinct) |
| `ingest_time` | TIMESTAMP |  | 0 | `2026-08-05T02:10:09`, `2026-08-05T04:05:06`, `2026-08-05T00:00:07` … (2,373 distinct) |
| `source_system` | STRING |  | 0 | `PHILIPS_IX_MONITOR` |
| `facility_id` | INTEGER | FK dim_facility | 0 | `330102`, `330101` |
| `schema_version` | DECIMAL |  | 0 | `1.0` |


### `stream/patient-vitals — payload`

**Landing** Eventstream (Kafka) + gzipped JSONL archive · **Cadence** Continuous · **Format** JSON · **Observed volume** 6000 sampled · **Columns** 13

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `patient_id` | UUID | FK patients.Id | 0 | `21606971-cec3-44d5-bf5a-782fcfd506b9`, `ea06913f-e3c8-4f90-a82e-e654fc5b10fd`, `2c825a52-c7ab-4dc4-a82c-4b5580575d0e` |
| `encounter_id` | STRING | FK encounters.Id | 0 | `ENC000000305`, `ENC000000317`, `ENC000000351`, `ENC000000429` |
| `hadm_id` | STRING | FK admissions.hadm_id | 0 | `HADM000000306`, `HADM000000318`, `HADM000000352`, `HADM000000430` |
| `unit_id` | STRING | FK dim_unit | 0 | `330102-NICU`, `330101-PACU`, `330102-MICU` |
| `bed_id` | STRING |  | 0 | `330101-PACU-B09`, `330102-MICU-B30`, `330102-NICU-B08`, `330102-NICU-B16` |
| `device_id` | STRING |  | 0 | `MON-PACU-040`, `MON-MICU-003`, `MON-NICU-013`, `MON-NICU-024` |
| `charttime` | TIMESTAMP |  | 0 | `2026-08-05T01:15:00`, `2026-08-05T02:25:00`, `2026-08-05T03:00:00` … (288 distinct) |
| `loinc_code` | STRING |  | 0 | `8867-4`, `8462-4`, `8480-6`, `9279-1`, `8310-5`, `2708-6` |
| `parameter_name` | STRING |  | 0 | `Heart rate`, `Diastolic arterial blood pressure`, `Systolic arterial blood pressure`, `Respiratory Rate`, `Body temperature`, `Oxygen saturation in Arterial blood` |
| `value_num` | DECIMAL |  | 0.47 | `93`, `91`, `92` … (215 distinct) |
| `value_uom` | STRING |  | 0 | `/min`, `mm[Hg]`, `Cel`, `%` |
| `warning` | BOOLEAN |  | 0 | `0`, `1` |
| `is_artifact` | BOOLEAN |  | 0 | `False`, `True` |


### — Streaming — prescription events —

*Anchored on: eICU `vitalPeriodic` + LOINC (vitals); MIMIC-IV `hosp.pharmacy` + FHIR `MedicationRequest`*


### `stream/prescription-events — envelope`

**Landing** Eventstream (Kafka) + gzipped JSONL archive · **Cadence** Continuous · **Format** JSON · **Observed volume** 6000 sampled · **Columns** 7

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `event_id` | UUID | PK (deterministic uuid5) | 0 | `818cd844-f35e-5f86-aa26-1e96b13c1a11`, `5dc2fd9d-32b9-548c-b71c-7002fbb22b43`, `6a8cdf45-267c-57d4-9af1-5913fe022523` … (5,987 distinct) |
| `event_type` | STRING |  | 0 | `prescription.issued` |
| `event_time` | TIMESTAMP |  | 0 | `2026-08-05T01:36:01`, `2026-08-05T05:53:47`, `2026-08-05T05:02:02` … (5,759 distinct) |
| `ingest_time` | TIMESTAMP |  | 0 | `2026-08-05T16:11:21`, `2026-08-05T03:59:10`, `2026-08-05T05:57:19` … (5,745 distinct) |
| `source_system` | STRING |  | 0 | `PHARMACY_OMS` |
| `facility_id` | STRING <sup>1</sup> | FK dim_facility | 0 | `330102`, `330101`, `050201`, `140401`, `030301`, `110501` |
| `schema_version` | DECIMAL |  | 0 | `1.0` |

<sup>1</sup> identifier — leading zeros are significant, never cast to a number


### `stream/prescription-events — payload`

**Landing** Eventstream (Kafka) + gzipped JSONL archive · **Cadence** Continuous · **Format** JSON · **Observed volume** 6000 sampled · **Columns** 36

| Column | Type | Key | Null % | Domain / sample |
|---|---|---|---:|---|
| `subject_id` | UUID | FK patients.Id | 0 | `bc6471b3-2f54-4be2-b300-456f8340d8fb`, `15fc1d2d-a1a3-4279-933e-904cc9497460`, `6b19927a-dc96-4c77-b8bd-f38800e150a3` … (707 distinct) |
| `hadm_id` | STRING | FK admissions.hadm_id | 0 | `HADM000012768`, `HADM000011315`, `HADM000011486` … (785 distinct) |
| `encounter_id` | STRING | FK encounters.Id | 0 | `ENC000012767`, `ENC000011314`, `ENC000011485` … (785 distinct) |
| `pharmacy_id` | INTEGER |  | 0 | `4742743`, `8752823`, `5927144` … (5,985 distinct) |
| `poe_id` | STRING |  | 0 | `HADM000000504-417`, `HADM000000506-368`, `HADM000000564-794` … (5,964 distinct) |
| `poe_seq` | INTEGER |  | 0 | `290`, `969`, `51` … (997 distinct) |
| `order_provider_id` | STRING | FK dim_staff.staff_id | 0 | `STF000126`, `STF001109`, `STF003497` … (3,507 distinct) |
| `drug` | STRING |  | 0 | `Sodium Chloride 0.9%`, `Acetaminophen 325 MG`, `Ondansetron 4 MG/2ML` … (40 distinct) |
| `drug_type` | STRING |  | 0 | `MAIN`, `BASE`, `ADDITIVE` |
| `formulary_drug_cd` | STRING |  | 0 | `SODIUMCHLOR`, `ACETAMINOPHE`, `ONDANSETRON` … (40 distinct) |
| `gsn` | INTEGER |  | 0 | `10859`, `3299`, `73425` … (5,823 distinct) |
| `ndc` | STRING <sup>1</sup> | FK dim_drug.ndc11 | 0 | `10000100030`, `10037100130`, `10074100230` … (40 distinct) |
| `rxcui_scd` | INTEGER | FK dim_drug.rxcui_scd | 0 | `1799706`, `721063`, `328689` … (40 distinct) |
| `prod_strength` | STRING |  | 0 | `Sodium Chloride 0.9%`, `Acetaminophen 325 MG`, `Ondansetron 4 MG/2ML` … (40 distinct) |
| `dose_val_rx` | DECIMAL |  | 0 | `2`, `25`, `1`, `50`, `5`, `100`, `20`, `1000`, `4`, `10`, `40`, `500`, `12.5`, `0.5` |
| `dose_unit_rx` | STRING |  | 0 | `mEq`, `mg`, `UNT`, `mcg`, `mL` |
| `form_val_disp` | INTEGER |  | 0 | `3`, `2`, `1`, `4` |
| `form_unit_disp` | STRING |  | 0 | `TAB`, `BAG`, `VIAL`, `SYR` |
| `doses_per_24_hrs` | DECIMAL |  | 0 | `1.0`, `6.0`, `3.0`, `2.0`, `4.0` |
| `route` | STRING |  | 0 | `INTRAVENOUS`, `ORAL`, `SUBCUTANEOUS`, `RESPIRATORY` |
| `frequency` | STRING |  | 0 | `Q12H`, `TID`, `PRN`, `BID`, `Q24H`, `Q6H`, `Q8H`, `ONCE` |
| `proc_type` | STRING |  | 0 | `Non-formulary`, `Large Volume`, `Unit Dose`, `IV Piggyback` |
| `status` | STRING |  | 0 | `active`, `inactive`, `discontinued` |
| `fhir_status` | STRING |  | 0 | `active`, `completed`, `stopped` |
| `fhir_intent` | STRING |  | 0 | `order` |
| `fhir_priority` | STRING |  | 0 | `routine`, `stat` |
| `entertime` | TIMESTAMP |  | 0 | `2026-08-05T01:36:01`, `2026-08-05T05:53:47`, `2026-08-05T05:02:02` … (5,759 distinct) |
| `verifiedtime` | TIMESTAMP |  | 0 | `2026-08-05T05:30:27`, `2026-08-05T06:13:54`, `2026-08-05T01:37:41` … (5,779 distinct) |
| `starttime` | TIMESTAMP |  | 0 | `2026-08-05T05:51:23`, `2026-08-05T04:20:07`, `2026-08-05T23:27:49` … (5,787 distinct) |
| `stoptime` | TIMESTAMP |  | 0 | `2026-08-05T12:56:37`, `2026-08-08T16:52:43`, `2026-08-08T02:20:03` … (5,931 distinct) |
| `dispensation` | STRING |  | 0 | `Main Pharmacy`, `Satellite`, `ADC` |
| `fill_quantity` | INTEGER |  | 0 | `1`, `5`, `3`, `6`, `4`, `2` |
| `is_controlled` | BOOLEAN |  | 0 | `False`, `True` |
| `dea_schedule` | STRING |  | 84.7 | `CII`, `CIV`, `CIII`, `CV` |
| `unit_id` | STRING | FK dim_unit | 0 | `330102-MS`, `330101-MS`, `050201-MS` … (52 distinct) |
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
| `diagnoses.hadm_id` | `admissions.hadm_id` | `HADM…` | Must resolve; 1:N |
| `ed_stays.hadm_id` | `admissions.hadm_id` | `HADM…` | **Nullable by design** — empty for patients discharged from ED (74% of ED stays). Join via `encounter_id` instead. |
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
| both streams `.encounter_id` | `encounters.Id` | `ENC…` | Must resolve |
| both streams `.unit_id` | `dim_unit.unit_id` | | Must resolve |

**Reconciliation rule.** `beds/nhsn_weekly` must reconcile against `beds/hourly_snapshot`
measured as of the Wednesday of the reporting week. Store the variance; do not discard it.

**Identity rule.** `mrn` is **per facility, not per patient**. 1,189 of 4,958 patients (24%)
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
> significant payer finding in the data. `unit_type` **must** keep all 15 — the Title 22 nurse
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
