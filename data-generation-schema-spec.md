# Data Generation Schema Specification
## Meridian Health Network — Patient Care & Hospital Operations Analytics Platform

**Owner:** Mubarak Tijani (data generation & source supply)
**Version:** 1.0 — 12 August 2026
**Purpose:** Define the schema for all 7 source feeds before any generator code is written, so that every field, code value and distribution is anchored in a real, citable, openly-licensed healthcare schema rather than invented.

---

## 1. Governing principle

> **No invented field names. No invented code values. No invented distributions.**

Every table below is anchored to a published schema. Where a field is our own addition (because no open standard defines it, or because the reference schema has a genuine gap), it is marked **`[OURS]`** with the reason. Where a value could not be verified, it is marked **⚠️** and must be confirmed before it is hard-coded.

The second principle matters as much as the first:

> **This is one hospital simulation, not seven independent file generators.**

If each source is generated independently, the referential-integrity and cross-source DQ checks the client requires (Section 4 of the client request) become trivially passable or trivially broken — and the dashboards will show nonsense (bed occupancy that doesn't move when patients are admitted, prescriptions for discharged patients, claims with no matching encounter). The generator must run a **single event-driven patient-journey simulation** and then *project* that state into the 7 source feeds:

```
patient arrives → ED encounter → triage → bed assigned  →  bed occupancy snapshot
                                       ↓
                              admitted (or discharged)
                                       ↓
                    vitals monitored ────────────────────→  vitals stream (Kafka)
                    drugs ordered   ────────────────────→  prescription stream (Kafka)
                         ↓                                        ↓
                    inventory depleted ──────────────────→  pharmacy inventory extract
                         ↓
                    transfers, LOS ─────────────────────→  EHR encounters extract
                         ↓
                    discharge → coded → billed ─────────→  claims extract
                         ↓
                    (30-day window) → possible readmission
                                       ↓
                    staff rostered against census ──────→  staff schedule (SharePoint)
```

---

## 2. Licensing — what we can legally ship

This matters: a graded deliverable that embeds a licensed code set is a real problem.

| Code set | Status | Action |
|---|---|---|
| **ICD-10-CM / ICD-10-PCS** | Free, US Govt work | Embed. [CMS ICD-10](https://www.cms.gov/medicare/coding-billing/icd-10-codes) |
| **MS-DRG** | Free (all files, no charge) | Embed. [CMS MS-DRG](https://www.cms.gov/medicare/payment/prospective-payment-systems/acute-inpatient-pps/ms-drg-classifications-and-software) |
| **HCPCS Level II** | Free CMS public use file, quarterly | Embed. [CMS quarterly update](https://www.cms.gov/medicare/coding-billing/healthcare-common-procedure-system/quarterly-update) |
| **NDC (FDA)** | Public domain, daily refresh | Embed. `accessdata.fda.gov/cder/ndctext.zip` |
| **RxNorm** | *Prescribable Content* release needs **no licence**; full release needs a free UMLS account | Use Prescribable Content |
| **LOINC** | Free with registration | Embed the ~12 vitals codes |
| **DEA schedules + drug codes** | Public domain | Embed. [DEA Orange Book](https://www.deadiversion.usdoj.gov/schedules/orangebook/c_cs_alpha.pdf) |
| **HL7 v2 tables, FHIR value sets, v3 ActCode** | CC0 / free | Embed |
| **Synthea** | Apache 2.0 | Reuse column names and generator freely |
| **MIMIC-IV / MIMIC-IV-ED** | ⚠️ **PhysioNet Credentialed licence — data is restricted** | **Use the public docs to copy the *schema*; do not download or ship the data.** Demo subsets (100 patients) are ODbL and unrestricted if we want a shape reference |
| **CPT (HCPCS Level I)** | ❌ AMA copyright | **Do not embed.** Use HCPCS Level II + revenue codes instead |
| **UB-04 / NUBC value sets** | ❌ AHA/NUBC copyright | Use the **CMS Blue Button** equivalents, which are free and carry the same values |
| **CARC / RARC** | X12 copyright; viewable free, machine-readable feed is paid | Embed the ~35 codes we need, cite X12 |
| **ATC** | ❌ "Copying and distribution for commercial purposes is not allowed" | **Avoid.** Use FDA `PHARM_CLASSES` (EPC) for therapeutic class |

**Bottom line:** every source can be anchored in a free, redistributable schema. The one thing to be careful about is MIMIC — we copy its *column names from public documentation*, which is fine, and we never touch the data.

---

## 3. The conformed dimension spine

These are generated **first**, are stable across the whole run, and are the join keys the platform's referential-integrity DQ checks will test. Get these right and everything else follows.

### 3.1 `dim_facility` — 7 facilities

Anchored on **CMS Hospital General Information** (Care Compare) attribute names. ⚠️ The exact CMS column names could not be fetched (JavaScript-rendered portal) — confirm against the CSV download before finalising.

| Field | Type | Notes |
|---|---|---|
| `facility_id` | VARCHAR(6) | CCN-shaped: 6 chars, state-prefixed (e.g. `330123`) |
| `facility_name` | VARCHAR(255) | |
| `facility_type` | VARCHAR(50) | `General Acute Care` / `Teaching` / `Regional` / `Community` / `Urgent Care` |
| `region` | VARCHAR(20) | `Northeast` / `West` / `Midwest` / `South` |
| `address`, `city`, `state`, `zip`, `county` | | |
| `emergency_services` | BOOLEAN | Urgent care = per client narrative; no inpatient beds |
| `licensed_beds`, `staffed_beds` | INT | See §3.2 for the distinction — this is a real definitional split and the client asks about capacity risk, so model both |
| `ownership` | VARCHAR(50) | |
| `open_date` | DATE | |

**Proposed network** (matches client request §1 — seven facilities across four regions):

| # | Facility | Type | Region | Licensed beds | Notes |
|---|---|---|---|---|---|
| 1 | Meridian General – Boston | General acute | Northeast | 420 | full ED |
| 2 | Meridian University Hospital | Teaching | Northeast | 610 | full ED, tertiary, highest acuity |
| 3 | Meridian General – Oakland | General acute | West | 380 | full ED |
| 4 | Meridian Regional – Phoenix | Regional | West | 250 | full ED |
| 5 | Meridian General – Chicago | General acute | Midwest | 340 | full ED |
| 6 | Meridian Community – Savannah | Community | South | 95 | small ED, no ICU tiers beyond MICU |
| 7 | Meridian Urgent Care – Austin | Urgent care | South | 0 | **no inpatient beds** — deliberately breaks naive bed-occupancy logic |

> Facility 7 having zero beds is intentional. It forces the Gold layer to handle divide-by-zero on occupancy and gives the DQ framework something real to catch.

### 3.2 `dim_unit` — departments / units

Bed capacity is managed unit-by-unit per the client request, so **unit is the grain that matters**. Structure anchored on the **HL7 v2 `PL` (Person Location)** composite data type — point of care / room / bed / facility / building / floor — and **FHIR R4 `Location.physicalType`** (`bu` building, `lvl` level, `wa` ward, `ro` room, `bd` bed).

| Field | Type | Notes |
|---|---|---|
| `unit_id` | VARCHAR(20) | |
| `facility_id` | VARCHAR(6) | FK → `dim_facility` |
| `unit_code` | VARCHAR(20) | HL7 point-of-care, e.g. `MICU`, `ED`, `5W` |
| `unit_name`, `unit_type` | | See table below |
| `building`, `floor` | | PL components |
| `licensed_beds` | INT | Beds the unit is licensed for |
| `staffed_beds` | INT | Beds it can actually staff today — **this is the denominator for real occupancy** |
| `blocked_beds` | INT | Out of service (maintenance, infection control) |
| `nurse_patient_ratio_target` | DECIMAL | From CA Title 22 §70217 — see §7.3 |
| `is_critical_care` | BOOLEAN | |

**Unit types and mandated ratios** — ratios are **verified** from [Cal. Code Regs. Tit. 22 § 70217](https://www.law.cornell.edu/regulations/california/Cal-Code-Regs-Tit-22-SS-70217), the only US mandated ratio law, which makes it the defensible source for a staffing-adequacy KPI:

| Unit type | Licensed nurse : patient | Typical beds (large / small) |
|---|---|---|
| Critical care (ICU/MICU/SICU/CVICU) | **1:2** | 24 / 8 |
| Neonatal ICU | 1:2 (critical care) | 20 / 0 |
| Post-anesthesia recovery (PACU) | **1:2** | 12 / 4 |
| Labor & delivery (active labor) | **1:2** | 10 / 4 |
| Step-down | **1:3** | 20 / 6 |
| Combined L/D/postpartum | **1:3** | — |
| Emergency department | **1:4** (min. two nurses present) | 40 / 10 |
| Telemetry | **1:4** | 32 / 8 |
| Specialty care (e.g. oncology) | **1:4** | 24 / 0 |
| Pediatrics | **1:4** | 20 / 6 |
| Antepartum / postpartum (couplets) | 1:4 | 16 / 6 |
| Medical/surgical | **1:5** | 60 / 30 |
| Postpartum (mothers only) | 1:6 | — |
| Psychiatric / behavioral health | **1:6** | 24 / 0 |
| Operating room | 1 circulating RN + 1 scrub per OR | 12 ORs / 3 |

> Bed counts are **`[OURS]`** — scaled by facility size. No open source publishes per-unit bed counts by hospital size; label them as modelling assumptions.

### 3.3 `dim_patient`

Anchored on **Synthea `patients.csv`** column names verbatim (Apache 2.0, and the only open schema with realistic PII-shaped demographics — which is exactly what we need to exercise the platform's PHI governance controls).

`Id` (UUID) · `BirthDate` · `DeathDate` · `SSN` · `Drivers` · `Passport` · `Prefix` · `First` · `Middle` · `Last` · `Suffix` · `Maiden` · `Marital` · `Race` · `Ethnicity` · `Gender` · `BirthPlace` · `Address` · `City` · `State` · `County` · `FIPS County Code` · `Zip` · `Lat` · `Lon` · `Healthcare_Expenses` · `Healthcare_Coverage` · `Income`

**`[OURS]` additions:**
- `mrn` — facility-scoped medical record number. **Deliberately not globally unique**: the same human appears with different MRNs at different facilities. This creates the real identity-matching problem the architecture diagram flags as an open question, and gives the Silver layer genuine work to do.
- `enterprise_patient_id` — the resolved cross-facility identity (the answer key; keep it out of Bronze).
- `phone`, `email` — more PII surface for masking/tokenization demos.

`Gender` uses FHIR **AdministrativeGender**: `male | female | other | unknown`.

⚠️ MIMIC's `race` / `marital_status` / `language` enumerations are **not published**; use Synthea's values (which follow US Census categories).

### 3.4 `dim_staff`

No dominant open standard for a hospital nurse roster exists. **Recommendation: do not base this on CMS PBJ** — PBJ is a *nursing-home* submission schema (its 34 job codes include "Mental Health Service Worker" and omit hospital-specific roles), and FHIR `Schedule`/`Slot` is really an appointment-booking model, not a shift roster. Instead: **borrow PBJ's column idiom** (verified from the [PBJ Employee Detail PUF documentation](https://download.cms.gov/pbj/pbj_employeedetailpuf_documentation_april_2022.pdf)) and build a proper roster.

PBJ verified columns, for reference: `PROVNUM`, `STATE`, `CY_QTR`, `WORKDATE`, `SYS_EMPLEE_ID`, `EMPLEE_JOB_CD_ID`, `EMPLEE_CTR` (1=Employee, 2=Contract), `WORK_HRS_NUM`, `INCOMPLETE`.

| Field | Type | Notes |
|---|---|---|
| `staff_id` | VARCHAR(20) | |
| `npi` | VARCHAR(10) | OMOP `PROVIDER.npi` — nulls for non-licensed roles |
| `first_name`, `last_name` | | PII |
| `job_code`, `job_title` | | `RN`, `LPN`, `CNA`, `MD`, `NP`, `PA`, `RPh`, `PharmTech`, `RT`, `Unit Clerk` |
| `credential` | VARCHAR(20) | |
| `primary_facility_id`, `primary_unit_id` | | FK |
| `employment_type` | INT | PBJ `EMPLEE_CTR` idiom: **1 = Employee, 2 = Contract/agency** |
| `fte` | DECIMAL(3,2) | |
| `hire_date`, `termination_date` | DATE | Termination drives realistic vacancy |
| `is_active` | BOOLEAN | |

⚠️ **Lead not yet run down:** one research thread flagged a **CDC NHSN "Nurse Staffing Hours" indicator** as a hospital-specific (rather than nursing-home) staffing schema. If it exists it is a better anchor than PBJ — worth 20 minutes before we finalise this table.

### 3.5 `dim_drug`

Grain = **RxNorm `RXCUI` at TTY=SCD** (ingredient + strength + dose form) — the level at which a hospital actually stocks and dispenses. Physical inventory joins on NDC-11; analytics rolls up on SCD/IN.

| Field | Source |
|---|---|
| `rxcui_scd` | RxNorm TTY=SCD — **dimension grain** |
| `rxcui_in` | RxNorm TTY=IN — ingredient, for shortage/class rollup |
| `ndc11` | FDA NDC, 11-digit 5-4-2 no dashes (the RxNorm join format) |
| `product_ndc` | FDA `PRODUCTNDC` |
| `proprietary_name`, `non_proprietary_name` | FDA `PROPRIETARYNAME`, `NONPROPRIETARYNAME` |
| `dosage_form_name`, `route_name` | FDA `DOSAGEFORMNAME`, `ROUTENAME` |
| `substance_name`, `strength_number`, `strength_unit` | FDA |
| `pharm_classes` | FDA `PHARM_CLASSES` — **use this as therapeutic class, not ATC** |
| `dea_schedule` | FDA `DEASCHEDULE` + DEA Orange Book |
| `dea_drug_code` | DEA CSCN |
| `labeler_name` | FDA `LABELERNAME` |
| `is_high_alert`, `is_shortage_prone` | **`[OURS]`** — drives stockout scenarios |

**Verified DEA schedules** (from the DEA Orange Book, with real DEA drug codes):
Fentanyl 9801/CII · Hydromorphone 9150/CII · Morphine 9300/CII · Oxycodone 9143/CII · Hydrocodone 9193/CII · Methadone 9250/CII · Codeine 9050/CII · Ketamine 7285/CIII · Buprenorphine 9064/CIII · Midazolam 2884/CIV · Lorazepam 2885/CIV · Diazepam 2765/CIV · Alprazolam 2882/CIV · Phenobarbital 2285/CIV · Tramadol 9752/CIV · Pregabalin 2782/CV

> **Propofol and dexmedetomidine are NOT DEA-scheduled.** Synthetic generators routinely get this wrong; getting it right is a cheap credibility win.
> **Gabapentin** is federally unscheduled but Schedule V in several states — flag if we model state rules.

Formulary should include ~30–40 drugs weighted toward high-volume hospital items (0.9% NaCl, acetaminophen, heparin, enoxaparin, ondansetron, pantoprazole, insulin, vancomycin, pip/tazo, cefazolin, ceftriaxone, furosemide, metoprolol, levetiracetam, norepinephrine, …) and should **deliberately include shortage-prone generic sterile injectables** (cefazolin, ondansetron injection, methylprednisolone) — 53% of new national shortages are in that category, so this is where stockout-risk analysis becomes non-trivial rather than decorative.

⚠️ Dose forms and classes for the non-controlled drugs must be validated by joining generic names against FDA `product.txt` — do not free-text them.

### 3.6 `dim_payer`

| Field | Notes |
|---|---|
| `payer_id`, `payer_name` | |
| `payer_type` | `Medicare` / `Medicare Advantage` / `Medicaid` / `Medicaid MC` / `Commercial` / `Self-Pay` / `Other` |
| `claim_filing_indicator_code` | X12 CLM03 |
| `prompt_pay_days` | Regulatory floor — see §7.4 |

### 3.7 `dim_date` / `dim_shift`

Standard date dimension plus a shift dimension for the staffing KPI: US hospitals run predominantly **12-hour shifts (07:00–19:00 day / 19:00–07:00 night)** with some 8-hour patterns. Shift is required by the client request ("by department and shift").

---

## 4. Batch source 1 — EHR encounters & admissions

**Landing:** secure cloud file storage → OneLake shortcut / Data Factory Copy
**Cadence:** daily (encounters closed yesterday), with a weekly full-refresh of dimensions
**Format:** CSV (UTF-8), one file per table per day: `ehr_encounters_YYYYMMDD.csv`

### 4.1 `encounters` — Synthea `encounters.csv` shape

`Id` · `Start` · `Stop` · `Patient` · `Organization` · `Provider` · `Payer` · `EncounterClass` · `Code` · `Description` · `Base_Encounter_Cost` · `Total_Claim_Cost` · `Payer_Coverage` · `ReasonCode` · `ReasonDescription`

`EncounterClass` — verified Synthea enum: `wellness`, `ambulatory`, `outpatient`, `inpatient`, `emergency`, `urgentcare`, `hospice`, `home`, `snf`, `virtual`.

**`[OURS]` additions:** `facility_id`, `unit_id`, plus coded companion columns so the extract has the dual code+display smell of a real interface feed:
- `encounter_class_code` — FHIR **v3 ActEncounterCode** (verified, 11 codes): `AMB` · `EMER` · `FLD` · `HH` · `IMP` · `ACUTE` · `NONAC` · `OBSENC` · `PRENC` · `SS` · `VR`
- `encounter_status` — FHIR **EncounterStatus**: `planned | arrived | triaged | in-progress | onleave | finished | cancelled` (+ `entered-in-error`, `unknown`). This gives the ED throughput state machine for free.
- `patient_class` — HL7 v2 table **0004** (verified, complete): `E` Emergency · `I` Inpatient · `O` Outpatient · `P` Preadmit · `R` Recurring · `B` Obstetrics · `C` Commercial Account · `N` Not Applicable · `U` Unknown

### 4.2 `admissions` — MIMIC-IV `hosp.admissions` shape (16 cols, verbatim)

`subject_id` · `hadm_id` · `admittime` · `dischtime` · `deathtime` · `admission_type` · `admit_provider_id` · `admission_location` · `discharge_location` · `insurance` · `language` · `marital_status` · `race` · `edregtime` · `edouttime` · `hospital_expire_flag`

**Verified value sets — use verbatim:**

- `admission_type` (9): `AMBULATORY OBSERVATION` · `DIRECT EMER.` · `DIRECT OBSERVATION` · `ELECTIVE` · `EU OBSERVATION` · `EW EMER.` · `OBSERVATION ADMIT` · `SURGICAL SAME DAY ADMISSION` · `URGENT`
- `admission_location`: `PHYSICIAN REFERRAL` · `WALK-IN/SELF REFERRAL` · `AMBULATORY SURGERY TRANSFER` · `INFORMATION NOT AVAILABLE` · `CLINIC REFERRAL` · `PROCEDURE SITE` · `PACU` · `TRANSFER FROM HOSPITAL` · `TRANSFER FROM SKILLED NURSING FACILITY` · `EMERGENCY ROOM` · `INTERNAL TRANSFER TO OR FROM PSYCH`
- `discharge_location`: `HOME` · `ACUTE HOSPITAL` · `SKILLED NURSING FACILITY` · `ASSISTED LIVING` · `HEALTHCARE FACILITY` · `HOME HEALTH CARE` · `AGAINST ADVICE` · `DIED` · `OTHER FACILITY` · `HOSPICE` · `REHAB` · `CHRONIC/LONG TERM ACUTE CARE` · `PSYCH FACILITY`

⚠️ MIMIC docs do not claim these location lists are exhaustive. Also add `admission_type_code` (HL7 table **0007**, verified complete: `A` Accident · `E` Emergency · `L` Labor and Delivery · `R` Routine · `N` Newborn · `U` Urgent · `C` Elective).

**`[OURS]`:** `facility_id`, `admit_decision_time` (see §4.4 — required for ED-2 and absent from every open schema).

### 4.3 `transfers` — MIMIC-IV `hosp.transfers` verbatim

`subject_id` · `hadm_id` · `transfer_id` · `eventtype` · `careunit` · `intime` · `outtime`

`eventtype` (verified): `ed` · `admit` · `transfer` · `discharge`. This one table models the whole stay and is what drives bed occupancy — see §7.

Optionally emit an **ADT message log** (`message_type`, `trigger_event`, `message_datetime`, `visit_number`) following the realistic sequence **A04** (ED registration) → **A06/A01** (admit) → **A02** (transfer) → **A03** (discharge), with occasional **A08** (demographic update), **A11**/**A13** (cancel admit / cancel discharge). Nothing makes a synthetic extract look like it came off an integration engine faster than this.

### 4.4 `ed_stays` — MIMIC-IV-ED `edstays` + `triage`

`subject_id` · `stay_id` · `hadm_id` (nullable — **this is the admitted-vs-discharged flag**) · `intime` · `outtime` · `gender` · `race` · `arrival_transport` · `disposition`
`triage`: `temperature` · `heartrate` · `resprate` · `o2sat` · `sbp` · `dbp` · `pain` · `acuity` · `chiefcomplaint`

`acuity` = **ESI 1–5**, where **1 = most acute**.

> ### ⚠️ Load-bearing finding
> **MIMIC-IV-ED has no triage timestamp and no admit-decision timestamp.** Its `triage` table is one row per stay with vitals and acuity but no `charttime`. Researchers proxy triage time with `min(vitalsign.charttime)`.
>
> That is a limitation to **fix, not copy**. Wait-time is the client's headline KPI, so we must emit explicitly:
> - `ed_arrival_time` (= `intime`)
> - **`triage_time`** `[OURS]`
> - **`admit_decision_time`** `[OURS]` — required for ED-2 boarding time
> - `ed_departure_time` (= `outtime`)
>
> Without these two additions the platform physically cannot answer "where are wait times longest".

**Real measure definitions our KPIs must match** (this is what turns "we made a wait-time chart" into "we implemented OP-18"):

| Measure | Definition | Generator implication |
|---|---|---|
| **OP-18** (CMS32v8) | Median minutes, ED arrival → ED departure, for patients **discharged** from ED. Excludes patients who **expired**, and ED visits **followed within 1 hour by inpatient admission at the same facility** | Need expired flag + the 1-hour join rule |
| **ED-1** (CMS55v6) | Median minutes, ED arrival → ED departure, for patients **admitted**. Population: inpatient encounters with **LOS ≤ 120 days**, preceded **within 1 hour** by an ED visit at the same facility. Excludes transfer-in from another hospital **within 6 hours** before ED start. **Stratified** psych vs non-psych principal diagnosis | Need transfer-in flag with 6h lookback + principal dx |
| **ED-2** (CMS111v11) | Median minutes, **admit decision → ED departure** (boarding time). Requires documented decision to admit before ED departure, within 1 hour of admission | **Requires `admit_decision_time`** |

### 4.5 `diagnoses` — MIMIC `diagnoses_icd` shape

`subject_id` · `hadm_id` · `seq_num` · `icd_code` · `icd_version` · `icd_title`

`seq_num = 1` is the **principal diagnosis** — nearly every KPI depends on this.

**Sample from real high-volume codes**, weighted by **AHRQ HCUP Statistical Brief #277** actual national stay counts, which is what makes the diagnosis mix pass a clinician's sniff test:

| Weight anchor (HCUP 2018) | Stays | % of all stays |
|---|---|---|
| Septicemia | 2,218,800 | 8.0% |
| Heart failure | 1,135,900 | 4.1% |
| Osteoarthritis | 1,128,100 | 4.1% |
| Pneumonia (excl. TB) | 740,700 | 2.7% |
| Diabetes with complication | 678,600 | 2.4% |
| Acute MI | 658,600 | 2.4% |
| Cardiac dysrhythmias | 620,000 | — |
| COPD / bronchiectasis | 569,600 | — |

**Verified ICD-10-CM codes to seed with** (descriptions confirmed): `A41.9` Sepsis unspecified organism · `A41.51` Sepsis due to E. coli · `R65.20` Severe sepsis without septic shock · `R65.21` Severe sepsis with septic shock · `I50.9` Heart failure unspecified · `I50.23` Acute on chronic systolic HF · `I50.33` Acute on chronic diastolic HF · `J44.1` COPD with acute exacerbation · `J44.0` COPD with acute lower respiratory infection · `J18.9` Pneumonia unspecified organism · `J15.9` Unspecified bacterial pneumonia · `I21.4` NSTEMI · `I21.3` STEMI unspecified site · `I21.A1` MI type 2 · `E11.65` T2DM with hyperglycemia · `E11.22` T2DM with diabetic CKD · `N17.9` Acute kidney failure unspecified · `N18.6` ESRD · `I63.9` Cerebral infarction unspecified · `N39.0` UTI site not specified · `I48.91` Unspecified atrial fibrillation · `J96.01` Acute respiratory failure with hypoxia · `K92.2` GI hemorrhage unspecified · `A04.72` C. diff enterocolitis not recurrent · `E86.0` Dehydration · `I10` Essential hypertension · `R07.9` Chest pain unspecified (**high-volume ED**) · `R10.9` Unspecified abdominal pain (**high-volume ED**)

The four HRRP condition cohorts (**AMI, COPD, heart failure, pneumonia**) are all represented — deliberately, so the readmission KPI has real cohorts.

⚠️ **Do not hard-code MS-DRG numbers from memory.** Assign DRGs by parsing the CMS v43/v44 Definitions Manual.

### 4.6 Readmission logic — CMS HRRP, verified

The readmission KPI must implement the real definition, not "any admission within 30 days":

- **Index admission requires:** discharged **alive** (`hospital_expire_flag = 0`), `discharge_location <> 'AGAINST ADVICE'`, not a primary psychiatric diagnosis, not rehabilitation, not medical treatment of cancer, not a PPS-exempt cancer hospital.
- **Outcome:** first **unplanned** readmission within 30 days of `dischtime`, at the **same or any other** acute care hospital, **regardless of principal diagnosis**.
- **Counting:** binary per index admission — if a patient has more than one unplanned admission in 30 days, **count only one**.
- **Planned-readmission rule:** if the *first* post-discharge readmission is planned, no subsequent unplanned readmission counts for that index.
- **Transfers are not readmissions** — model them as `discharge_location = 'ACUTE HOSPITAL'` so they can be filtered.
- HRRP's own six measures cover **AMI, COPD, HF, pneumonia, CABG, elective THA/TKA**.

Also enforce inpatient **LOS ≤ 120 days** for the bulk of records so the ED measure populations behave.

---

## 5. Batch source 2 — Billing & claims

**Landing:** secure cloud file storage
**Cadence:** daily submissions extract + daily remittance extract (they arrive on different clocks — that gap *is* the AR metric)

> **Recommendation: base this on a flattened X12 837I + 835 pair.** Not DE-SynPUF, not Synthea.
>
> Reasoning: **CMS DE-SynPUF** looks like the obvious choice (it's real, CMS-published, synthetic, no DUA) but it is **100% Medicare FFS with no payer dimension, no submission date, no remittance date, and no denial or adjustment fields**. You cannot compute denial rate, days-in-AR, or revenue-at-risk-by-payer from it. **Synthea's** claims export has an excellent AR ledger model but **no denial concept at all** (`Status` is only `BILLED`/`CLOSED`). Only the 837/835 pair carries every field the three financial KPIs need.

Use the [Healthcare Data Insight flattened 837I dictionary](https://datainsight.health/docs/datadict/837i-claim/) for the billing side and the [CMS 835 flat file spec](https://www.cms.gov/medicare/billing/electronicbillingeditrans/downloads/835-flatfile.pdf) for the remit side.

### 5.1 `claim_header`

`patient_control_number` (CLM01) · `total_charge_amount` (CLM02) · `claim_filing_indicator_code` (CLM03) · `type_of_bill` (CLM05-1 + CLM05-3) · `payer_id` / `payer_name` · `statement_date_from` / `statement_date_to` (DTP 434) · `admission_date_and_hour` (DTP 435) · `discharge_time` (DTP 096) · `admission_type_code` (CL101) · `admission_source_code` (CL102) · `patient_status_code` (CL103) · `drg_code` · `principal_diagnosis` (HI **ABK**) · `admitting_diagnosis` (HI **ABJ**) · `other_diagnoses` (HI **ABF**) · `attending_provider_npi` · `prior_authorization_number` (REF G1) · `payer_claim_control_number` (REF F8) · `medical_record_number` (REF EA)

**`[OURS]` but essential:** `submission_date` and `remit_date` — the AR clock. Neither X12 transaction carries both, and no open schema does.

**Verified free CMS value sets** (use these instead of the NUBC-copyright originals):
- **Type of Bill digit 1** (`CLM_FAC_TYPE_CD`): 1 Hospital · 2 SNF · 3 HHA · 4 Religious non-medical · 6 Intermediate care · 7 Clinic/renal dialysis · 8 ASC/other special facility
- **digit 2** (`CLM_SRVC_CLSFCTN_TYPE_CD`): 1 Inpatient · 2 Inpatient Part B · 3 Outpatient · 4 Other Part B · 7 Subacute inpatient · 8 Swing bed
- **digit 3** (`CLM_FREQ_CD`): 1 Admit-thru-discharge · 2 Interim first · 3 Interim continuing · 4 Interim last · **7 Replacement of prior claim** · **8 Void/cancel prior claim** · 9 Final
  → realistic TOB strings: `0111`, `0121`, `0131`, `0141`, `0117`, `0118`, `0851`
- **Point of origin** (`CLM_SRC_IP_ADMSN_CD`): 1 Physician referral · 2 Clinic referral · 4 Transfer from hospital · 5 Transfer from SNF/ICF · 6 Transfer from other facility · **7 Emergency room** · 8 Court/law enforcement · 9 Info not available · D Transfer from IP same facility · E Transfer from ASC · F Transfer from hospice
- **Patient discharge status** (FL17): 01 Home/self-care · 02 Short-term general hospital · 03 SNF Medicare-certified · 04 Custodial/supportive care · 05 Cancer center/children's hospital · 06 Home health · **07 Left AMA** · 09 Admitted as inpatient to this hospital · **20 Expired** · 30 Still a patient · 41 Expired in medical facility · 50 Hospice home · 51 Hospice medical facility · 62 IRF · 63 LTCH · 65 Psychiatric hospital · 66 CAH

### 5.2 `claim_line`

`line_control_number` (REF 6R) · **`revenue_code` (SV201)** · `procedure_code` + qualifier (SV202 — use **HCPCS Level II**, never CPT) · `modifier_1..4` · `line_charge_amount` (SV203) · `unit_type` (SV204) · `unit_count` (SV205) · `non_covered_amount` (SV207) · `service_date_from` / `_to` · `drug_ndc` (LIN03) · `drug_quantity` (CTP04)

Revenue code families (free CMS/Noridian list): `011X` private room · `012X` semi-private · `020X` ICU · `021X` CCU · `025X` pharmacy · `030X`–`031X` lab · `032X` radiology diagnostic · `036X` operating room · **`045X` emergency room** · `080X` inpatient renal dialysis · `090X` behavioral health

### 5.3 `remit` + `remit_adjustment`

`remit`: `claim_status_code` (**CLP02**) · `total_claim_charge_amount` (CLP03) · `claim_payment_amount` (CLP04) · `patient_responsibility_amount` (CLP05) · `payer_claim_control_number` (CLP07) · `drg_code` (CLP11) · `drg_weight` (CLP12) · `check_eft_trace_number` (TRN02) · `payment_method_code` (BPR04) · `check_date`

**`CLP02` Claim Status Code — verified X12 element 1029:** 1 Processed as Primary · 2 Processed as Secondary · 3 Processed as Tertiary · **4 Denied** · 5 Pended · 19/20/21 Processed & forwarded · **22 Reversal of Previous Payment** · 23 Not our claim, forwarded · 25 Predetermination pricing only

> **`CLP02 = 4` is the denial flag. `22` is the takeback/reversal flag.** Note that CLP02 exists **only in the 835** — the 837 has no adjudication status. (This was a category error in my original brief to the research agent; correcting it here.)

`remit_adjustment` (mirrors CAS02–CAS19, six repeating reason/amount/quantity trios): `group_code` · `reason_code` (CARC) · `amount` · `quantity` · `remark_code` (RARC via LQ)

**Group codes, verified:** **CO** Contractual Obligation (provider write-off) · **PR** Patient Responsibility · **OA** Other Adjustment · **PI** Payer Initiated Reductions (*not used by Medicare*). Group code CR has been deleted.

**CARCs to embed** (X12, verified descriptions, last modified 03/01/2025 — 139 codes exist; these are the ones that matter):

*True denials:* `CO-16` Claim/service lacks information or has submission/billing error(s) — **must accompany a RARC** · `CO-50` Non-covered, not deemed medical necessity · `CO-197` Precertification/authorization absent · `CO-198` Precertification exceeded · `CO-29` The time limit for filing has expired · `CO-96` Non-covered charge(s) · `CO-109` Not covered by this payer/contractor · `CO-11` Diagnosis inconsistent with procedure · `CO-181` Procedure code invalid on date of service · `CO-146` Diagnosis invalid for date(s) of service · `CO-252` Attachment/documentation required · `CO-227` Requested information not provided · `CO-204` Not covered under patient's current benefit plan · `CO-31` Patient cannot be identified as our insured · `CO-27` Expenses incurred after coverage terminated · `CO-39` Services denied at time authorization was requested

*Contractual write-offs (NOT denials — a common analytical error):* `CO-45` Charge exceeds fee schedule/maximum allowable · `CO-97` Benefit included in payment for another service · `CO-24` Covered under capitation agreement

*Patient responsibility:* `PR-1` Deductible Amount · `PR-2` Coinsurance Amount · `PR-3` Co-payment Amount

*Other:* `OA-18` Exact duplicate claim/service (**X12 mandates group OA here**) · `OA-23` Impact of prior payer(s) adjudication

*Verified RARCs:* `MA130` unprocessable, no appeal rights · `MA04` secondary payment needs primary info · `M127` missing patient medical record

⚠️ `CARC 15` was **deactivated 05/01/2018** — do not generate it. ⚠️ Do not generate **PLB03-1** provider-adjustment reason codes; that list could not be verified.

---

## 6. Batch sources 3–5

### 6.1 Pharmacy inventory

**Landing:** secure cloud file storage · **Cadence:** daily snapshot + weekly reconciliation

**Recommendation: model on FHIR R5 `InventoryReport` *semantics*, flattened to a star schema — do not emit literal FHIR JSON.** `InventoryReport.countType = snapshot | difference` is exactly our daily-snapshot-vs-delta axis, and `inventoryListing.location` + `countingDateTime` + `item.quantity` is the right grain. But R5 InventoryReport/InventoryItem are **Maturity Level 0 / Draft**, `InventoryItem.instance` is only `0..1` (so FHIR literally cannot express one item with many lots), and there are **no on-hand / par / reorder / consumption fields at all**. So FHIR gives us the vocabulary; the operational columns are ours.

`fact_inventory_snapshot`, grain = (`snapshot_date`, `location_id`, `ndc11`, `lot_number`):

| Field | Source |
|---|---|
| `snapshot_date`, `counting_datetime` | FHIR `reportedDateTime` / `countingDateTime` |
| `count_type` | FHIR `countType`: `snapshot` \| `difference` |
| `facility_id`, `location_id` | FK — pharmacy, satellite pharmacy, or ADC/Pyxis cabinet on a unit |
| `ndc11`, `gtin14`, `rxcui_scd` | FDA / GS1 / RxNorm |
| `lot_number` | GS1 **AI 10** — VARCHAR ≤20, **preserve leading zeros** |
| `expiration_date` | GS1 **AI 17** (YYMMDD on barcode payloads) |
| `qty_on_hand`, `base_unit`, `qty_on_order` | |
| `par_level`, `reorder_point`, `safety_stock` | **`[OURS]`** — no open standard defines these |
| `avg_daily_usage_30d`, `days_on_hand`, `abc_class` | **`[OURS]`** |
| `is_controlled`, `dea_schedule` | DEA |
| `shortage_status` | openFDA: `Currently in Shortage` \| `Resolved` \| `Discontinuation` \| `Available` |
| `unit_cost`, `last_count_variance` | |

**Shortage realism** — openFDA drug shortages `shortage_reason` verbatim categories: *complying with GMP · regulatory delay · shortage of active ingredient · shortage of inactive ingredient · discontinuation of manufacture · delay in shipping · demand increase*.

Published national context: **323 active shortages** (all-time high, early 2024); **95 new** in 2024; **50% persist ≥2 years**; **53% of new shortages are generic sterile injectables**; ~60% have "unknown" attributed cause.

⚠️ **No published item-level stockout probability exists.** National shortage counts are *not* an SKU-level stockout rate. Whatever we choose is a modelling assumption and must be labelled as one. Same for the inventory benchmarks (8–12 turns/yr, 30–45 days on hand, ABC 10–20%/70–80% split) — these came from a consulting blog only, not a peer-reviewed or ASHP source.

⚠️ **Verify the live FDA `product.txt` header row before writing the parser.** FDA's docs render CamelCase (`StrengthNumber`) but the actual TSV appears to be UPPERCASE, and the historical names were `ACTIVE_NUMERATOR_STRENGTH` / `ACTIVE_INGRED_UNIT`.

📅 **Forward-looking note:** FDA's final rule (published 5 Mar 2026) moves NDC to a uniform **12-digit 6-4-2** format effective **7 Mar 2033**. Not our problem for this project, but worth a sentence in the documentation deliverable.

### 6.2 Bed capacity snapshots

**Landing:** secure cloud file storage · **Cadence:** the client asks for "near-real-time" bed capacity, so emit **hourly** snapshots batched into a daily file, plus a daily census roll-up

**Anchor: the CDC NHSN Hospital Respiratory Data (HRD) weekly reporting form** — verified fields, and the only currently-live real US bed-capacity reporting schema. Note it is **weekly**, collected as of "**Wednesday of the reporting week**" for capacity/occupancy and Sunday–Saturday for new admissions. Its verified field families:

*Staffed bed capacity:* All hospital inpatient beds · All adult inpatient beds · All pediatric inpatient beds
*Inpatient occupancy:* All hospital / adult / pediatric inpatient occupancy
*ICU beds:* All ICU beds · Adult ICU beds · Pediatric ICU beds
*ICU occupancy:* All / adult / pediatric ICU bed occupancy

> **Important:** NHSN's grain is **facility × week**, which is far too coarse for the client's "which unit, and when" question. So: adopt NHSN's **field naming and the staffed-bed concept** (it is the defensible national definition), but generate at **facility × unit × hour**, and emit an NHSN-shaped facility-week roll-up as a *second* file. That roll-up is a genuinely good DQ test — the hourly detail must reconcile to it.

`fact_bed_snapshot`, grain = (`snapshot_datetime`, `facility_id`, `unit_id`):

`snapshot_datetime` · `facility_id` · `unit_id` · `licensed_beds` · **`staffed_beds`** · `blocked_beds` · `occupied_beds` · `available_beds` · `pending_admissions` (ED boarding, waiting for this unit) · `pending_discharges` · `occupancy_rate` (= occupied / staffed) · `is_at_capacity` (≥85%) · `census_at_midnight`

**Bed designation definitions — model all three separately, they are genuinely different:**
- **Licensed** beds — what the state licence permits
- **Staffed** beds — what can actually be staffed today (**this is the real denominator**; NHSN reports staffed)
- **Blocked / out-of-service** beds — unavailable for maintenance or infection control

**Occupancy realism, verified** — Leuchter et al., *JAMA Network Open* (UCLA, Feb 2025):
- Pre-pandemic decade mean US hospital occupancy: **~64%**
- Post-pandemic mean: **~75%** (11 points higher)
- Projected to reach **85% by 2032** for adult beds
- **85% is the widely-used bed-shortage threshold** — associated with long ED waits, medication errors and other adverse events
- CDC: when national **ICU occupancy reaches 75%**, there are ~12,000 excess deaths two weeks later

→ Generate a network mean around **75%**, with the teaching hospital and ICUs running hotter (85–95%, i.e. genuinely in the risk zone), the community hospital cooler (60–70%), plus diurnal (afternoon peak), day-of-week (midweek peak) and seasonal (winter respiratory) patterns. This gives the operational dashboard something real to flag.

⚠️ Diurnal/weekly/seasonal amplitude figures were not verified from a citable source — mark as modelling assumptions.

### 6.3 Staff schedules — **SharePoint document library**

**Landing:** SharePoint Online → Dataflow Gen2 · **Cadence:** weekly (published roster) + daily (actuals with call-outs)
**Format:** **`.xlsx`, one workbook per facility per week**, because that is what a real internally-managed operational document looks like — and it deliberately exercises a different ingestion path from the CSV cloud drops.

This is where realism should include realistic *mess*, since it is a human-maintained spreadsheet:
- Merged header rows and a title row above the real header
- A facility name in a cell rather than a column
- Dates as text in mixed formats
- Trailing blank rows, and a "Notes" column with free text
- Occasional duplicated staff rows

`staff_schedule` (per row = one staff member × one shift):

`facility_id` · `unit_id` · `work_date` · `shift_code` (`D` 07:00–19:00 / `N` 19:00–07:00 / `E` 8-hour evening) · `shift_start`, `shift_end` · `staff_id` · `job_code` · `scheduled_hours` · **`actual_hours`** · `employment_type` (1 Employee / 2 Contract — PBJ idiom) · `is_overtime` · `is_call_out` · `is_float` (pulled from another unit) · `notes`

**`[OURS]` for the staffing KPI:** `patient_census_at_shift` and derived `actual_nurse_patient_ratio`, compared to `dim_unit.nurse_patient_ratio_target`. This is the whole "are we adequately staffed relative to patient load, by department and shift" question — and note it can only be answered because bed occupancy and the roster share `unit_id` and `work_date`. That join is the single most important referential-integrity check in the platform.

Realism knobs: build understaffing in **deliberately and non-uniformly** — night shifts and the med/surg units at the community hospital chronically under target, weekends worse than weekdays, with agency/contract fill rising to compensate. If understaffing is uniform random noise, the operational dashboard has nothing to discover.

⚠️ Real-world HPPD benchmarks by unit type, nurse vacancy/turnover rates, and agency-usage rates were not verified (research cut short) — either source them or label the numbers as assumptions.

---

## 7. Streaming sources

**Transport:** Kafka-compatible. ⚠️ **Decide this now:** the architecture diagram says Azure Event Hubs (Kafka endpoint); the work plan task 2.4 says Upstash Kafka. This changes my side of the work materially — Event Hubs gives durable replay for free via Event Hubs Capture → ADLS, which the diagram's replay path depends on; Upstash does not have an equivalent, so I'd need to write the raw events to storage myself.

**Envelope** (both topics — this is the shape the Eventstream and the Bronze landing will parse):

```json
{
  "event_id": "uuid",
  "event_type": "vitals.reading | prescription.issued",
  "event_time": "2026-08-12T14:32:05.123Z",
  "ingest_time": "2026-08-12T14:32:06.001Z",
  "source_system": "PHILIPS_IX_MONITOR | PHARMACY_OMS",
  "facility_id": "330123",
  "schema_version": "1.0",
  "payload": { }
}
```

`event_id` must be a **stable, deterministic ID** so at-least-once delivery is de-duplicable — the diagram's "stable event IDs" control depends on the generator actually providing them. `event_time` vs `ingest_time` separation is what makes late-arriving-data handling testable.

### 7.1 Patient vitals stream

**Anchor: eICU `vitalPeriodic`** — verified columns and, critically, verified cadence: *"Data are generally interfaced as 1 minute averages, and archived into the vitalPeriodic table as 5 minute median values."* That 1-min-interface / 5-min-archive pattern is real monitor behaviour and exactly what we should reproduce.

eICU `vitalPeriodic` verified columns: `patientunitstayid` · `vitalperiodicid` · `observationyear` · `observationtime24` · `observationtime` · `temperature` · `sao2` · `heartrate` · `respiration` · `cvp` · `etco2` · `systemicsystolic` · `systemicdiastolic` · `systemicmean` · `pasystolic` · `padiastolic` · `pamean` · `st1` · `st2` · `st3` · `icp`

MIMIC-IV `icu.chartevents` verified columns (the EAV alternative): `subject_id` · `hadm_id` · `stay_id` · `caregiver_id` · `charttime` · `storetime` · `itemid` · `value` · `valuenum` · `valueuom` · **`warning`**

> Recommendation: emit **one event per parameter reading** (tall/EAV, like `chartevents`) rather than a wide row. It handles missing parameters naturally, matches how monitors actually emit, and gives the Silver layer real pivoting work. Keep `warning` — it's a genuine device-side validity flag.

**Payload:**

`patient_id` · `encounter_id` · `facility_id` · `unit_id` · `bed_id` · `device_id` · `charttime` · `loinc_code` · `parameter_name` · `value_num` · `value_uom` · `warning` (device validity flag) · `alarm_state` (`none` / `advisory` / `warning` / `crisis`) · `is_artifact`

**LOINC codes — verified from the FHIR R4 Vital Signs profile** (use these exactly; this is the single most-often-fudged detail in synthetic health data):

| Parameter | LOINC | Display | UCUM |
|---|---|---|---|
| Vital signs panel | **85353-1** | Vital signs, weight, height, head circumference, oxygen saturation and BMI panel | — |
| Blood pressure panel | **85354-9** | Blood pressure panel with all children optional | — |
| Systolic BP | **8480-6** | Systolic arterial blood pressure | `mm[Hg]` |
| Diastolic BP | **8462-4** | Diastolic arterial blood pressure | `mm[Hg]` |
| Heart rate | **8867-4** | Heart rate | `/min` |
| Respiratory rate | **9279-1** | Respiratory Rate | `/min` |
| Body temperature | **8310-5** | Body temperature | `Cel` |
| Oxygen saturation | **2708-6** | Oxygen saturation in Arterial blood | `%` |
| Body weight | **29463-7** | Body weight | `kg` |
| Body height | **8302-2** | Body height | `cm` |
| BMI | **39156-5** | Body mass index (BMI) [Ratio] | `kg/m2` |
| Head circumference | **9843-4** | Head Occipital-frontal circumference | `cm` |

> **On `2708-6` vs `59408-5`:** the FHIR R4 vital signs profile uses **2708-6** ("Oxygen saturation in Arterial blood"). `59408-5` is the pulse-oximetry-specific code and appears in the US Core / Vitals IG. **Use 2708-6** for profile conformance; optionally carry 59408-5 as a device-specific translation.

**Critical-alert logic: NEWS2** (Royal College of Physicians). Verified thresholds: **aggregate ≥5** triggers urgent clinical review; **≥7** triggers emergency response; **a single parameter scoring 3** prompts clinician review.

Canonical NEWS2 bands (score 0 unless stated):

| Parameter | 3 | 2 | 1 | 0 | 1 | 2 | 3 |
|---|---|---|---|---|---|---|---|
| Respiration (/min) | ≤8 | — | 9–11 | 12–20 | — | 21–24 | ≥25 |
| SpO2 Scale 1 (%) | ≤91 | 92–93 | 94–95 | ≥96 | — | — | — |
| Air or oxygen | — | O₂ | — | Air | — | — | — |
| Systolic BP (mmHg) | ≤90 | 91–100 | 101–110 | 111–219 | — | — | ≥220 |
| Pulse (/min) | ≤40 | — | 41–50 | 51–90 | 91–110 | 111–130 | ≥131 |
| Consciousness (ACVPU) | C/V/P/U | — | — | Alert | — | — | — |
| Temperature (°C) | ≤35.0 | — | 35.1–36.0 | 36.1–38.0 | 38.1–39.0 | ≥39.1 | — |

⚠️ **Verify the systolic BP and SpO2-Scale-2 rows against the official RCP NEWS2 chart before coding.** The PDF I extracted from gave systolic bands (111–169 = 0, ≥200 = 3) that disagree with the canonical chart above — it appears to be a locally-modified version. The RCP chart is the authority.

**Alert-rate realism:** ⚠️ the alarm-fatigue literature (alarms per bed per day, % clinically actionable) was **not retrieved** — the research agent was cut short. Worth finding, because "unusually high rate of critical alerts" is an explicit client requirement and a defensible per-bed alarm rate makes that dashboard tile credible.

**Volume estimate:** ~200 monitored beds network-wide × 7 parameters × 1 reading/min ≈ **1,400 events/min ≈ 2M events/day**. Worth sanity-checking against Fabric capacity sizing (which the architecture diagram already lists as an open question) before we turn the taps on.

### 7.2 Prescription issuance stream

**Anchor: MIMIC-IV `hosp.pharmacy` + `hosp.prescriptions`**, with FHIR `MedicationRequest` supplying the status/intent vocabulary.

> **Why not NCPDP SCRIPT?** It is the correct e-prescribing standard, but the spec is **paywalled** and the NewRx-specific element names (`WrittenDate`, `NumberOfRefills`, `Sig`) are **not publicly documented**. We cannot be schema-faithful to it without buying it, and inventing those names would violate rule #1. MIMIC is real, fully documented publicly, models the order lifecycle, and carries `ndc` natively so it joins straight to inventory.

**Payload — MIMIC field names verbatim:**

`subject_id` (patient) · `hadm_id` · `pharmacy_id` · `poe_id` · `poe_seq` · `order_provider_id` · `drug` (free text, as in real EHRs) · `drug_type` (`MAIN` / `BASE` / `ADDITIVE`) · `formulary_drug_cd` · `gsn` · **`ndc`** · `prod_strength` · `dose_val_rx` · `dose_unit_rx` · `form_val_disp` · `form_unit_disp` · `doses_per_24_hrs` · `route` · `frequency` · `proc_type` (*IV Piggyback, Non-formulary, Unit Dose*) · `status` (`active` / `inactive` / `discontinued`) · `entertime` · `verifiedtime` · `starttime` · `stoptime` · `dispensation` · `fill_quantity`
**`[OURS]`:** `facility_id`, `unit_id`, `rxcui_scd`, `event_type` (`ordered` / `verified` / `dispensed` / `discontinued`)

FHIR `MedicationRequest` vocabulary if we want a standards-mapped view: `status` = `active | on-hold | cancelled | completed | entered-in-error | stopped | draft | unknown`; `intent` = `proposal | plan | order | original-order | reflex-order | filler-order | instance-order | option`; `priority` = `routine | urgent | asap | stat`.

> **Deliberate realism worth keeping:** MIMIC stores `dose_val_rx`, `fill_quantity` and `infusion_rate` as **VARCHAR, not numeric**. That is authentic EHR messiness and it gives the Silver layer a real type-conformance job. Keep it.

**Optional second topic** modelled on `emar` / `emar_detail` (medication administration): carries `barcode_type` and `reason_for_no_barcode`, i.e. genuine BCMA scan-failure signal, and it is administration — not ordering — that actually depletes inventory. Worth doing if time allows, since it closes the loop between the prescription stream and the inventory extract.

---

## 8. Deliberate imperfections — the DQ framework's test material

The client requires a documented DQ framework with **visible pass/fail results**, and work-plan task 5.2 requires deliberately breaking something. A perfectly clean generator makes both undemonstrable. So the generator needs a **defect injection layer with a configurable rate and a seed**, and — critically — **an answer key** recording every defect injected, so we can prove the DQ framework caught them rather than asserting it.

| Defect class | Injection | Which DQ check should catch it |
|---|---|---|
| Completeness | Null required fields (missing `dischtime`, missing `acuity`) | Completeness |
| Validity | Invalid ICD-10 code; `admission_type` outside the value set; negative `qty_on_hand` | Validity / domain |
| Type conformance | `"12.5 mg"` in a numeric dose field; date as `08/13/2026` vs ISO | Type conformance |
| Uniqueness | Duplicate `hadm_id`; duplicate Kafka `event_id` | Uniqueness / dedupe |
| Referential integrity | Claim with no matching encounter; prescription for a discharged patient; roster row for a terminated staff member; `unit_id` not in `dim_unit` | **Cross-source RI** |
| Temporal | `dischtime` before `admittime`; ED departure before arrival; negative LOS | Temporal validity |
| Freshness | Skip a daily file entirely; deliver one 6 hours late | Freshness / SLA |
| Stream gaps | 15-minute silence on the vitals topic; out-of-order `event_time`; a burst of late events | Late-data handling, watermarks |
| Cross-source reconciliation | Hourly bed snapshots that don't sum to the NHSN-shaped weekly roll-up | Reconciliation |
| Identity | Same human with different MRNs across facilities | Identity matching |

Target ~**0.5–2% defect rate** in normal runs, with a `--chaos` mode for the Phase 5 test.

---

## 9. Open decisions I need from the team

1. **Kafka: Event Hubs or Upstash?** Changes whether I emit raw events to storage myself for replay. (Diagram says one; work plan says the other.)
2. **Cloud file storage: S3 or ADLS/OneLake?** Diagram says "provider to confirm"; work plan task 2.2 says AWS S3. Changes the delivery mechanism, not the schema.
3. **History depth and volume.** My recommendation: **18 months of history** (enough for seasonality, YoY trend, and a full 30-day readmission window with room to spare) at roughly **1.5–2M encounters** network-wide, ~5,000 staff, ~2M vitals events/day. Confirm this fits the Fabric capacity before I generate at full scale.
4. **PHI realism.** I propose **realistic-looking but fully synthetic** PII (Faker-generated names/addresses/SSN-shaped values) so the RLS/OLS/masking/tokenization controls have something real to protect. Confirm the team is comfortable with SSN-shaped values in Bronze, since that is precisely what the governance layer is meant to demonstrate.
5. **Do we want the HL7 v2 ADT message log and the OMOP export?** Both are cheap for me to add and both are strong "we understand real healthcare data" signals for the final presentation.

---

## 10. Verify-before-coding checklist

Carried forward from research; each one is a real risk of shipping a wrong value.

- [ ] FDA `product.txt` live header row — CamelCase vs UPPERCASE, `StrengthNumber` vs `ACTIVE_NUMERATOR_STRENGTH`
- [ ] RCP NEWS2 chart — systolic BP bands and SpO2 Scale 2 (my extracted source disagrees with canonical)
- [ ] CMS Hospital General Information exact column names (portal is JS-rendered; download the CSV)
- [ ] MS-DRG numbers — parse the CMS v43/v44 Definitions Manual, never hard-code from memory
- [ ] HL7 v2 tables 0018, 0023, 0069, 0112, 0113 value lists — pull from `terminology.hl7.org` at build time
- [ ] FHIR Identifier Type Codes (v2-0203) — fetch the CodeSystem JSON rather than typing `MR`/`SS`/`PI` from memory
- [ ] Full CARC text for codes 23, 24, 49, 51, 55, 58, 59 (came back truncated)
- [ ] CMS PBJ full 34 job-code list (only summarised)
- [ ] Whether a CDC NHSN **hospital** nurse-staffing-hours schema exists (better anchor than PBJ)
- [ ] HPPD benchmarks by unit type; nurse vacancy/turnover/agency rates
- [ ] Alarm-fatigue literature: alarms per bed per day, % clinically actionable
- [ ] Item-level stockout probability — likely doesn't exist; label as assumption
- [ ] ClaimResponse.outcome canonical value-set URL (R4 `remittance-outcome` vs R5 `claim-outcome`)
- [ ] DE-SynPUF codebook per-variable value labels (PDF returned HTTP 503)

---

## Sources

**EHR / encounters:** [Synthea CSV data dictionary](https://github.com/synthetichealth/synthea/wiki/CSV-File-Data-Dictionary) · [MIMIC-IV hosp module](https://mimic.mit.edu/docs/iv/modules/hosp/) · [MIMIC-IV-ED](https://mimic.mit.edu/docs/iv/modules/ed/) · [MIMIC-IV demo (ODbL)](https://physionet.org/content/mimic-iv-demo/2.2/) · [MIMIC-IV chartevents](https://mimic.mit.edu/docs/iv/modules/icu/chartevents/) · [OMOP CDM v5.4](https://ohdsi.github.io/CommonDataModel/cdm54.html) · [FHIR R4 Encounter](https://hl7.org/fhir/R4/encounter.html) · [FHIR R4 Patient](https://hl7.org/fhir/R4/patient.html) · [v3 ActEncounterCode](https://terminology.hl7.org/ValueSet-v3-ActEncounterCode.html) · [HL7 v2 PV1](https://www.hl7.eu/refactored/segPV1.html) · [HL7 v2.4 Ch.3 ADT](https://www.hl7.eu/HL7v2x/v24/std24/ch03.htm) · [CMS ICD-10](https://www.cms.gov/medicare/coding-billing/icd-10-codes) · [CMS MS-DRG](https://www.cms.gov/medicare/payment/prospective-payment-systems/acute-inpatient-pps/ms-drg-classifications-and-software) · [HCUP Statistical Brief #277](https://hcup-us.ahrq.gov/reports/statbriefs/sb277-Top-Reasons-Hospital-Stays-2018.pdf) · [CMS HRRP](https://www.cms.gov/medicare/quality/value-based-programs/hospital-readmissions) · [HWR NQF 1789](https://www.cms.gov/priorities/innovation/files/fact-sheet/bpciadvanced-fs-nqf1789.pdf) · [OP-18 CMS32v8](https://ecqi.healthit.gov/sites/default/files/ecqm/measures/CMS32v8.html) · [ED-1 CMS55v6](https://ecqi.healthit.gov/sites/default/files/ecqm/measures/CMS55v6.html) · [ED-2 CMS111v11](https://ecqi.healthit.gov/ecqm/hosp-inpt/2023/cms0111v11)

**Claims:** [X12 CARC](https://x12.org/codes/claim-adjustment-reason-codes) · [X12 RARC](https://x12.org/codes/remittance-advice-remark-codes) · [X12 group codes](https://x12.org/codes/claim-adjustment-group-codes) · [X12 element 1029](https://www.stedi.com/edi/x12/element/1029) · [CMS 835 flat file](https://www.cms.gov/medicare/billing/electronicbillingeditrans/downloads/835-flatfile.pdf) · [CMS Claims Processing Manual Ch.25](https://www.cms.gov/Regulations-and-Guidance/Guidance/Manuals/downloads/clm104c25.pdf) · [837I flattened dictionary](https://datainsight.health/docs/datadict/837i-claim/) · [CMS DE-SynPUF](https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-claims-synthetic-public-use-files/cms-2008-2010-data-entrepreneurs-synthetic-public-use-file-de-synpuf) · [CMS Blue Button CLM_FAC_TYPE_CD](https://bluebutton.cms.gov/resources/variables/clm_fac_type_cd) · [CLM_FREQ_CD](https://bluebutton.cms.gov/resources/variables/clm_freq_cd) · [CLM_SRC_IP_ADMSN_CD](https://bluebutton.cms.gov/resources/variables/clm_src_ip_admsn_cd) · [REV_CNTR](https://bluebutton.cms.gov/resources/variables/rev_cntr) · [Noridian discharge status codes](https://med.noridianmedicare.com/web/jea/topics/claim-submission/patient-discharge-status-codes) · [HCUP Statistical Brief #316](https://hcup-us.ahrq.gov/reports/statbriefs/sb316-most-expensive-conditions-by-payer-2022.pdf) · [CMS HCPCS quarterly](https://www.cms.gov/medicare/coding-billing/healthcare-common-procedure-system/quarterly-update) · [NUBC licence](https://www.nubc.org/license) · [FHIR R4 ExplanationOfBenefit](https://hl7.org/fhir/R4/explanationofbenefit.html)

**Pharmacy:** [FDA NDC product file definitions](https://www.fda.gov/drugs/drug-approvals-and-databases/ndc-product-file-definitions) · [FDA NDC package file definitions](https://www.fda.gov/drugs/drug-approvals-and-databases/ndc-package-file-definitions) · [RxNorm overview](https://www.nlm.nih.gov/research/umls/rxnorm/overview.html) · [RxNorm TTYs](https://www.nlm.nih.gov/research/umls/rxnorm/docs/appendix5.html) · [GS1 US DSCSA guideline R12](https://documents.gs1us.org/adobe/assets/deliver/urn:aaid:aem:4ea01fd3-a893-4114-a0ef-5156f8d022da/Guideline-Implementation-Guideline-Applying-GS1-Standards-for-DSCSA-and-Traceability-R12.pdf) · [FHIR R4 MedicationRequest](https://hl7.org/fhir/R4/medicationrequest.html) · [FHIR R5 InventoryReport](https://fhir.hl7.org/fhir/inventoryreport.html) · [FHIR R5 InventoryItem](https://fhir.hl7.org/fhir/inventoryitem.html) · [MIMIC-IV prescriptions](https://mimic.mit.edu/docs/iv/modules/hosp/prescriptions/) · [MIMIC-IV pharmacy](https://mimic.mit.edu/docs/iv/modules/hosp/pharmacy/) · [MIMIC-IV emar](https://mimic.mit.edu/docs/iv/modules/hosp/emar/) · [openFDA drug shortages](https://open.fda.gov/data/drugshortages/) · [ASHP 2023 shortages survey](https://www.ashp.org/-/media/assets/drug-shortages/docs/ASHP-2023-Drug-Shortages-Survey-Report.pdf) · [DEA controlled substances list](https://www.deadiversion.usdoj.gov/schedules/orangebook/c_cs_alpha.pdf) · [Federal Register: NDC format revision](https://www.federalregister.gov/documents/2026/03/05/2026-04368/revising-the-national-drug-code-format-and-drug-label-barcode-requirements)

**Beds / staffing / vitals:** [NHSN HRD weekly reporting form 57.101](https://www.cdc.gov/nhsn/pdfs/pscmanual/Hospital-Respiratory-Data-Form-Weekly-Reporting-57.101.pdf) · [NHSN HRD protocol](https://www.cdc.gov/nhsn/pdfs/pscmanual/HRD-Protocol-Final.pdf) · [Cal. Code Regs. Tit. 22 §70217](https://www.law.cornell.edu/regulations/california/Cal-Code-Regs-Tit-22-SS-70217) · [CMS PBJ Employee Detail PUF documentation](https://download.cms.gov/pbj/pbj_employeedetailpuf_documentation_april_2022.pdf) · [CMS PBJ Daily Nurse Staffing](https://data.cms.gov/quality-of-care/payroll-based-journal-daily-nurse-staffing) · [FHIR R4 vital signs profile](https://build.fhir.org/observation-vitalsigns.html) · [eICU vitalPeriodic](https://notebook.community/mit-eicu/eicu-code/notebooks/vitalperiodic) · [eICU CRD paper](https://www.nature.com/articles/sdata2018178) · [RCP NEWS2](https://www.rcp.ac.uk/resources/national-early-warning-score-news-2/) · [Leuchter et al., JAMA Netw Open — US hospital occupancy](https://www.uclahealth.org/news/release/us-facing-critical-hospital-bed-shortage-2032-ucla-research)
