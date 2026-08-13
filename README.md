# Meridian Health Network — Synthetic Source Data Generator

Generates all **seven source feeds** for the Patient Care & Hospital Operations
Analytics Platform, on schemas anchored to real published healthcare standards.

Companion document: **`data-generation-schema-spec.md`** — the field-level
specification, with citations for every code set and a verify-before-coding
checklist. It is maintained outside this repository; ask Mubarak for a copy.
Read it first if you are changing schemas; this README is just how to run the
thing.

---

## Quick start

```bash
pip install -r requirements.txt

python run.py --days 7            # smoke test, everything lands in ./out
python validate.py out            # prove the data answers the client's questions
```

`--days 7` finishes in well under a minute and writes every one of the seven
feeds. Open the files, check the columns, then scale up. Generating 18 months
first means any column mistake costs you the whole run again.

### Warmup

The simulation opens with every bed empty, so without a warmup the first days
of any window are not steady state. Measured on a cold 7-day run, vitals events
per day went 343k → 748k → 838k → … → 890k: census takes ~3 days to fill, and
occupancy, staffing ratios and vitals volume are all understated until it does.

`--warmup` (**default 14 days**) simulates that many days *before* `--start`
and does not emit them. It also gives the 30-day readmission lookback real
index admissions to point back to instead of an empty history. Set `--warmup 0`
to reproduce the old cold-start behaviour — but then discard the first 3 days
before calibrating anything.

Warmup days cost simulation time but emit nothing, so on an 18-month run the
overhead is negligible.

---

## The seven sources

| # | Source | Landing | Cadence | Format | Anchored on |
|---|---|---|---|---|---|
| 1 | EHR encounters & admissions | cloud files | daily | CSV | Synthea + MIMIC-IV `admissions`/`transfers`/`edstays` |
| 2 | Billing & claims | cloud files | daily | CSV | flattened X12 837I + 835 |
| 3 | Pharmacy inventory | cloud files | daily snapshot | CSV | FHIR R5 `InventoryReport` semantics + FDA NDC + RxNorm |
| 4 | Bed capacity | cloud files | hourly + weekly roll-up | CSV | CDC NHSN Hospital Respiratory Data |
| 5 | Staff schedules | **SharePoint** | weekly | **XLSX** | CMS PBJ column idiom + CA Title 22 ratios |
| 6 | Patient vitals | **Kafka** | 5-minute archive | JSON | eICU `vitalPeriodic`, LOINC vitals, NEWS2 |
| 7 | Prescription issuance | **Kafka** | continuous | JSON | MIMIC-IV `hosp.pharmacy` + FHIR `MedicationRequest` |

---

## It is one simulation, not seven generators

This is the single most important design decision, and it is why the code is
shaped the way it is.

```
patient arrives → ED encounter → triage → bed assigned  →  bed occupancy snapshot
                                       ↓
                              admitted (or discharged)
                                       ↓
                    vitals monitored ────────────────────→  vitals stream
                    drugs ordered   ────────────────────→  prescription stream
                         ↓                                        ↓
                    inventory depleted ──────────────────→  inventory extract
                         ↓
                    transfers, LOS ─────────────────────→  EHR extract
                         ↓
                    discharge → coded → billed ─────────→  claims extract
                         ↓
                    (30-day window) → possible readmission
                                       ↓
                    staff rostered against census ──────→  staff schedule
```

Generate the seven independently and the cross-source referential-integrity
checks become meaningless while the dashboards contradict each other — bed
occupancy that doesn't move when patients are admitted, prescriptions for
discharged patients, claims with no matching encounter.

Concretely, capacity actually constrains flow: when the receiving unit is full,
ED boarding time inflates (`BOARDING_CAPACITY_PENALTY`), patients cascade down
the acuity ladder to whatever unit has a bed, and if nothing is open they
transfer out. That link is the entire capacity story on the operational
dashboard, and you only get it from a simulation.

---

## Pushing to Fabric

**Batch → OneLake Files.** OneLake speaks the ADLS Gen2 API, so workspace is
the filesystem and the path is `<Lakehouse>.Lakehouse/Files/<prefix>/…`.

```bash
python run.py --days 7 \
  --onelake-workspace "Meridian-DEV" \
  --onelake-lakehouse "lh_bronze" \
  --onelake-prefix "landing"
```

Auth is `DefaultAzureCredential` — `az login` locally, or a service principal
via environment variables.

**Stream → Fabric Eventstream Kafka endpoint.** You do not need Azure Event
Hubs or Upstash. In the Eventstream, add a **custom endpoint source**, publish,
select the source tile, then the **Kafka** tab → **SAS Key Authentication**
page gives you all three values.

```bash
python run.py --days 7 \
  --kafka-bootstrap "<namespace>.servicebus.windows.net:9093" \
  --kafka-connection-string "Endpoint=sb://…;SharedAccessKey=…" \
  --kafka-topic "es_meridian"
```

> **Replay path.** Eventstream has no Event Hubs Capture equivalent, so the
> generator always keeps a raw JSONL archive locally even when pushing to
> Kafka. The more Fabric-native option is to attach a **second destination** on
> the same Eventstream writing raw events to a Lakehouse — that gives you Bronze
> replay with no extra Azure resource. Update the architecture diagram
> accordingly; it currently shows Event Hubs Capture → ADLS.

---

## Data quality: defects with an answer key

The client requires a DQ framework with visible pass/fail results, and
work-plan task 5.2 requires deliberately breaking something. A perfectly clean
generator makes both undemonstrable.

Every run injects defects at a configurable rate **and records each one** in
`out/dq_answer_key.json`, so in Phase 5 you can prove the framework caught them
rather than asserting it.

```bash
python run.py --days 7                # ~0.5-2% defect rate
python run.py --days 7 --chaos        # 12x rates, for the break-it test
python run.py --days 7 --no-defects   # clean baseline
```

| Defect class | Which DQ check should catch it |
|---|---|
| null required field | completeness |
| invalid code value | validity / domain |
| type non-conformance (`"12.5 mg"` in a numeric field) | type conformance |
| numeric outlier | validity / plausibility range |
| duplicate row / duplicate `event_id` | uniqueness / dedupe |
| orphan foreign key | **cross-source referential integrity** |
| temporal inversion (discharge before admit) | temporal validity |
| late event | late-data handling, watermarks |
| mixed date formats in the XLSX | parsing / standardisation |
| hourly beds vs NHSN weekly mismatch | reconciliation |
| same person, different MRN per facility | identity matching |

> ⚠️ **`dq_answer_key.json` is the marking scheme, not source data. Never land
> it in Bronze.**

Always run a `--no-defects` baseline alongside a normal run. When something
looks wrong, that tells you in one step whether it is a generator bug or an
injected defect doing its job. Both were true during development, and only the
baseline separated them.

---

## What `validate.py` proves

It computes all six client business questions using the **real** measure
definitions, not approximations:

- **Wait times** — OP-18, ED-1 and ED-2 with the actual CMS exclusions (expired
  patients, the 1-hour ED→inpatient join, the 6-hour transfer-in lookback)
- **Bed capacity** — occupancy against the 85% shortage threshold, plus NHSN
  weekly-to-hourly reconciliation
- **Staffing** — actual nurse-to-patient ratio against the California Title 22
  mandated target, by department and shift
- **Readmissions** — CMS HRRP index eligibility and exclusions, one per index,
  with right-censoring of the trailing 30 days
- **Claims** — denial rate from `CLP02 = 4`, days-in-AR, revenue at risk by
  payer, and critically: contractual write-offs (`CO-45`/`97`/`24`) kept
  separate from true denials
- **Stockouts** — days-on-hand against reorder point

Plus cross-source referential integrity and answer-key reconciliation.

**If a KPI cannot be computed here, it cannot be computed in the Gold layer
either** — so the schema is wrong, and it is far cheaper to fix now than after
the medallion layers are built.

---

## Calibration status

Landing close to published benchmarks (30–90 day runs):

| Metric | Generated | Published source |
|---|---|---|
| Initial claim denial rate | ~11.8% | 11.81% (Kodiak, 2024) |
| Mean hospital occupancy | ~79% | ~75% post-pandemic (JAMA Netw Open, 2025) |
| Median days to insurance payment | ~55 | 55.2 (Kodiak, 2025) |
| Denial rate by payer | Medicaid highest, MA ≈ 2× traditional Medicare | direction confirmed by Kodiak (no figures published) |
| ESI acuity scale | 1–5, 1 = most acute | MIMIC-IV-ED |
| Nurse-to-patient targets | ICU 1:2, ED 1:4, SDU 1:3, TELE 1:4, MS 1:5 | Cal. Code Regs. Tit. 22 §70217 |
| Vitals cadence | 5-minute archive of 1-minute averages | eICU `vitalPeriodic` |

Every parameter in `config.py` is tagged **`[VERIFIED]`** (traceable to a cited
source) or **`[ASSUMPTION]`** (our modelling choice). **Do not let an
`[ASSUMPTION]` be presented as fact in the project documentation** — several
things the client will ask about, notably item-level stockout probability and
HPPD benchmarks, have no published figure at all.

---

## Sizing note before the full run

The vitals stream dwarfs everything else. Measured, not estimated, from a
7-day run:

| Artefact | Per day | 548 days (18 months) |
|---|---|---|
| All batch feeds (CSV + XLSX) | ~2 MB | **~1.2 GB** |
| `prescription-events` raw | ~13 MB | ~7 GB |
| `patient-vitals` raw JSONL | **~570 MB** | **~310 GB** |
| `patient-vitals` gzipped | ~33 MB | ~18 GB |

Why vitals is so large: `emit_vitals` is tall/EAV — one event per *parameter*
per reading, six parameters in `VITAL_PARAMS`, every 5 minutes, for every
occupied monitored bed. That is ~890k events/day at steady state, and each
event spends ~639 bytes of envelope (UUID `event_id`, two timestamps,
`source_system`, six ids, LOINC code + spelled-out parameter name) to deliver
one number. The format is deliberate — it matches how monitors actually emit
and gives Silver real pivoting work — but it means the feed is ~17x
compressible.

Two controls, and you want both:

**1. The local archive is gzipped by default** (`.jsonl.gz`, level 6, 17.4x).
Spark, pandas and Fabric read it transparently. `--no-gzip-streams` opts out.

**2. `--stream-days N` limits the streams to the last N days** of the window
while batch still covers all of it. The operational dashboard only answers
"what needs action today", so 30–60 days of vitals is enough; the batch feeds
are what carry seasonality, trend and the 30-day readmission window.

```bash
# one run: 18 months of batch history + 45 days of vitals, ~2.7 GB total
python run.py --days 548 --start 2025-02-01 --stream-days 45
```

Prefer that single run over two separate ones. Splitting it across two
invocations with the same `--seed` does *not* reproduce the same hospital: the
RNG is consumed in simulation order, so a different `--days` or `--start`
yields a different patient population and different bed state. One run keeps
the streams referentially consistent with the batch feeds they must join to.

If you still need less, the remaining lever is `VITALS_INTERVAL_MIN` in
`config.py` — but it is tagged `[VERIFIED]` against eICU's 5-minute archive,
so changing it means the cadence claim in the documentation stops being true.
Say so explicitly if you do.

---

## Layout

```
meridian/
  config.py       network, units, formulary, payers, all tunable parameters
                  (each tagged [VERIFIED] or [ASSUMPTION])
  refdata.py      real code sets: ICD-10-CM, CARC/RARC, revenue codes, HCPCS,
                  MIMIC value sets, HL7/FHIR value sets, DEA schedules
  dimensions.py   the conformed join spine: facility, unit, patient, staff,
                  drug, payer
  simulate.py     the patient-journey engine and live bed state
  emitters.py     the seven source feeds
  sinks.py        local / OneLake / Fabric Eventstream Kafka
  defects.py      defect injection with the answer key
run.py            CLI
validate.py       KPI + integrity + answer-key harness
```

## Open items

Carried from the spec — worth closing before the documentation deliverable:

1. **Real NDC and RXCUI values.** Currently NDC-shaped and RXCUI-shaped
   synthetic codes. Drop the FDA `product.txt` and the RxNorm Prescribable
   Content release into `refdata/` and they get used automatically. **Verify the
   live `product.txt` header casing first** — FDA docs render CamelCase but the
   file appears to be UPPERCASE.
2. **MS-DRG assignment.** `drg_code` is null. Parse the CMS v43/v44 Definitions
   Manual rather than hard-coding numbers.
3. **NEWS2 systolic BP and SpO2 Scale 2 bands.** Confirm against the official
   RCP chart; the PDF I extracted from disagreed with the canonical table.
4. **Alarm rates.** The alarm-fatigue literature (alarms per bed per day, %
   clinically actionable) was not retrieved. Needed to defend the critical-alert
   tile on the operational dashboard.
5. **Whether a CDC NHSN *hospital* nurse-staffing-hours schema exists** — if so
   it is a better anchor for `dim_staff` than nursing-home PBJ.
