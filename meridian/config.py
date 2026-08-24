"""
Meridian Health Network — synthetic data generator configuration.

Every parameter here is either:
  [VERIFIED]   traceable to a cited real source (see data-generation-schema-spec.md)
  [ASSUMPTION] our own modelling choice, no open source publishes it

Do not silently promote an [ASSUMPTION] to fact in the project documentation.
"""

from dataclasses import dataclass, field

SEED = 20260812

# ---------------------------------------------------------------------------
# Facility network — client-request Section 1: seven facilities, four regions
# ---------------------------------------------------------------------------

@dataclass
class Facility:
    facility_id: str
    name: str
    facility_type: str
    region: str
    city: str
    state: str
    zip: str
    county: str
    licensed_beds: int
    has_ed: bool
    ownership: str
    # [ASSUMPTION] baseline daily ED arrivals, scaled from bed count
    ed_arrivals_per_day: int
    # [ASSUMPTION] acuity multiplier — teaching hospital sees sicker patients
    acuity_bias: float = 1.0


FACILITIES = [
    Facility("330101", "Meridian General Hospital – Boston", "General Acute Care",
             "Northeast", "Boston", "MA", "02118", "Suffolk", 420, True,
             "Voluntary non-profit - Private", 190, 1.00),
    Facility("330102", "Meridian University Hospital", "Teaching",
             "Northeast", "Boston", "MA", "02215", "Suffolk", 610, True,
             "Voluntary non-profit - Church", 265, 1.25),
    Facility("050201", "Meridian General Hospital – Oakland", "General Acute Care",
             "West", "Oakland", "CA", "94609", "Alameda", 380, True,
             "Voluntary non-profit - Private", 175, 1.00),
    Facility("030301", "Meridian Regional Medical Center – Phoenix", "Regional",
             "West", "Phoenix", "AZ", "85006", "Maricopa", 250, True,
             "Proprietary", 130, 0.95),
    Facility("140401", "Meridian General Hospital – Chicago", "General Acute Care",
             "Midwest", "Chicago", "IL", "60612", "Cook", 340, True,
             "Voluntary non-profit - Private", 160, 1.05),
    Facility("110501", "Meridian Community Hospital – Savannah", "Community",
             "South", "Savannah", "GA", "31404", "Chatham", 95, True,
             "Government - Local", 55, 0.85),
    # Zero inpatient beds. Deliberate: forces divide-by-zero handling in Gold
    # and gives the DQ framework a real edge case to catch.
    Facility("450601", "Meridian Urgent Care – Austin", "Urgent Care",
             "South", "Austin", "TX", "78702", "Travis", 0, False,
             "Proprietary", 85, 0.60),
]

# ---------------------------------------------------------------------------
# Unit types
#
# nurse_ratio: [VERIFIED] Cal. Code Regs. Tit. 22 sec. 70217 — the only US
#   mandated nurse-to-patient ratio law, therefore the defensible source for a
#   staffing-adequacy KPI.
# alos_days / bed counts: [ASSUMPTION] — no open source publishes per-unit bed
#   counts by hospital size or ALOS by unit type at this granularity.
# ---------------------------------------------------------------------------

@dataclass
class UnitType:
    unit_type: str
    unit_code: str
    nurse_ratio: float          # patients per licensed nurse
    is_critical_care: bool
    monitored: bool             # emits continuous vitals telemetry
    alos_days: float
    alos_sigma: float
    bed_share: float            # share of facility licensed beds
    min_beds: int = 0


UNIT_TYPES = [
    UnitType("Emergency Department",   "ED",     4.0, False, True,  0.35, 0.30, 0.00, 0),
    UnitType("Medical Intensive Care", "MICU",   2.0, True,  True,  3.8,  2.60, 0.055, 6),
    UnitType("Surgical Intensive Care","SICU",   2.0, True,  True,  3.2,  2.20, 0.040, 0),
    UnitType("Cardiovascular ICU",     "CVICU",  2.0, True,  True,  3.5,  2.40, 0.035, 0),
    UnitType("Neonatal ICU",           "NICU",   2.0, True,  True,  12.0, 9.00, 0.030, 0),
    UnitType("Step-Down",              "SDU",    3.0, False, True,  3.0,  1.80, 0.060, 4),
    UnitType("Telemetry",              "TELE",   4.0, False, True,  3.4,  2.00, 0.090, 6),
    UnitType("Post-Anesthesia Care",   "PACU",   2.0, False, True,  0.25, 0.15, 0.030, 4),
    UnitType("Medical/Surgical",       "MS",     5.0, False, False, 4.4,  3.10, 0.330, 20),
    UnitType("Specialty Care Oncology","ONC",    4.0, False, False, 5.6,  4.00, 0.055, 0),
    UnitType("Pediatrics",             "PEDS",   4.0, False, False, 2.9,  1.90, 0.050, 0),
    UnitType("Labor & Delivery",       "LD",     2.0, False, True,  1.2,  0.70, 0.040, 0),
    UnitType("Postpartum",             "PP",     4.0, False, False, 2.1,  1.00, 0.055, 0),
    UnitType("Psychiatric",            "PSY",    6.0, False, False, 7.8,  5.50, 0.060, 0),
    UnitType("Rehabilitation",         "REHAB",  5.0, False, False, 11.0, 6.00, 0.030, 0),
    # Outpatient. The client request names outpatient wait times as one of its
    # two stated wait-time problems ("emergency and outpatient departments"),
    # so these cannot be omitted. They hold no inpatient beds -- bed_share 0 and
    # the bed count is sized from appointment volume in dimensions._build_units.
    # nurse_ratio: [ASSUMPTION] -- Cal. Title 22 sec. 70217 mandates ratios for
    #   inpatient units only and is silent on outpatient clinics.
    UnitType("Outpatient Clinic",      "OPC",    8.0, False, False, 0.0,  0.00, 0.000, 0),
    UnitType("Ambulatory Surgery",     "ASC",    4.0, False, True,  0.0,  0.00, 0.000, 0),
]

# Unit codes that see scheduled outpatient appointments rather than admissions.
OUTPATIENT_UNIT_CODES = ("OPC", "ASC")

# Small facilities do not carry every unit type.
UNITS_BY_FACILITY_TYPE = {
    "Teaching":           [u.unit_code for u in UNIT_TYPES],
    "General Acute Care": ["ED","MICU","SICU","CVICU","SDU","TELE","PACU","MS","ONC","PEDS","LD","PP","PSY","OPC","ASC"],
    "Regional":           ["ED","MICU","SDU","TELE","PACU","MS","PEDS","LD","PP","OPC","ASC"],
    "Community":          ["ED","MICU","TELE","PACU","MS","OPC"],
    "Urgent Care":        ["ED","OPC"],
}

# ---------------------------------------------------------------------------
# Occupancy targets
# [VERIFIED] Leuchter et al., JAMA Netw Open (UCLA, Feb 2025): US mean hospital
#   occupancy ~64% pre-pandemic, ~75% post-pandemic; 85% is the widely-used
#   bed-shortage threshold. CDC: ICU occupancy at 75% associates with ~12,000
#   excess deaths two weeks later.
# Per-facility spread and diurnal/weekly/seasonal amplitudes: [ASSUMPTION]
# ---------------------------------------------------------------------------

CAPACITY_THRESHOLD = 0.85
TARGET_OCCUPANCY = {
    "Teaching": 0.88, "General Acute Care": 0.78, "Regional": 0.74,
    "Community": 0.65, "Urgent Care": 0.00,
}
ICU_OCCUPANCY_UPLIFT = 0.07          # [ASSUMPTION] ICUs run hotter than house average
# Day-to-day swing applied to each unit's target so census varies instead of
# sitting pinned at the target -- otherwise every ICU reads "at capacity" every
# hour and the operational dashboard's alert has no signal. [ASSUMPTION]
OCCUPANCY_DAILY_JITTER = 0.11
OCCUPANCY_TARGET_CAP = 0.93
STAFFED_BED_FRACTION = (0.88, 0.97)  # [ASSUMPTION] staffed as share of licensed
BLOCKED_BED_FRACTION = (0.00, 0.04)  # [ASSUMPTION] out-of-service beds

# Hour-of-day arrival weights, 24 values. [ASSUMPTION] — shape reflects the
# well-known late-morning-to-evening ED peak but the amplitude is ours.
DIURNAL_ARRIVAL = [
    0.45,0.36,0.30,0.26,0.25,0.30,0.45,0.68,0.92,1.15,1.30,1.38,
    1.36,1.32,1.30,1.28,1.28,1.26,1.20,1.08,0.94,0.80,0.66,0.55,
]
# Monday=0 .. Sunday=6. [ASSUMPTION]
DOW_ARRIVAL = [1.08, 1.05, 1.04, 1.02, 1.00, 0.88, 0.93]
# Month 1..12 — winter respiratory season. [ASSUMPTION]
SEASONAL_ARRIVAL = [1.14,1.12,1.05,0.98,0.95,0.93,0.94,0.95,0.98,1.03,1.08,1.13]

# ---------------------------------------------------------------------------
# ED flow
# [VERIFIED] ESI acuity is a 1-5 scale, 1 = most acute (MIMIC-IV-ED `acuity`).
# ESI distribution, minutes, and admit probabilities: [ASSUMPTION]
# ---------------------------------------------------------------------------

ESI_DISTRIBUTION = [0.02, 0.16, 0.44, 0.31, 0.07]     # ESI 1..5
ADMIT_PROB_BY_ESI = [0.86, 0.58, 0.24, 0.06, 0.02]    # ESI 1..5
ARRIVAL_TRANSPORT = {"WALK IN": 0.70, "AMBULANCE": 0.26, "HELICOPTER": 0.01, "UNKNOWN": 0.03}

# Minutes: (median, sigma) lognormal
TRIAGE_DELAY_MIN = (9, 0.8)            # arrival -> triage
ED_WORKUP_MIN_BY_ESI = {              # triage -> admit decision or discharge
    1: (55, 0.55), 2: (135, 0.60), 3: (190, 0.62), 4: (110, 0.60), 5: (70, 0.60),
}
BOARDING_MIN = (95, 0.85)              # admit decision -> ED departure (ED-2)
# Boarding inflates when the receiving unit is full — that link is what makes
# the operational dashboard's capacity story real rather than decorative.
BOARDING_CAPACITY_PENALTY = 2.6

# triage -> first physician/APP contact. Without this, door-to-doctor cannot be
# computed at all, and it is a standard ED measure alongside OP-18/ED-1/ED-2.
# [ASSUMPTION] — CMS retired the OP-20 door-to-diagnostic-evaluation measure and
#   publishes no current national median, so these are clinically conventional
#   rather than sourced. Sicker patients are seen faster.
PROVIDER_SEEN_MIN_BY_ESI = {
    1: (4, 0.50), 2: (14, 0.65), 3: (34, 0.80), 4: (48, 0.85), 5: (55, 0.90),
}

# ---------------------------------------------------------------------------
# Outpatient
#
# The client request names outpatient wait times as one of two stated
# wait-time problems, and outpatient is normally the bulk of a health
# network's visit volume — which is also what closes the gap to the stated
# "several million patient visits a year".
#
# All [ASSUMPTION]. No open source publishes outpatient visits per ED arrival,
# appointment punctuality distributions, or clinic cycle times at this
# granularity.
#
# SIZING NOTE: outpatient dominates encounter volume. At 6.0 the generator
# produces roughly 7x the encounters (and therefore claims and diagnoses) of an
# ED+inpatient-only run. Lower this if batch output size matters more than
# matching the client's stated volume.
# ---------------------------------------------------------------------------

OUTPATIENT_PER_ED_ARRIVAL = 6.0        # outpatient appointments per ED arrival
# Clinics run business hours, so appointments are not spread across 24h.
OUTPATIENT_HOUR_WEIGHTS = {
    7: 0.02, 8: 0.09, 9: 0.12, 10: 0.13, 11: 0.11, 12: 0.05,
    13: 0.10, 14: 0.12, 15: 0.11, 16: 0.09, 17: 0.05, 18: 0.01,
}
OUTPATIENT_DOW_WEIGHTS = [1.06, 1.04, 1.03, 1.00, 0.97, 0.22, 0.08]  # Mon..Sun
# Patient arrival relative to appointment time, minutes. Most arrive early.
OUTPATIENT_ARRIVAL_OFFSET_MIN = (-18, 12)
# Appointment time -> provider seen. This is the outpatient wait measure, and
# it degrades as the clinic session fills up.
OUTPATIENT_SEEN_DELAY_MIN = (16, 0.85)
OUTPATIENT_VISIT_MIN = (22, 0.55)      # seen -> departure
OUTPATIENT_NO_SHOW_RATE = 0.081        # [ASSUMPTION] appointment never arrives
# Share of outpatient encounters that are same-day surgery rather than clinic.
OUTPATIENT_ASC_SHARE = 0.06
# Same-day surgery escalating to an inpatient admission.
ASC_ADMIT_PROB = 0.024

# ---------------------------------------------------------------------------
# Payer mix
# [VERIFIED] AHRQ HCUP Statistical Brief #316 (2022), share of 32.9M stays.
#   Medicare 40.2 / Medicaid 23.6 / Private 28.7 / Self-pay 4.2 / Other 3.3.
#   Medicare and Medicaid Advantage/MC splits within those totals: [ASSUMPTION]
# ---------------------------------------------------------------------------

PAYERS = [
    # payer_id, name, payer_type, claim_filing_indicator, share, prompt_pay_days
    ("PAY001", "Medicare Part A/B",          "Medicare",           "MB", 0.242, 30),
    ("PAY002", "Vantage Medicare Advantage", "Medicare Advantage", "16", 0.160, 60),
    ("PAY003", "State Medicaid FFS",         "Medicaid",           "MC", 0.118, 30),
    ("PAY004", "Harborview Medicaid MC",     "Medicaid MC",        "MC", 0.118, 45),
    ("PAY005", "Atlas Health Commercial",    "Commercial",         "CI", 0.126, 40),
    ("PAY006", "Northwind PPO",              "Commercial",         "CI", 0.101, 40),
    ("PAY007", "Ironbridge HMO",             "Commercial",         "CI", 0.060, 40),
    ("PAY008", "Self-Pay",                   "Self-Pay",           "09", 0.042, 0),
    ("PAY009", "Other / Workers Comp",       "Other",              "WC", 0.033, 45),
]

# ---------------------------------------------------------------------------
# Claims / revenue cycle
# [VERIFIED] Kodiak Solutions: initial denial rate 11.81% of claims (2024);
#   median final denial 2.7% (2025); median days to insurance payment 55.2
#   (2025); ~90% of claims ultimately paid. Kodiak states rates are "highly
#   variable by payor category, with Medicaid leading" and that "Medicare
#   Advantage plans had initial and final denial rates more than double the
#   rates for traditional Medicare" -- but publishes no per-payer numbers.
# Therefore the per-payer multipliers below are [ASSUMPTION], calibrated so the
#   blended rate lands on Kodiak's published 11.81%.
# [VERIFIED] regulatory floors: Medicare FFS 30-day prompt payment standard;
#   Medicaid 42 CFR 447.45(d) 90% of clean claims within 30 days.
# ---------------------------------------------------------------------------

BLENDED_INITIAL_DENIAL_RATE = 0.1181
DENIAL_MULTIPLIER_BY_PAYER_TYPE = {
    "Medicare": 0.51, "Medicare Advantage": 1.14, "Medicaid": 1.34,
    "Medicaid MC": 1.31, "Commercial": 1.04, "Self-Pay": 0.0, "Other": 0.95,
}
# Calibrated so the volume-weighted blend lands on Kodiak's published median
# of 55.2 days to insurance payment (2025), while respecting the regulatory
# floors. The per-payer split itself is [ASSUMPTION] -- no free source
# publishes days-to-payment by payer.
DAYS_TO_PAYMENT_BY_PAYER_TYPE = {   # (median days, lognormal sigma)
    "Medicare": (34, 0.35), "Medicare Advantage": (82, 0.55),
    "Medicaid": (42, 0.45), "Medicaid MC": (74, 0.55),
    "Commercial": (56, 0.45), "Self-Pay": (150, 0.85), "Other": (64, 0.55),
}
APPEAL_OVERTURN_RATE = 0.427        # [VERIFIED] Kodiak: 42.7% appeal success (2024)
CODING_LAG_DAYS = (2, 6)            # discharge -> claim submission [ASSUMPTION]

# ---------------------------------------------------------------------------
# Vitals streaming
# [VERIFIED] eICU vitalPeriodic: "interfaced as 1 minute averages, archived as
#   5 minute median values."
# [VERIFIED] LOINC codes and UCUM units from the FHIR R4 vital signs profile.
#   Note 2708-6 (profile) not 59408-5 (pulse-ox specific, US Core / Vitals IG).
# Normal ranges and variance: [ASSUMPTION] (clinically conventional)
# ---------------------------------------------------------------------------

VITALS_INTERVAL_MIN = 5

VITAL_PARAMS = [
    # loinc, name, uom, healthy_mean, healthy_sd, hard_min, hard_max, decimals
    ("8867-4", "Heart rate",                        "/min",  78,  11, 25,  220, 0),
    ("8480-6", "Systolic arterial blood pressure",  "mm[Hg]",122, 14, 50,  260, 0),
    ("8462-4", "Diastolic arterial blood pressure", "mm[Hg]", 72,  9, 25,  150, 0),
    ("9279-1", "Respiratory Rate",                  "/min",   16,  3,  4,   60, 0),
    ("8310-5", "Body temperature",                  "Cel",  36.8,0.4, 32, 42.5, 1),
    ("2708-6", "Oxygen saturation in Arterial blood","%",      97,1.8, 60,  100, 0),
]

# [VERIFIED] NEWS2 (Royal College of Physicians): aggregate >=5 triggers urgent
#   clinical review, >=7 an emergency response, single parameter scoring 3
#   prompts clinician review.
# NOTE the systolic and SpO2-Scale-2 bands still need confirming against the
#   official RCP chart -- see the verify-before-coding checklist in the spec.
NEWS2_URGENT = 5
NEWS2_EMERGENCY = 7

# ---------------------------------------------------------------------------
# Pharmacy
# DEA schedules and drug codes: [VERIFIED] DEA Controlled Substances list.
#   Propofol and dexmedetomidine are correctly NOT scheduled.
# Dose forms / classes / usage weights: [ASSUMPTION] -- must be validated
#   against FDA product.txt before the documentation deliverable claims them.
# ---------------------------------------------------------------------------

# name, dose_form, route, class, dea_schedule, dea_code, usage_weight,
# unit_cost, shortage_prone, high_alert
FORMULARY = [
    ("Sodium Chloride 0.9%",        "INJECTION, SOLUTION", "INTRAVENOUS", "Electrolyte",              None,  None, 10.0, 1.85,  False, False),
    ("Acetaminophen 325 MG",        "TABLET",              "ORAL",        "Analgesic",                None,  None,  8.5, 0.04,  False, False),
    ("Ondansetron 4 MG/2ML",        "INJECTION",           "INTRAVENOUS", "5-HT3 Antagonist",         None,  None,  6.2, 1.40,  True,  False),
    ("Pantoprazole 40 MG",          "INJECTION, POWDER",   "INTRAVENOUS", "Proton Pump Inhibitor",    None,  None,  5.4, 3.20,  False, False),
    ("Heparin Sodium 5000 UNT/ML",  "INJECTION, SOLUTION", "SUBCUTANEOUS","Anticoagulant",            None,  None,  5.0, 2.10,  False, True),
    ("Enoxaparin 40 MG/0.4ML",      "INJECTION, SOLUTION", "SUBCUTANEOUS","LMW Heparin",              None,  None,  4.6, 8.75,  False, True),
    ("Cefazolin 1 G",               "INJECTION, POWDER",   "INTRAVENOUS", "Cephalosporin",            None,  None,  4.4, 4.60,  True,  False),
    ("Piperacillin-Tazobactam 3.375 G","INJECTION, POWDER","INTRAVENOUS", "Beta-Lactam Combination",  None,  None,  4.0, 9.20,  True,  False),
    ("Vancomycin 1 G",              "INJECTION, POWDER",   "INTRAVENOUS", "Glycopeptide",             None,  None,  3.8, 7.40,  False, True),
    ("Ceftriaxone 1 G",             "INJECTION, POWDER",   "INTRAVENOUS", "Cephalosporin",            None,  None,  3.5, 5.10,  True,  False),
    ("Furosemide 40 MG",            "TABLET",              "ORAL",        "Loop Diuretic",            None,  None,  3.4, 0.06,  False, False),
    ("Metoprolol Tartrate 5 MG/5ML","INJECTION",           "INTRAVENOUS", "Beta Blocker",             None,  None,  3.0, 2.95,  False, False),
    ("Insulin Aspart 100 UNT/ML",   "INJECTION, SOLUTION", "SUBCUTANEOUS","Insulin",                  None,  None,  2.9, 26.40, False, True),
    ("Levetiracetam 500 MG/5ML",    "INJECTION, SOLUTION", "INTRAVENOUS", "Antiepileptic",            None,  None,  2.6, 6.80,  False, False),
    ("Methylprednisolone 125 MG",   "INJECTION, POWDER",   "INTRAVENOUS", "Corticosteroid",           None,  None,  2.5, 11.30, True,  False),
    ("Famotidine 20 MG/2ML",        "INJECTION",           "INTRAVENOUS", "H2 Antagonist",            None,  None,  2.3, 1.95,  True,  False),
    ("Albuterol 2.5 MG/3ML",        "INHALATION SOLUTION", "RESPIRATORY", "Beta2 Agonist",            None,  None,  2.2, 0.55,  False, False),
    ("Potassium Chloride 10 MEQ",   "INJECTION, SOLUTION", "INTRAVENOUS", "Electrolyte",              None,  None,  2.1, 3.40,  False, True),
    ("Atorvastatin 40 MG",          "TABLET",              "ORAL",        "Statin",                   None,  None,  2.0, 0.09,  False, False),
    ("Lisinopril 10 MG",            "TABLET",              "ORAL",        "ACE Inhibitor",            None,  None,  1.9, 0.05,  False, False),
    ("Aspirin 81 MG",               "TABLET, DELAYED RELEASE","ORAL",     "Antiplatelet",             None,  None,  1.8, 0.02,  False, False),
    ("Apixaban 5 MG",               "TABLET",              "ORAL",        "Direct Oral Anticoagulant",None,  None,  1.6, 7.90,  False, True),
    ("Norepinephrine 4 MG/4ML",     "INJECTION, SOLUTION", "INTRAVENOUS", "Vasopressor",              None,  None,  1.5, 14.60, True,  True),
    ("Propofol 10 MG/ML",           "INJECTION, EMULSION", "INTRAVENOUS", "General Anesthetic",       None,  None,  1.4, 9.80,  False, True),
    ("Dexmedetomidine 200 MCG/2ML", "INJECTION, SOLUTION", "INTRAVENOUS", "Alpha2 Agonist",           None,  None,  1.2, 22.50, False, True),
    ("Fentanyl Citrate 100 MCG/2ML","INJECTION, SOLUTION", "INTRAVENOUS", "Opioid Analgesic",         "CII", "9801", 2.4, 1.30,  True,  True),
    ("Hydromorphone 1 MG/ML",       "INJECTION, SOLUTION", "INTRAVENOUS", "Opioid Analgesic",         "CII", "9150", 1.7, 2.60,  False, True),
    ("Morphine Sulfate 4 MG/ML",    "INJECTION, SOLUTION", "INTRAVENOUS", "Opioid Analgesic",         "CII", "9300", 1.6, 2.20,  True,  True),
    ("Oxycodone 5 MG",              "TABLET",              "ORAL",        "Opioid Analgesic",         "CII", "9143", 1.5, 0.18,  False, True),
    ("Hydrocodone-Acetaminophen 5-325 MG","TABLET",        "ORAL",        "Opioid Combination",       "CII", "9193", 1.3, 0.22,  False, True),
    ("Methadone 10 MG",             "TABLET",              "ORAL",        "Opioid Analgesic",         "CII", "9250", 0.5, 0.30,  False, True),
    ("Ketamine 500 MG/10ML",        "INJECTION, SOLUTION", "INTRAVENOUS", "General Anesthetic",       "CIII","7285", 0.7, 8.40,  False, True),
    ("Buprenorphine 0.3 MG/ML",     "INJECTION, SOLUTION", "INTRAVENOUS", "Partial Opioid Agonist",   "CIII","9064", 0.4, 6.20,  False, True),
    ("Midazolam 2 MG/2ML",          "INJECTION, SOLUTION", "INTRAVENOUS", "Benzodiazepine",           "CIV", "2884", 1.5, 2.80,  True,  True),
    ("Lorazepam 2 MG/ML",           "INJECTION, SOLUTION", "INTRAVENOUS", "Benzodiazepine",           "CIV", "2885", 1.4, 3.10,  True,  True),
    ("Diazepam 5 MG",               "TABLET",              "ORAL",        "Benzodiazepine",           "CIV", "2765", 0.6, 0.11,  False, False),
    ("Alprazolam 0.5 MG",           "TABLET",              "ORAL",        "Benzodiazepine",           "CIV", "2882", 0.5, 0.08,  False, False),
    ("Phenobarbital 65 MG/ML",      "INJECTION, SOLUTION", "INTRAVENOUS", "Barbiturate",              "CIV", "2285", 0.3, 4.90,  False, False),
    ("Tramadol 50 MG",              "TABLET",              "ORAL",        "Opioid Analgesic",         "CIV", "9752", 0.9, 0.14,  False, False),
    ("Pregabalin 75 MG",            "CAPSULE",             "ORAL",        "Anticonvulsant",           "CV",  "2782", 0.6, 0.35,  False, False),
]

# Inventory behaviour. [ASSUMPTION] -- the source for these (a pharmacy
# consulting blog) is not authoritative; label as modelling choices.
DAYS_ON_HAND_TARGET = (18, 34)
REORDER_POINT_DAYS = 7
SAFETY_STOCK_DAYS = 4
ABC_THRESHOLDS = (0.80, 0.95)     # cumulative value share for A / B cut-offs
SHORTAGE_PROBABILITY_PER_DRUG = 0.06   # [ASSUMPTION] no published SKU-level rate
SHORTAGE_DURATION_DAYS = (14, 120)

# [VERIFIED] openFDA drugshortages `status` and `shortage_reason` value sets
SHORTAGE_STATUS = ["Currently in Shortage", "Resolved", "Discontinuation", "Available"]
SHORTAGE_REASONS = [
    "Requirements related to complying with good manufacturing practices",
    "Regulatory delay",
    "Shortage of an active ingredient",
    "Shortage of an inactive ingredient component",
    "Discontinuation of the manufacture of the drug",
    "Delay in shipping of the drug",
    "Demand increase for the drug",
]

# ---------------------------------------------------------------------------
# Staffing
# [ASSUMPTION] all of the below -- HPPD benchmarks by unit type and published
#   vacancy / turnover / agency-usage rates were not verified. Do not cite.
# ---------------------------------------------------------------------------

# code, start hour, length hours.
#
# A clean three-shift 8-hour pattern, because "by department and shift" is a
# stated client question and the answer has to be coherent. The previous
# two-shift 12-hour pattern (D 07-19, N 19-07) already covered the full day, so
# bolting an evening shift onto it double-counted the same hours and made every
# evening look catastrophically understaffed against a census requirement it
# was never rostered to meet.
#
# Each of D/E/N now covers its own block and is staffed to the census ratio.
# On-call sits outside the pattern: it is cover, rostered as a fixed small team
# rather than scaled from census, and a mandated-ratio KPI is not computed
# against it.
SHIFTS = [("D", 7, 8), ("E", 15, 8), ("N", 23, 8), ("OC", 19, 12)]
# Night conventionally runs slightly leaner than day. [ASSUMPTION]
SHIFT_COVERAGE_SHARE = {"D": 1.00, "E": 0.96, "N": 0.90}
CALL_OUT_RATE = 0.043
OVERTIME_RATE = 0.11
CONTRACT_STAFF_BASE = 0.09                 # baseline agency share
# Chronic understaffing, injected deliberately and non-uniformly so the
# operational dashboard has something to discover. Uniform noise would hide it.
UNDERSTAFF_BIAS = {
    ("Community", "MS", "N"): 0.78,
    ("Community", "MS", "D"): 0.88,
    ("Community", "TELE", "N"): 0.80,
    ("General Acute Care", "MS", "N"): 0.87,
    ("Regional", "MS", "N"): 0.84,
    ("Teaching", "MICU", "N"): 0.92,
}
UNDERSTAFF_DEFAULT = 0.99
WEEKEND_STAFF_PENALTY = 0.94
STAFF_PER_BED = 2.4                        # headcount pool sizing

# Staff churn inside the observation window, so the dimension actually changes
# between weekly snapshots and there is SCD-2 history to model. Without this
# every dim_staff snapshot is byte-identical and slowly-changing-dimension
# handling has nothing to be tested against.
STAFF_HIRES_PER_WEEK = (2, 9)              # per facility [ASSUMPTION]
STAFF_TERMINATIONS_PER_WEEK = (1, 7)       # per facility [ASSUMPTION]
STAFF_UNIT_TRANSFER_PER_WEEK = (0, 4)      # per facility [ASSUMPTION]
# Minimum employment span. Independent draws previously produced termination
# dates on or before the hire date -- a defect that was never recorded in the
# answer key, so nobody could account for it.
MIN_EMPLOYMENT_DAYS = 21

# ---------------------------------------------------------------------------
# Source-system provenance
#
# The client states facilities "run on a mix of electronic health record (EHR)
# systems". That mix is what makes standardisation a real problem rather than a
# formality: each system writes timestamps and enum casing its own way.
# System names are generic on purpose -- naming real vendors in synthetic data
# invites the output being mistaken for a real extract.
# ---------------------------------------------------------------------------

EHR_SYSTEMS = {
    "330101": "MERIDIAN_EHR_CORE",
    "330102": "ACADEMIC_CIS",          # teaching hospital runs its own
    "050201": "MERIDIAN_EHR_CORE",
    "030301": "REGIONAL_HIS",
    "140401": "MERIDIAN_EHR_CORE",
    "110501": "COMMUNITY_CARE_EHR",
    "450601": "URGENTCARE_CLOUD",
}
# Per-system timestamp format. Silver has to normalise these; Bronze must not.
EHR_DATE_FORMATS = {
    "MERIDIAN_EHR_CORE": "%Y-%m-%d %H:%M:%S",
    "ACADEMIC_CIS":      "%Y-%m-%dT%H:%M:%S",
    "REGIONAL_HIS":      "%d/%m/%Y %H:%M",
    "COMMUNITY_CARE_EHR": "%m/%d/%Y %H:%M",
    "URGENTCARE_CLOUD":  "%Y-%m-%d %H:%M",
}
# Per-system enum casing, applied to encounter_class and disposition values.
EHR_ENUM_CASE = {
    "MERIDIAN_EHR_CORE": "lower",
    "ACADEMIC_CIS":      "lower",
    "REGIONAL_HIS":      "upper",
    "COMMUNITY_CARE_EHR": "title",
    "URGENTCARE_CLOUD":  "lower",
}

# Payer name spelling variants, keyed by payer_id. Real claim files carry the
# payer's name as the clearinghouse received it, so the same payer arrives
# spelled several ways and Silver has to resolve them to one canonical name.
PAYER_NAME_VARIANTS = {
    "PAY001": ["Medicare Part A/B", "MEDICARE PART A/B", "Medicare A/B", "medicare part a+b"],
    "PAY002": ["Vantage Medicare Advantage", "VANTAGE MED ADV", "Vantage MA", "Vantage Medicare Adv."],
    "PAY003": ["State Medicaid FFS", "STATE MEDICAID", "St Medicaid FFS"],
    "PAY004": ["Harborview Medicaid MC", "HARBORVIEW MCO", "Harborview Medicaid Managed Care"],
    "PAY005": ["Atlas Health Commercial", "ATLAS HEALTH", "Atlas Hlth Comm"],
    "PAY006": ["Northwind PPO", "NORTHWIND PPO", "Northwind P.P.O."],
    "PAY007": ["Ironbridge HMO", "IRONBRIDGE HMO", "Ironbridge H.M.O."],
    "PAY008": ["Self-Pay", "SELF PAY", "Self Pay"],
    "PAY009": ["Other / Workers Comp", "WORKERS COMP", "Other/WC"],
}

# ---------------------------------------------------------------------------
# Readmission
# [VERIFIED] CMS HRRP: 30-day unplanned readmission, index must be discharged
#   alive, excludes AMA / primary psych / rehab / cancer treatment, counts one
#   readmission per index, planned readmissions excluded. HRRP cohorts are AMI,
#   COPD, HF, pneumonia, CABG, elective THA/TKA.
# Per-cohort rates: [ASSUMPTION]
# ---------------------------------------------------------------------------

READMISSION_RATE_BY_COHORT = {
    "HF": 0.215, "COPD": 0.196, "PN": 0.168, "AMI": 0.152, "OTHER": 0.112,
}
PLANNED_READMISSION_SHARE = 0.16
READMISSION_DAY_WEIGHTS_LAMBDA = 11.0      # exponential-ish, front-loaded

# ---------------------------------------------------------------------------
# Defect injection — test material for the DQ framework.
# A perfectly clean generator makes the DQ deliverable undemonstrable.
# Every defect is recorded in an answer key so we can prove detection.
# ---------------------------------------------------------------------------

DEFECT_RATES = {
    "null_required_field":    0.0040,
    "invalid_code_value":     0.0025,
    "type_nonconformance":    0.0030,
    "duplicate_row":          0.0015,
    "orphan_foreign_key":     0.0012,
    "temporal_inversion":     0.0010,
    "outlier_numeric":        0.0018,
    "duplicate_event_id":     0.0020,
    "late_event":             0.0060,
    # File-level failures. Per FILE, not per row, so the rates are far higher
    # than the row rates above -- at 0.004 per row a missing file would never
    # happen, and the client requires a demonstrably failed run to recover from.
    # Roughly one missing file every ~60 drops and one short file every ~35.
    #
    # There is deliberately no "late file" class. Whether a drop arrived on time
    # is a property of the delivery channel, not of the file, so it cannot be
    # detected from the files alone and faking it would mean inventing a column
    # no source system emits. Row-level lateness IS detectable and is covered by
    # late_event via the event_time / ingest_time split.
    "missing_file":           0.016,
    "truncated_file":         0.028,
}
CHAOS_MULTIPLIER = 12.0        # --chaos flag, for the Phase 5 break-it test
