"""
Reference code sets.

Everything in this module is a REAL published code value, transcribed from the
sources cited in data-generation-schema-spec.md. Nothing here is invented.

Where a full official list is large (ICD-10-CM, NDC, HCPCS, MS-DRG), we embed a
verified seed subset and provide a loader that will use the full CMS/FDA flat
file if it has been downloaded into ./refdata/. See fetch_reference_data.py.
"""

import os
import csv

REFDATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "refdata")

# ---------------------------------------------------------------------------
# ICD-10-CM — [VERIFIED] descriptions confirmed against the 2026 code edition.
# Weights approximate AHRQ HCUP Statistical Brief #277 national stay counts.
# hrrp_cohort maps to the CMS HRRP measure cohorts.
# ---------------------------------------------------------------------------

# code, long_title, weight, setting, hrrp_cohort
ICD10_SEED = [
    ("A41.9",  "Sepsis, unspecified organism",                                   8.00, "IP",    "OTHER"),
    ("A41.51", "Sepsis due to Escherichia coli [E. coli]",                       1.60, "IP",    "OTHER"),
    ("R65.20", "Severe sepsis without septic shock",                             1.20, "IP",    "OTHER"),
    ("R65.21", "Severe sepsis with septic shock",                                0.70, "IP",    "OTHER"),
    ("I50.9",  "Heart failure, unspecified",                                     2.10, "IP",    "HF"),
    ("I50.23", "Acute on chronic systolic (congestive) heart failure",           1.10, "IP",    "HF"),
    ("I50.33", "Acute on chronic diastolic (congestive) heart failure",          0.90, "IP",    "HF"),
    ("J44.1",  "Chronic obstructive pulmonary disease with (acute) exacerbation",2.00, "IP",    "COPD"),
    ("J44.0",  "Chronic obstructive pulmonary disease with (acute) lower respiratory infection", 0.85, "IP", "COPD"),
    ("J18.9",  "Pneumonia, unspecified organism",                                2.70, "IP",    "PN"),
    ("J15.9",  "Unspecified bacterial pneumonia",                                0.80, "IP",    "PN"),
    ("I21.4",  "Non-ST elevation (NSTEMI) myocardial infarction",                1.40, "IP",    "AMI"),
    ("I21.3",  "ST elevation (STEMI) myocardial infarction of unspecified site", 0.65, "IP",    "AMI"),
    ("I21.A1", "Myocardial infarction type 2",                                   0.35, "IP",    "AMI"),
    ("E11.65", "Type 2 diabetes mellitus with hyperglycemia",                    2.40, "BOTH",  "OTHER"),
    ("E11.22", "Type 2 diabetes mellitus with diabetic chronic kidney disease",  0.90, "IP",    "OTHER"),
    ("N17.9",  "Acute kidney failure, unspecified",                              1.70, "IP",    "OTHER"),
    ("N18.6",  "End stage renal disease",                                        0.80, "IP",    "OTHER"),
    ("I63.9",  "Cerebral infarction, unspecified",                               1.50, "IP",    "OTHER"),
    ("N39.0",  "Urinary tract infection, site not specified",                    1.90, "BOTH",  "OTHER"),
    ("I48.91", "Unspecified atrial fibrillation",                                1.80, "BOTH",  "OTHER"),
    ("J96.01", "Acute respiratory failure with hypoxia",                         1.30, "IP",    "OTHER"),
    ("K92.2",  "Gastrointestinal hemorrhage, unspecified",                       1.10, "IP",    "OTHER"),
    ("A04.72", "Enterocolitis due to Clostridium difficile, not specified as recurrent", 0.55, "IP", "OTHER"),
    ("E86.0",  "Dehydration",                                                    1.60, "BOTH",  "OTHER"),
    ("I10",    "Essential (primary) hypertension",                               2.20, "BOTH",  "OTHER"),
    ("R07.9",  "Chest pain, unspecified",                                        6.50, "ED",    "OTHER"),
    ("R10.9",  "Unspecified abdominal pain",                                     5.80, "ED",    "OTHER"),
]

# ---------------------------------------------------------------------------
# MS-DRG
#
# Codes and titles are real MS-DRGs. THE GROUPING IS NOT.
#
# A real MS-DRG assignment needs the principal diagnosis, all secondary
# diagnoses graded for CC/MCC, any OR procedures, discharge disposition and
# sometimes age or birth weight, resolved through the CMS Definitions Manual
# and the GROUPER software. What is below picks a severity tier at random
# within the correct DRG family for the principal diagnosis. That is enough to
# populate drg_code, exercise a DRG dimension and give case-mix a plausible
# spread -- and it is NOT a substitute for the grouper.
#
# relative_weight: [ASSUMPTION] approximate, and weights are revised every
#   federal fiscal year. Do not quote these as CMS figures. The real table is
#   the FY MS-DRG relative weight file published with the IPPS final rule.
#
# Tuple: (drg_code, description, relative_weight, tier)
#   tier: MCC = major complication, CC = complication, NONE = neither
# ---------------------------------------------------------------------------

MSDRG_FAMILIES = {
    # principal-diagnosis family -> severity tiers
    "SEPSIS": [
        ("871", "Septicemia or severe sepsis without MV >96 hours with MCC", 1.85, "MCC"),
        ("872", "Septicemia or severe sepsis without MV >96 hours without MCC", 1.05, "NONE"),
    ],
    "HF": [
        ("291", "Heart failure and shock with MCC", 1.40, "MCC"),
        ("292", "Heart failure and shock with CC", 0.90, "CC"),
        ("293", "Heart failure and shock without CC/MCC", 0.62, "NONE"),
    ],
    "COPD": [
        ("190", "Chronic obstructive pulmonary disease with MCC", 1.18, "MCC"),
        ("191", "Chronic obstructive pulmonary disease with CC", 0.90, "CC"),
        ("192", "Chronic obstructive pulmonary disease without CC/MCC", 0.71, "NONE"),
    ],
    "PN": [
        ("193", "Simple pneumonia and pleurisy with MCC", 1.38, "MCC"),
        ("194", "Simple pneumonia and pleurisy with CC", 0.93, "CC"),
        ("195", "Simple pneumonia and pleurisy without CC/MCC", 0.69, "NONE"),
    ],
    "AMI": [
        ("280", "Acute myocardial infarction, discharged alive with MCC", 1.70, "MCC"),
        ("281", "Acute myocardial infarction, discharged alive with CC", 1.02, "CC"),
        ("282", "Acute myocardial infarction, discharged alive without CC/MCC", 0.75, "NONE"),
    ],
    "DIABETES": [
        ("637", "Diabetes with MCC", 1.32, "MCC"),
        ("638", "Diabetes with CC", 0.82, "CC"),
        ("639", "Diabetes without CC/MCC", 0.58, "NONE"),
    ],
    "RENAL": [
        ("682", "Renal failure with MCC", 1.55, "MCC"),
        ("683", "Renal failure with CC", 0.94, "CC"),
        ("684", "Renal failure without CC/MCC", 0.62, "NONE"),
    ],
    "STROKE": [
        ("064", "Intracranial hemorrhage or cerebral infarction with MCC", 1.75, "MCC"),
        ("065", "Intracranial hemorrhage or cerebral infarction with CC", 1.05, "CC"),
        ("066", "Intracranial hemorrhage or cerebral infarction without CC/MCC", 0.78, "NONE"),
    ],
    "UTI": [
        ("689", "Kidney and urinary tract infections with MCC", 1.10, "MCC"),
        ("690", "Kidney and urinary tract infections without MCC", 0.75, "NONE"),
    ],
    "ARRHYTHMIA": [
        ("308", "Cardiac arrhythmia and conduction disorders with MCC", 1.12, "MCC"),
        ("309", "Cardiac arrhythmia and conduction disorders with CC", 0.75, "CC"),
        ("310", "Cardiac arrhythmia and conduction disorders without CC/MCC", 0.55, "NONE"),
    ],
    "RESP_FAILURE": [
        ("189", "Pulmonary edema and respiratory failure", 1.20, "MCC"),
    ],
    "GI_BLEED": [
        ("377", "Gastrointestinal hemorrhage with MCC", 1.62, "MCC"),
        ("378", "Gastrointestinal hemorrhage with CC", 0.92, "CC"),
        ("379", "Gastrointestinal hemorrhage without CC/MCC", 0.65, "NONE"),
    ],
    "GI_INFECTION": [
        ("371", "Major gastrointestinal disorders and peritoneal infections with MCC", 1.85, "MCC"),
        ("372", "Major gastrointestinal disorders and peritoneal infections with CC", 1.10, "CC"),
        ("373", "Major gastrointestinal disorders and peritoneal infections without CC/MCC", 0.78, "NONE"),
    ],
    "FLUID_ELECTROLYTE": [
        ("640", "Miscellaneous disorders of nutrition, metabolism, fluids and electrolytes with MCC", 1.00, "MCC"),
        ("641", "Miscellaneous disorders of nutrition, metabolism, fluids and electrolytes without MCC", 0.65, "NONE"),
    ],
    "HYPERTENSION": [
        ("304", "Hypertension with MCC", 1.00, "MCC"),
        ("305", "Hypertension without MCC", 0.60, "NONE"),
    ],
    "CHEST_PAIN": [
        ("313", "Chest pain", 0.55, "NONE"),
    ],
    "OTHER_MEDICAL": [
        ("947", "Signs and symptoms with MCC", 1.00, "MCC"),
        ("948", "Signs and symptoms without MCC", 0.65, "NONE"),
    ],
}

# ICD-10-CM principal diagnosis prefix -> MS-DRG family. Longest prefix wins.
ICD_TO_DRG_FAMILY = [
    ("A41", "SEPSIS"), ("R65", "SEPSIS"),
    ("I50", "HF"),
    ("J44", "COPD"),
    ("J18", "PN"), ("J15", "PN"),
    ("I21", "AMI"),
    ("E11.22", "RENAL"), ("E11", "DIABETES"),
    ("N17", "RENAL"), ("N18", "RENAL"),
    ("I63", "STROKE"),
    ("N39", "UTI"),
    ("I48", "ARRHYTHMIA"),
    ("J96", "RESP_FAILURE"),
    ("K92", "GI_BLEED"),
    ("A04", "GI_INFECTION"),
    ("E86", "FLUID_ELECTROLYTE"),
    ("I10", "HYPERTENSION"),
    ("R07", "CHEST_PAIN"),
    ("R10", "OTHER_MEDICAL"),
]


def drg_family_for(icd_code: str) -> str:
    """Longest-prefix match from principal diagnosis to MS-DRG family."""
    best, best_len = "OTHER_MEDICAL", 0
    for prefix, fam in ICD_TO_DRG_FAMILY:
        if icd_code.startswith(prefix) and len(prefix) > best_len:
            best, best_len = fam, len(prefix)
    return best


def all_drgs():
    """Flat, de-duplicated MS-DRG list for the dim_drg reference feed."""
    seen, out = set(), []
    for fam, tiers in MSDRG_FAMILIES.items():
        for code, desc, weight, tier in tiers:
            if code in seen:
                continue
            seen.add(code)
            out.append({"drg_code": code, "drg_description": desc,
                        "relative_weight": weight, "severity_tier": tier,
                        "drg_family": fam, "drg_type": "MED",
                        "weight_source": "synthetic (approximate) — replace with the CMS FY relative weight file"})
    return out


# ---------------------------------------------------------------------------
# MIMIC-IV hosp.admissions value sets — [VERIFIED] verbatim from MIMIC docs.
# ---------------------------------------------------------------------------

ADMISSION_TYPE = [
    "AMBULATORY OBSERVATION", "DIRECT EMER.", "DIRECT OBSERVATION", "ELECTIVE",
    "EU OBSERVATION", "EW EMER.", "OBSERVATION ADMIT",
    "SURGICAL SAME DAY ADMISSION", "URGENT",
]

ADMISSION_LOCATION = [
    "PHYSICIAN REFERRAL", "WALK-IN/SELF REFERRAL", "AMBULATORY SURGERY TRANSFER",
    "INFORMATION NOT AVAILABLE", "CLINIC REFERRAL", "PROCEDURE SITE", "PACU",
    "TRANSFER FROM HOSPITAL", "TRANSFER FROM SKILLED NURSING FACILITY",
    "EMERGENCY ROOM", "INTERNAL TRANSFER TO OR FROM PSYCH",
]

DISCHARGE_LOCATION = [
    "HOME", "ACUTE HOSPITAL", "SKILLED NURSING FACILITY", "ASSISTED LIVING",
    "HEALTHCARE FACILITY", "HOME HEALTH CARE", "AGAINST ADVICE", "DIED",
    "OTHER FACILITY", "HOSPICE", "REHAB", "CHRONIC/LONG TERM ACUTE CARE",
    "PSYCH FACILITY",
]
# [ASSUMPTION] distribution over the verified value set
DISCHARGE_LOCATION_WEIGHTS = [
    0.560, 0.022, 0.118, 0.020, 0.014, 0.106, 0.011, 0.024,
    0.010, 0.021, 0.058, 0.014, 0.022,
]

TRANSFER_EVENTTYPE = ["ed", "admit", "transfer", "discharge"]   # [VERIFIED]

# MIMIC hosp.services curr_service — [VERIFIED]
HOSPITAL_SERVICE = [
    "CMED", "CSURG", "DENT", "ENT", "EYE", "GU", "GYN", "MED", "NB", "NBB",
    "NMED", "NSURG", "OBS", "ORTHO", "OMED", "PSURG", "PSYCH", "SURG",
    "TRAUM", "TSURG", "VSURG",
]

# ---------------------------------------------------------------------------
# HL7 / FHIR value sets — [VERIFIED]
# ---------------------------------------------------------------------------

# FHIR v3 ActEncounterCode, all 11 codes
ACT_ENCOUNTER_CODE = {
    "AMB": "ambulatory", "EMER": "emergency", "FLD": "field", "HH": "home health",
    "IMP": "inpatient encounter", "ACUTE": "inpatient acute",
    "NONAC": "inpatient non-acute", "OBSENC": "observation encounter",
    "PRENC": "pre-admission", "SS": "short stay", "VR": "virtual",
}

# FHIR EncounterStatus
ENCOUNTER_STATUS = [
    "planned", "arrived", "triaged", "in-progress", "onleave", "finished",
    "cancelled", "entered-in-error", "unknown",
]

# HL7 v2 table 0004 Patient Class — complete
PATIENT_CLASS = {
    "E": "Emergency", "I": "Inpatient", "O": "Outpatient", "P": "Preadmit",
    "R": "Recurring patient", "B": "Obstetrics", "C": "Commercial Account",
    "N": "Not Applicable", "U": "Unknown",
}

# HL7 v2 table 0007 Admission Type — complete, 7 codes
ADMISSION_TYPE_CODE = {
    "A": "Accident", "E": "Emergency", "L": "Labor and Delivery", "R": "Routine",
    "N": "Newborn", "U": "Urgent", "C": "Elective",
}

# Synthea EncounterClass enum — [VERIFIED] from Synthea source
ENCOUNTER_CLASS = [
    "wellness", "ambulatory", "outpatient", "inpatient", "emergency",
    "urgentcare", "hospice", "home", "snf", "virtual",
]

# FHIR AdministrativeGender
GENDER = ["male", "female", "other", "unknown"]

# MIMIC-IV-ED arrival_transport / disposition — [VERIFIED] shape
ED_DISPOSITION = ["HOME", "ADMITTED", "TRANSFER", "LEFT WITHOUT BEING SEEN",
                  "ELOPED", "LEFT AGAINST MEDICAL ADVICE", "EXPIRED", "OTHER"]

# ---------------------------------------------------------------------------
# Claims code sets
# ---------------------------------------------------------------------------

# [VERIFIED] CMS Blue Button CLM_FAC_TYPE_CD (Type of Bill digit 1)
TOB_FACILITY_TYPE = {
    "1": "Hospital", "2": "Skilled Nursing Facility", "3": "Home Health Agency",
    "4": "Religious Non-medical (hospital)", "6": "Intermediate Care",
    "7": "Clinic services or hospital-based renal dialysis facility",
    "8": "Ambulatory Surgery Center or other special facility",
}

# [VERIFIED] CMS Blue Button CLM_SRVC_CLSFCTN_TYPE_CD (digit 2), facility types 1-6,9
TOB_SERVICE_CLASSIFICATION = {
    "1": "Inpatient", "2": "Inpatient or Home Health (covered on Part B)",
    "3": "Outpatient (or HHA - covered on Part A)", "4": "Other (Part B)",
    "5": "Intermediate care - level I", "6": "Intermediate care - level II",
    "7": "Subacute Inpatient", "8": "Swing bed",
}

# [VERIFIED] CMS Blue Button CLM_FREQ_CD (digit 3)
TOB_FREQUENCY = {
    "0": "Non-payment/zero claims", "1": "Admit thru discharge claim",
    "2": "Interim - first claim", "3": "Interim - continuing claim",
    "4": "Interim - last claim", "5": "Late charge(s) only claim",
    "7": "Replacement of prior claim", "8": "Void/cancel prior claim",
    "9": "Final claim",
}

# [VERIFIED] CMS Blue Button CLM_SRC_IP_ADMSN_CD (point of origin / admission source)
ADMISSION_SOURCE_CODE = {
    "1": "Non-Health Care Facility Point of Origin (Physician Referral)",
    "2": "Clinic referral", "4": "Transfer from hospital (Different Facility)",
    "5": "Transfer from a SNF or Intermediate Care Facility",
    "6": "Transfer from another health care facility", "7": "Emergency room",
    "8": "Court/law enforcement", "9": "Information not available",
    "D": "Transfer from hospital inpatient in the same facility",
    "E": "Transfer from Ambulatory Surgical Center",
    "F": "Transfer from hospice and is under a hospice plan of care",
}

# [VERIFIED] UB-04 FL17 Patient Discharge Status (free CMS/Noridian published list)
PATIENT_STATUS_CODE = {
    "01": "Discharged to home or self-care (routine discharge)",
    "02": "Discharged/transferred to a short-term general hospital for inpatient care",
    "03": "Discharged/transferred to skilled nursing facility (SNF) with Medicare certification",
    "04": "Discharged/transferred to a facility that provides custodial or supportive care",
    "05": "Discharged/transferred to a designated cancer center or children's hospital",
    "06": "Discharged/transferred to home under care of organized home health service organization",
    "07": "Left against medical advice or discontinued care",
    "09": "Admitted as an inpatient to this hospital",
    "20": "Expired",
    "30": "Still a patient",
    "41": "Expired in a medical facility",
    "43": "Discharged/transferred to a federal health care facility",
    "50": "Hospice - home",
    "51": "Hospice - medical facility (certified) providing hospice level of care",
    "62": "Discharged/transferred to an inpatient rehabilitation facility (IRF)",
    "63": "Discharged/transferred to a Medicare certified long term care hospital (LTCH)",
    "65": "Discharged/transferred to a psychiatric hospital or psychiatric distinct part unit",
    "66": "Discharged/transferred to a critical access hospital (CAH)",
}

# Maps MIMIC discharge_location -> UB-04 patient status code
DISCHARGE_TO_STATUS_CODE = {
    "HOME": "01", "ACUTE HOSPITAL": "02", "SKILLED NURSING FACILITY": "03",
    "ASSISTED LIVING": "04", "HEALTHCARE FACILITY": "04",
    "HOME HEALTH CARE": "06", "AGAINST ADVICE": "07", "DIED": "20",
    "OTHER FACILITY": "43", "HOSPICE": "51", "REHAB": "62",
    "CHRONIC/LONG TERM ACUTE CARE": "63", "PSYCH FACILITY": "65",
}

# [VERIFIED] X12 element 1029, restricted to the HIPAA 835 TR3 subset.
# CLP02 = 4 is the denial flag; 22 is the takeback/reversal flag.
CLAIM_STATUS_CODE = {
    "1": "Processed as Primary", "2": "Processed as Secondary",
    "3": "Processed as Tertiary", "4": "Denied", "5": "Pended",
    "19": "Processed as Primary, Forwarded to Additional Payer(s)",
    "20": "Processed as Secondary, Forwarded to Additional Payer(s)",
    "21": "Processed as Tertiary, Forwarded to Additional Payer(s)",
    "22": "Reversal of Previous Payment",
    "23": "Not Our Claim, Forwarded to Additional Payer(s)",
    "25": "Predetermination Pricing Only - No Payment",
}

# [VERIFIED] X12 Claim Adjustment Group Codes. CR has been deleted.
# PI is not used by Medicare.
ADJUSTMENT_GROUP_CODE = {
    "CO": "Contractual Obligation", "PR": "Patient Responsibility",
    "OA": "Other Adjustment", "PI": "Payer Initiated Reductions",
}

# [VERIFIED] X12 CARC descriptions (list last modified 03/01/2025).
# group is the group code X12 pairs the reason with.
# is_denial distinguishes a true denial from a contractual write-off -- a
# distinction analysts routinely get wrong, and one our Gold layer must respect.
CARC = [
    # code, group, description, is_denial, weight
    ("16",  "CO", "Claim/service lacks information or has submission/billing error(s).", True,  27.0),
    ("50",  "CO", 'These are non-covered services because this is not deemed a "medical necessity" by the payer.', True, 9.5),
    ("197", "CO", "Precertification/authorization/notification/pre-treatment absent.", True, 9.0),
    ("198", "CO", "Precertification/notification/authorization/pre-treatment exceeded.", True, 3.0),
    ("29",  "CO", "The time limit for filing has expired.", True, 4.5),
    ("96",  "CO", "Non-covered charge(s).", True, 6.0),
    ("109", "CO", "Claim/service not covered by this payer/contractor. You must send the claim/service to the correct payer/contractor.", True, 5.5),
    ("11",  "CO", "The diagnosis is inconsistent with the procedure.", True, 4.0),
    ("181", "CO", "Procedure code was invalid on the date of service.", True, 2.5),
    ("146", "CO", "Diagnosis was invalid for the date(s) of service reported.", True, 2.0),
    ("252", "CO", "An attachment/other documentation is required to adjudicate this claim/service.", True, 5.0),
    ("227", "CO", "Information requested from the patient/insured/responsible party was not provided or was insufficient/incomplete.", True, 4.0),
    ("204", "CO", "This service/equipment/drug is not covered under the patient's current benefit plan", True, 4.5),
    ("31",  "CO", "Patient cannot be identified as our insured.", True, 6.5),
    ("27",  "CO", "Expenses incurred after coverage terminated.", True, 3.5),
    ("39",  "CO", "Services denied at the time authorization/pre-certification was requested.", True, 2.0),
    ("18",  "OA", "Exact duplicate claim/service", True, 5.5),
    # Contractual write-offs -- NOT denials
    ("45",  "CO", "Charge exceeds fee schedule/maximum allowable or contracted/legislated fee arrangement.", False, 0.0),
    ("97",  "CO", "The benefit for this service is included in the payment/allowance for another service/procedure that has already been adjudicated.", False, 0.0),
    ("24",  "CO", "Charges are covered under a capitation agreement/managed care plan.", False, 0.0),
    ("23",  "OA", "The impact of prior payer(s) adjudication including payments and/or adjustments.", False, 0.0),
    # Patient responsibility
    ("1",   "PR", "Deductible Amount", False, 0.0),
    ("2",   "PR", "Coinsurance Amount", False, 0.0),
    ("3",   "PR", "Co-payment Amount", False, 0.0),
]
# NOTE: CARC 15 was deactivated 05/01/2018 and is deliberately absent.

DENIAL_CARCS = [(c, g, d, w) for c, g, d, isd, w in CARC if isd]

# [VERIFIED] X12 RARC
RARC = {
    "MA130": "Your claim contains incomplete and/or invalid information, and no appeal rights are afforded because the claim is unprocessable.",
    "MA04":  "Secondary payment cannot be considered without the identity of or payment information from the primary payer.",
    "M127":  "Missing patient medical record for this service.",
}

# [VERIFIED] revenue code families, free CMS/Noridian published list
REVENUE_CODES = {
    "0111": "Room and Board Private (one bed) - Medical/Surgical/Gyn",
    "0121": "Room and Board Semi-private (two beds) - Medical/Surgical/Gyn",
    "0200": "Intensive Care Unit - General Classification",
    "0210": "Coronary Care Unit - General Classification",
    "0250": "Pharmacy - General Classification",
    "0251": "Pharmacy - Generic Drugs",
    "0258": "Pharmacy - IV Solutions",
    "0300": "Laboratory - General Classification",
    "0320": "Radiology Diagnostic - General Classification",
    "0360": "Operating Room Services - General Classification",
    "0370": "Anesthesia - General Classification",
    "0410": "Respiratory Services - General Classification",
    "0420": "Physical Therapy - General Classification",
    "0450": "Emergency Room - General Classification",
    "0460": "Pulmonary Function - General Classification",
    "0730": "EKG/ECG - General Classification",
    "0800": "Inpatient Renal Dialysis - General Classification",
    "0900": "Behavioral Health Treatments/Services - General Classification",
}

# [VERIFIED] HCPCS Level II is free from CMS and appears on genuine hospital
# claims. CPT (HCPCS Level I) is AMA-copyright and deliberately NOT used here.
HCPCS_SEED = {
    "J1200": "Injection, diphenhydramine hcl, up to 50 mg",
    "J2405": "Injection, ondansetron hydrochloride, per 1 mg",
    "J1644": "Injection, heparin sodium, per 1000 units",
    "J0690": "Injection, cefazolin sodium, 500 mg",
    "J3370": "Injection, vancomycin hcl, 500 mg",
    "J2543": "Injection, piperacillin/tazobactam, 1.125 g",
    "J1100": "Injection, dexamethasone sodium phosphate, 1 mg",
    "J2270": "Injection, morphine sulfate, up to 10 mg",
    "J3010": "Injection, fentanyl citrate, 0.1 mg",
    "J2250": "Injection, midazolam hydrochloride, per 1 mg",
    "J1170": "Injection, hydromorphone, up to 4 mg",
    "A4216": "Sterile water, saline and/or dextrose, diluent/flush, 10 ml",
    "G0378": "Hospital observation service, per hour",
    "Q9967": "Low osmolar contrast material, 300-399 mg/ml iodine, per ml",
}


def load_icd10():
    """Full CMS ICD-10-CM order file if downloaded, else the verified seed."""
    path = os.path.join(REFDATA_DIR, "icd10cm_codes.csv")
    if os.path.exists(path):
        rows = []
        with open(path, newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                rows.append((r["code"], r["long_title"],
                             float(r.get("weight", 0.1) or 0.1),
                             r.get("setting", "BOTH"),
                             r.get("hrrp_cohort", "OTHER")))
        if rows:
            return rows
    return ICD10_SEED


def load_ndc_products():
    """
    FDA product.txt if downloaded, else None (generator synthesises NDC-shaped
    codes from the formulary).

    NOTE: verify the live header row casing before trusting this. FDA docs
    render CamelCase (StrengthNumber) but the actual TSV appears to be
    UPPERCASE, and the historical names were ACTIVE_NUMERATOR_STRENGTH /
    ACTIVE_INGRED_UNIT. See the verify-before-coding checklist.
    """
    path = os.path.join(REFDATA_DIR, "product.txt")
    if not os.path.exists(path):
        return None
    with open(path, newline="", encoding="latin-1") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))
