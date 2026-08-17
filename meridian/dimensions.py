"""
Conformed dimensions — the join spine.

These are generated once, are stable for the whole run, and are the keys the
platform's referential-integrity DQ checks will test. Get these right and every
fact table lines up; get them wrong and the Gold layer cannot be fixed.
"""

import hashlib
import math
import random
from dataclasses import dataclass, field, asdict

from faker import Faker

from . import config as C
from . import refdata as R


def _ndc11(labeler: int, product: int, package: int) -> str:
    """11-digit HIPAA 5-4-2 NDC, no dashes — the format RxNorm stores."""
    return f"{labeler:05d}{product:04d}{package:02d}"


def _rxcui(name: str, tty: str) -> str:
    """
    Deterministic pseudo-RXCUI. Real RXCUIs must come from the RxNorm
    Prescribable Content release (no licence required) — see refdata.
    Marked in the docs as synthetic until that join is wired up.
    """
    h = hashlib.sha1(f"{tty}|{name}".encode()).hexdigest()
    return str(200000 + int(h[:8], 16) % 1700000)


@dataclass
class Unit:
    unit_id: str
    facility_id: str
    unit_code: str
    unit_name: str
    unit_type: str
    building: str
    floor: int
    licensed_beds: int
    staffed_beds: int
    blocked_beds: int
    nurse_patient_ratio_target: float
    is_critical_care: bool
    is_monitored: bool
    alos_days: float
    alos_sigma: float
    target_occupancy: float


class Dimensions:
    def __init__(self, seed: int = C.SEED):
        self.rng = random.Random(seed)
        self.fake = Faker("en_US")
        Faker.seed(seed)

        self.facilities = list(C.FACILITIES)
        self.unit_types = {u.unit_code: u for u in C.UNIT_TYPES}
        self.units = self._build_units()
        self.units_by_facility = {}
        for u in self.units:
            self.units_by_facility.setdefault(u.facility_id, []).append(u)
        self.units_by_id = {u.unit_id: u for u in self.units}

        self.payers = self._build_payers()
        self.drugs = self._build_drugs()
        self.staff = self._build_staff()
        self.staff_by_unit = {}
        for s in self.staff:
            self.staff_by_unit.setdefault((s["primary_facility_id"], s["primary_unit_id"]), []).append(s)

        self.icd10 = R.load_icd10()
        self._icd_weights = [r[2] for r in self.icd10]
        self._icd_ed = [r for r in self.icd10 if r[3] in ("ED", "BOTH")]
        self._icd_ed_w = [r[2] for r in self._icd_ed]
        self._icd_ip = [r for r in self.icd10 if r[3] in ("IP", "BOTH")]
        self._icd_ip_w = [r[2] for r in self._icd_ip]

        self.patients = []           # populated lazily by the simulation
        self._patient_seq = 0

    # -- units ------------------------------------------------------------
    def _build_units(self):
        units = []
        for f in self.facilities:
            allowed = C.UNITS_BY_FACILITY_TYPE[f.facility_type]
            base_occ = C.TARGET_OCCUPANCY[f.facility_type]
            for code in allowed:
                ut = self.unit_types[code]
                if code == "ED":
                    # ED beds sized from arrival volume, not bed share
                    licensed = max(6, int(f.ed_arrivals_per_day * 0.22))
                elif code in C.OUTPATIENT_UNIT_CODES:
                    # Outpatient units hold no inpatient beds, so bed_share is 0
                    # and the licensed-bed formula would size them out of
                    # existence. Size them as exam rooms / procedure rooms from
                    # appointment volume instead.
                    appts = f.ed_arrivals_per_day * C.OUTPATIENT_PER_ED_ARRIVAL
                    if code == "ASC":
                        if f.licensed_beds == 0:
                            continue        # no surgery at an urgent care centre
                        licensed = max(2, int(appts * C.OUTPATIENT_ASC_SHARE / 6))
                    else:
                        licensed = max(4, int(appts / 14))
                else:
                    licensed = int(round(f.licensed_beds * ut.bed_share))
                    if licensed < ut.min_beds:
                        licensed = ut.min_beds
                    if licensed == 0:
                        continue
                staffed = max(1, int(round(licensed * self.rng.uniform(*C.STAFFED_BED_FRACTION))))
                blocked = int(round(licensed * self.rng.uniform(*C.BLOCKED_BED_FRACTION)))
                occ = base_occ + (C.ICU_OCCUPANCY_UPLIFT if ut.is_critical_care else 0.0)
                occ = min(occ, 0.97)
                units.append(Unit(
                    unit_id=f"{f.facility_id}-{code}",
                    facility_id=f.facility_id,
                    unit_code=code,
                    unit_name=f"{ut.unit_type} ({code})",
                    unit_type=ut.unit_type,
                    building="Main" if self.rng.random() < 0.8 else "North Tower",
                    floor=self.rng.randint(1, 9),
                    licensed_beds=licensed,
                    staffed_beds=staffed,
                    blocked_beds=blocked,
                    nurse_patient_ratio_target=ut.nurse_ratio,
                    is_critical_care=ut.is_critical_care,
                    is_monitored=ut.monitored,
                    alos_days=ut.alos_days,
                    alos_sigma=ut.alos_sigma,
                    target_occupancy=occ,
                ))
        return units

    def inpatient_units(self, facility_id):
        """Units that hold admitted patients — excludes ED and outpatient."""
        return [u for u in self.units_by_facility.get(facility_id, [])
                if u.unit_code != "ED" and u.unit_code not in C.OUTPATIENT_UNIT_CODES]

    def outpatient_units(self, facility_id):
        return [u for u in self.units_by_facility.get(facility_id, [])
                if u.unit_code in C.OUTPATIENT_UNIT_CODES]

    # -- payers -----------------------------------------------------------
    def _build_payers(self):
        out = []
        for pid, name, ptype, cfi, share, prompt in C.PAYERS:
            out.append({
                "payer_id": pid, "payer_name": name, "payer_type": ptype,
                "claim_filing_indicator_code": cfi, "share": share,
                "prompt_pay_days": prompt,
            })
        return out

    def pick_payer(self):
        return self.rng.choices(self.payers, weights=[p["share"] for p in self.payers])[0]

    # -- drugs ------------------------------------------------------------
    def _build_drugs(self):
        drugs = []
        ndc_products = R.load_ndc_products()
        for i, (name, form, route, cls, dea, dea_code, weight,
                cost, shortage_prone, high_alert) in enumerate(C.FORMULARY):
            labeler = 10000 + (i * 37) % 60000
            ndc = _ndc11(labeler, 1000 + i, 30)
            gtin = "003" + ndc[:10] + str((i * 7) % 10)
            drugs.append({
                "rxcui_scd": _rxcui(name, "SCD"),
                "rxcui_in": _rxcui(name.split()[0], "IN"),
                "ndc11": ndc,
                "product_ndc": f"{labeler:05d}-{1000+i:04d}",
                "gtin14": gtin[:14],
                "proprietary_name": name.split()[0].title(),
                "non_proprietary_name": name,
                "dosage_form_name": form,
                "route_name": route,
                "pharm_classes": cls,
                "dea_schedule": dea,
                "dea_drug_code": dea_code,
                "is_controlled": dea is not None,
                "labeler_name": f"{self.fake.company()[:40]} Pharmaceuticals",
                "unit_cost": cost,
                "usage_weight": weight,
                "is_shortage_prone": shortage_prone,
                "is_high_alert": high_alert,
                "ndc_source": "FDA product.txt" if ndc_products else "synthetic (NDC-shaped)",
            })
        # ABC classification by annual value share
        total = sum(d["usage_weight"] * d["unit_cost"] for d in drugs)
        ranked = sorted(drugs, key=lambda d: -(d["usage_weight"] * d["unit_cost"]))
        cum = 0.0
        for d in ranked:
            cum += (d["usage_weight"] * d["unit_cost"]) / total
            d["abc_class"] = "A" if cum <= C.ABC_THRESHOLDS[0] else ("B" if cum <= C.ABC_THRESHOLDS[1] else "C")
        return drugs

    def pick_drug(self):
        return self.rng.choices(self.drugs, weights=[d["usage_weight"] for d in self.drugs])[0]

    # -- staff ------------------------------------------------------------
    JOB_MIX = [  # job_code, job_title, credential, share, licensed_nurse
        ("RN",       "Registered Nurse",            "RN",     0.46, True),
        ("LPN",      "Licensed Practical Nurse",    "LPN",    0.07, True),
        ("CNA",      "Certified Nursing Assistant", "CNA",    0.17, False),
        ("MD",       "Physician",                   "MD",     0.09, False),
        ("NP",       "Nurse Practitioner",          "NP",     0.03, True),
        ("PA",       "Physician Assistant",         "PA-C",   0.02, False),
        ("RT",       "Respiratory Therapist",       "RRT",    0.05, False),
        ("RPh",      "Pharmacist",                  "PharmD", 0.03, False),
        ("PharmTech","Pharmacy Technician",         "CPhT",   0.03, False),
        ("UC",       "Unit Clerk",                  None,     0.05, False),
    ]

    def _build_staff(self):
        staff = []
        seq = 0
        for f in self.facilities:
            units = self.units_by_facility.get(f.facility_id, [])
            beds = max(sum(u.licensed_beds for u in units), 12)
            headcount = int(beds * C.STAFF_PER_BED)
            for _ in range(headcount):
                seq += 1
                job = self.rng.choices(self.JOB_MIX, weights=[j[3] for j in self.JOB_MIX])[0]
                unit = self.rng.choice(units)
                hire_days_ago = self.rng.randint(30, 4200)
                terminated = self.rng.random() < 0.11   # [ASSUMPTION] turnover
                # Termination must postdate the hire by at least a probation
                # period. Drawing the two offsets independently previously
                # produced ~21 staff whose termination_date was on or before
                # their hire_date -- a temporal inversion that was never
                # recorded in the answer key, so a DQ check would flag rows
                # nobody could account for.
                term_offset = None
                if terminated:
                    latest = hire_days_ago - C.MIN_EMPLOYMENT_DAYS
                    if latest <= 0:
                        terminated = False          # hired too recently to have left
                    else:
                        term_offset = -self.rng.randint(0, min(400, latest))
                staff.append({
                    "staff_id": f"STF{seq:06d}",
                    "npi": f"{self.rng.randint(1000000000, 1999999999)}" if job[4] or job[0] in ("MD","PA","RPh") else None,
                    "first_name": self.fake.first_name(),
                    "last_name": self.fake.last_name(),
                    "job_code": job[0],
                    "job_title": job[1],
                    "credential": job[2],
                    "is_licensed_nurse": job[4],
                    "primary_facility_id": f.facility_id,
                    "primary_unit_id": unit.unit_id,
                    # PBJ EMPLEE_CTR idiom: 1 = Employee, 2 = Contract
                    "employment_type": 2 if self.rng.random() < C.CONTRACT_STAFF_BASE else 1,
                    "fte": self.rng.choice([1.0, 1.0, 1.0, 0.9, 0.8, 0.6]),
                    "hire_date_offset_days": -hire_days_ago,
                    "is_terminated": terminated,
                    "termination_offset_days": term_offset,
                })
        return staff

    # -- staff churn inside the window (SCD-2 material) --------------------
    def apply_staff_churn(self, as_of_offset: int):
        """
        Hire, terminate and transfer a handful of staff per facility per week,
        so successive dim_staff snapshots genuinely differ.

        Without this every weekly snapshot is byte-identical and there is no
        slowly-changing-dimension history for the warehouse to model. Returns
        a list of change records for the run manifest.

        `as_of_offset` is the number of days from the staff epoch to the
        snapshot being built. Events are dated in the week ENDING there, never
        after it -- an event dated in the future is filtered out of the snapshot
        by the emitter and the churn would be invisible.
        """
        changes = []
        if as_of_offset <= 0:
            return changes

        def event_offset():
            """A day inside the week ending at the snapshot."""
            back = self.rng.randint(0, min(6, as_of_offset - 1))
            return as_of_offset - back

        for f in self.facilities:
            units = self.units_by_facility.get(f.facility_id, [])
            if not units:
                continue
            pool = [s for s in self.staff
                    if s["primary_facility_id"] == f.facility_id and not s["is_terminated"]]

            for _ in range(self.rng.randint(*C.STAFF_TERMINATIONS_PER_WEEK)):
                if len(pool) < 20:
                    break
                s = self.rng.choice(pool)
                # terminate inside this week, and never before the hire date
                offset = event_offset()
                if offset <= s["hire_date_offset_days"] + C.MIN_EMPLOYMENT_DAYS:
                    continue
                s["is_terminated"] = True
                s["termination_offset_days"] = offset
                pool.remove(s)
                changes.append(("terminate", s["staff_id"]))

            for _ in range(self.rng.randint(*C.STAFF_HIRES_PER_WEEK)):
                self._staff_seq = getattr(self, "_staff_seq", len(self.staff))
                self._staff_seq += 1
                job = self.rng.choices(self.JOB_MIX, weights=[j[3] for j in self.JOB_MIX])[0]
                unit = self.rng.choice(units)
                rec = {
                    "staff_id": f"STF{self._staff_seq:06d}",
                    "npi": f"{self.rng.randint(1000000000, 1999999999)}" if job[4] or job[0] in ("MD", "PA", "RPh") else None,
                    "first_name": self.fake.first_name(),
                    "last_name": self.fake.last_name(),
                    "job_code": job[0], "job_title": job[1], "credential": job[2],
                    "is_licensed_nurse": job[4],
                    "primary_facility_id": f.facility_id,
                    "primary_unit_id": unit.unit_id,
                    "employment_type": 2 if self.rng.random() < C.CONTRACT_STAFF_BASE else 1,
                    "fte": self.rng.choice([1.0, 1.0, 1.0, 0.9, 0.8, 0.6]),
                    # hired this week
                    "hire_date_offset_days": event_offset(),
                    "is_terminated": False,
                    "termination_offset_days": None,
                }
                self.staff.append(rec)
                self.staff_by_unit.setdefault(
                    (rec["primary_facility_id"], rec["primary_unit_id"]), []).append(rec)
                changes.append(("hire", rec["staff_id"]))

            for _ in range(self.rng.randint(*C.STAFF_UNIT_TRANSFER_PER_WEEK)):
                if len(pool) < 20 or len(units) < 2:
                    break
                s = self.rng.choice(pool)
                old = s["primary_unit_id"]
                new = self.rng.choice([u for u in units if u.unit_id != old])
                bucket = self.staff_by_unit.get((s["primary_facility_id"], old), [])
                if s in bucket:
                    bucket.remove(s)
                s["primary_unit_id"] = new.unit_id
                self.staff_by_unit.setdefault(
                    (s["primary_facility_id"], new.unit_id), []).append(s)
                changes.append(("transfer", s["staff_id"]))
        return changes

    def nurses_for_unit(self, facility_id, unit_id):
        pool = self.staff_by_unit.get((facility_id, unit_id), [])
        return [s for s in pool if s["is_licensed_nurse"]] or pool

    # -- patients ---------------------------------------------------------
    def new_patient(self):
        """
        Synthea patients.csv column names verbatim, plus our additions:
        `mrn` is facility-scoped and deliberately NOT globally unique, so the
        same human appears under different MRNs at different facilities. That
        creates the real identity-matching problem the architecture flags as an
        open question. `enterprise_patient_id` is the answer key — it must not
        reach Bronze.
        """
        self._patient_seq += 1
        sex = self.rng.choice(["male", "female"])
        if self.rng.random() < 0.004:
            sex = self.rng.choice(["other", "unknown"])
        first = self.fake.first_name_male() if sex == "male" else self.fake.first_name_female()
        # Age skewed older — hospital population, not general population
        age = min(101, max(0, int(self.rng.gauss(58, 22))))
        state_fac = self.rng.choice(self.facilities)
        p = {
            "Id": self.fake.uuid4(),
            "BirthDate": None,          # filled by simulation, needs run date
            "_age": age,
            "DeathDate": None,
            "SSN": self.fake.ssn(),
            "Drivers": f"S{self.rng.randint(10000000, 99999999)}" if age >= 16 else None,
            "Passport": f"X{self.rng.randint(10000000, 99999999)}" if self.rng.random() < 0.35 else None,
            "Prefix": ("Mr." if sex == "male" else "Ms.") if age >= 18 else None,
            "First": first,
            "Middle": self.fake.first_name() if self.rng.random() < 0.7 else None,
            "Last": self.fake.last_name(),
            "Suffix": None,
            "Maiden": self.fake.last_name() if sex == "female" and age > 30 and self.rng.random() < 0.4 else None,
            "Marital": self.rng.choice(["M", "S", "D", "W", None]),
            "Race": self.rng.choices(
                ["white", "black", "asian", "native", "other"],
                weights=[0.60, 0.18, 0.08, 0.01, 0.13])[0],
            "Ethnicity": self.rng.choices(["nonhispanic", "hispanic"], weights=[0.81, 0.19])[0],
            "Gender": sex,
            "BirthPlace": f"{self.fake.city()} {state_fac.state} US",
            "Address": self.fake.street_address(),
            "City": state_fac.city,
            "State": state_fac.state,
            "County": state_fac.county,
            "FIPS County Code": f"{self.rng.randint(1001, 56045):05d}",
            "Zip": state_fac.zip,
            "Lat": round(self.fake.latitude().__float__(), 6),
            "Lon": round(self.fake.longitude().__float__(), 6),
            "Healthcare_Expenses": round(self.rng.lognormvariate(10.4, 0.9), 2),
            "Healthcare_Coverage": round(self.rng.lognormvariate(9.6, 1.0), 2),
            "Income": int(self.rng.lognormvariate(10.8, 0.6)),
            # ---- our additions ----
            "phone": self.fake.phone_number(),
            "email": self.fake.email(),
            # [OURS] population-health risk flag. No open standard defines it;
            # real systems compute it from a risk model. Age-weighted here so it
            # correlates with readmission and utilisation rather than being noise.
            "is_high_risk": self.rng.random() < min(0.42, 0.03 + max(0, age - 45) * 0.006),
            "enterprise_patient_id": f"EPI{self._patient_seq:08d}",
            "mrn_by_facility": {},
        }
        self.patients.append(p)
        return p

    def mrn_for(self, patient, facility_id):
        if facility_id not in patient["mrn_by_facility"]:
            patient["mrn_by_facility"][facility_id] = \
                f"{facility_id[:3]}{self.rng.randint(100000, 999999)}"
        return patient["mrn_by_facility"][facility_id]

    # -- diagnoses --------------------------------------------------------
    def pick_diagnosis(self, setting="IP"):
        pool, weights = (self._icd_ip, self._icd_ip_w) if setting == "IP" else (self._icd_ed, self._icd_ed_w)
        return self.rng.choices(pool, weights=weights)[0]
