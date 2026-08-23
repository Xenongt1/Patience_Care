"""
Patient-journey simulation.

This is the heart of the generator, and the reason it is a simulation rather
than seven independent file writers: the same state transition that admits a
patient must also occupy a bed, start a vitals stream, trigger drug orders that
deplete inventory, and eventually produce a coded claim. Generate the sources
independently and the platform's cross-source referential-integrity checks
become meaningless while the dashboards contradict each other.
"""

import math
import random
from collections import defaultdict
from datetime import date, datetime, timedelta, time
from dataclasses import dataclass, field

from . import config as C
from . import refdata as R
from .dimensions import Dimensions


@dataclass
class Encounter:
    encounter_id: str
    patient: dict
    facility_id: str
    encounter_class: str          # Synthea EncounterClass
    act_code: str                 # FHIR v3 ActEncounterCode
    patient_class: str            # HL7 v2 table 0004
    # ED timeline
    ed_arrival: datetime = None
    triage_time: datetime = None
    provider_seen_time: datetime = None
    admit_decision_time: datetime = None
    ed_departure: datetime = None
    esi_acuity: int = None
    chief_complaint: str = None
    arrival_transport: str = None
    ed_disposition: str = None
    triage_vitals: dict = field(default_factory=dict)
    # inpatient timeline
    hadm_id: str = None
    admittime: datetime = None
    dischtime: datetime = None
    deathtime: datetime = None
    admission_type: str = None
    admission_type_code: str = None
    admission_location: str = None
    discharge_location: str = None
    hospital_expire_flag: int = 0
    hospital_service: str = None
    stays: list = field(default_factory=list)     # (unit, intime, outtime)
    diagnoses: list = field(default_factory=list) # (seq, code, title, cohort)
    # outpatient timeline — scheduled appointments, not walk-ins
    appointment_time: datetime = None
    outpatient_unit: object = None
    is_no_show: bool = False
    # billing
    drg_code: str = None
    drg_weight: float = None
    # linkage
    payer: dict = None
    is_readmission: bool = False
    is_planned_readmission: bool = False
    index_encounter_id: str = None
    transferred_in_within_6h: bool = False
    prescriptions: list = field(default_factory=list)

    @property
    def is_inpatient(self):
        return self.admittime is not None

    @property
    def is_outpatient(self):
        return self.appointment_time is not None

    @property
    def los_days(self):
        if self.admittime and self.dischtime:
            return (self.dischtime - self.admittime).total_seconds() / 86400.0
        return None

    @property
    def principal_diagnosis(self):
        return self.diagnoses[0] if self.diagnoses else None


class BedState:
    """Tracks live occupancy per unit so capacity actually constrains flow."""

    def __init__(self, dims: Dimensions):
        self.dims = dims
        self.occupied = {u.unit_id: 0 for u in dims.units}
        self.pending_admissions = {u.unit_id: 0 for u in dims.units}
        self.pending_discharges = {u.unit_id: 0 for u in dims.units}

    def available(self, unit):
        return max(0, unit.staffed_beds - unit.blocked_beds - self.occupied[unit.unit_id])

    def occupancy(self, unit):
        denom = unit.staffed_beds - unit.blocked_beds
        if denom <= 0:
            return 0.0
        return self.occupied[unit.unit_id] / denom

    def admit(self, unit):
        self.occupied[unit.unit_id] += 1

    def release(self, unit):
        self.occupied[unit.unit_id] = max(0, self.occupied[unit.unit_id] - 1)


class Simulation:
    def __init__(self, start_date: date, days: int, dims: Dimensions = None,
                 seed: int = C.SEED, patient_reuse: float = 0.62):
        self.rng = random.Random(seed + 1)
        self.dims = dims or Dimensions(seed)
        self.start_date = start_date
        self.days = days
        self.end_date = start_date + timedelta(days=days)
        self.beds = BedState(self.dims)
        self.patient_reuse = patient_reuse

        self.encounters = []
        self.active = []               # currently admitted encounters
        self.discharged_index = []     # candidates for readmission
        self._scheduled_returns = []   # (date, patient, cohort, index_id)
        self._seq = 0
        self.snapshots_by_date = defaultdict(list)
        self.daily_census = {}         # (date, unit_id) -> midnight census
        self._boarding = defaultdict(list)   # facility_id -> [(decision, departure)]
        # indexes built once after the run; scanning all encounters per output
        # day is O(days^2) and makes an 18-month run unusable
        self.by_close_date = defaultdict(list)
        self.by_disch_date = defaultdict(list)
        self.active_by_date = defaultdict(list)

    # -- helpers ----------------------------------------------------------
    def _next_id(self, prefix):
        self._seq += 1
        return f"{prefix}{self._seq:09d}"

    def _lognorm_minutes(self, spec):
        median, sigma = spec
        return max(1.0, median * math.exp(self.rng.gauss(0, sigma)))

    def _arrival_count(self, facility, day: date, hour: int):
        base = facility.ed_arrivals_per_day / 24.0
        lam = (base
               * C.DIURNAL_ARRIVAL[hour]
               * C.DOW_ARRIVAL[day.weekday()]
               * C.SEASONAL_ARRIVAL[day.month - 1])
        # Poisson via Knuth (small lambda, fine here)
        l, k, p = math.exp(-lam), 0, 1.0
        while True:
            p *= self.rng.random()
            if p <= l:
                return k
            k += 1
            if k > 200:
                return k

    def _get_patient(self):
        if self.dims.patients and self.rng.random() < self.patient_reuse:
            return self.rng.choice(self.dims.patients)
        return self.dims.new_patient()

    # -- main loop --------------------------------------------------------
    def run(self, progress=None):
        for d in range(self.days):
            day = self.start_date + timedelta(days=d)
            for hour in range(24):
                now = datetime.combine(day, time(hour, 0))
                self._discharge_due(now)
                self._process_arrivals(day, hour, now)
                if hour in (7, 11, 14):
                    self._elective_admissions(day, hour, now)
                self._snapshot_beds(now)
            self._record_midnight_census(day)
            if progress:
                progress(d + 1, self.days)
        # close out anyone still admitted at the horizon
        horizon = datetime.combine(self.end_date, time(0, 0))
        for enc in list(self.active):
            self._discharge(enc, horizon, forced=True)
        self.build_indexes()
        return self.encounters

    def build_indexes(self):
        """One O(N) pass so the emitters never rescan the encounter list."""
        self.by_close_date.clear()
        self.by_disch_date.clear()
        self.active_by_date.clear()
        for e in self.encounters:
            close = e.dischtime or e.ed_departure
            if close:
                self.by_close_date[close.date()].append(e)
            if e.is_inpatient and e.dischtime:
                self.by_disch_date[e.dischtime.date()].append(e)
            for unit, tin, tout in e.stays:
                if tin is None:
                    continue
                end = (tout or datetime.combine(self.end_date, time(0, 0))).date()
                d = tin.date()
                while d <= end:
                    self.active_by_date[d].append(e)
                    d += timedelta(days=1)
        # an encounter with several stays in one day would be listed twice
        for d, lst in self.active_by_date.items():
            seen, uniq = set(), []
            for e in lst:
                if e.encounter_id not in seen:
                    seen.add(e.encounter_id)
                    uniq.append(e)
            self.active_by_date[d] = uniq

    # -- elective / direct admissions -------------------------------------
    ELECTIVE_SERVICE = {
        "MS": ["SURG", "MED", "ORTHO", "GU"], "ONC": ["OMED"],
        "SICU": ["SURG", "TSURG", "VSURG"], "CVICU": ["CSURG", "CMED"],
        "MICU": ["MED", "NMED"], "SDU": ["MED", "CMED", "SURG"],
        "TELE": ["CMED", "MED"], "PACU": ["SURG", "ORTHO", "PSURG"],
        "PEDS": ["MED", "NB"], "LD": ["OBS"], "PP": ["OBS"],
        "NICU": ["NBB"], "PSY": ["PSYCH"], "REHAB": ["MED", "ORTHO"],
    }

    def _elective_admissions(self, day, hour, now):
        """
        Top units up toward their target occupancy. Driving the fill from the
        occupancy gap (rather than a fixed arrival rate) is what lets us land on
        a defensible network occupancy figure and keeps the specialty units
        populated instead of empty.
        """
        weekend = day.weekday() >= 5
        for facility in self.dims.facilities:
            for unit in self.dims.inpatient_units(facility.facility_id):
                denom = unit.staffed_beds - unit.blocked_beds
                if denom <= 0:
                    continue
                # jitter the target per unit-day so census breathes
                jitter = self.rng.gauss(0, C.OCCUPANCY_DAILY_JITTER)
                seasonal = C.SEASONAL_ARRIVAL[day.month - 1]
                target = min(C.OCCUPANCY_TARGET_CAP,
                             unit.target_occupancy * seasonal + jitter)
                gap = target - self.beds.occupancy(unit)
                if gap <= 0:
                    continue
                # elective volume collapses at weekends, as in real hospitals
                fill = gap * denom * (0.10 if weekend else 0.30)
                n = int(fill) + (1 if self.rng.random() < (fill % 1) else 0)
                for _ in range(min(n, self.beds.available(unit))):
                    enc = Encounter(
                        encounter_id=self._next_id("ENC"),
                        patient=self._get_patient(),
                        facility_id=facility.facility_id,
                        encounter_class="inpatient", act_code="IMP",
                        patient_class="I",
                        esi_acuity=self.rng.choices([2, 3, 4], weights=[.2, .55, .25])[0],
                    )
                    enc.payer = self.dims.pick_payer()
                    arrival = now.replace(minute=self.rng.randint(0, 59))
                    self._admit(enc, unit, arrival)
                    enc.hospital_service = self.rng.choice(
                        self.ELECTIVE_SERVICE.get(unit.unit_code, ["MED"]))
                    self.encounters.append(enc)

    def _process_arrivals(self, day, hour, now):
        for facility in self.dims.facilities:
            # scheduled readmissions and elective admissions first
            self._release_scheduled_returns(facility, day, hour, now)
            self._outpatient_appointments(facility, day, hour, now)
            # An urgent care centre has no emergency department in the CMS sense
            # -- has_ed drives dim_facility.emergency_services -- but it very
            # much has walk-in arrivals. Gating on has_ed alone meant facility
            # 450601 produced zero encounters of any kind.
            if not facility.has_ed and facility.facility_type != "Urgent Care":
                continue
            n = self._arrival_count(facility, day, hour)
            for _ in range(n):
                minute = self.rng.randint(0, 59)
                arrival = now.replace(minute=minute)
                self._ed_arrival(facility, arrival)

    # -- outpatient -------------------------------------------------------
    def _outpatient_appointment_count(self, facility, day, hour):
        w = C.OUTPATIENT_HOUR_WEIGHTS.get(hour)
        if not w:
            return 0                     # clinics are shut
        daily = facility.ed_arrivals_per_day * C.OUTPATIENT_PER_ED_ARRIVAL
        lam = daily * w * C.OUTPATIENT_DOW_WEIGHTS[day.weekday()]
        lam *= C.SEASONAL_ARRIVAL[day.month - 1]
        # Poisson via Knuth, same as ED arrivals
        l, k, p = math.exp(-lam), 0, 1.0
        while True:
            p *= self.rng.random()
            if p <= l:
                return k
            k += 1
            if k > 400:
                return k

    def _outpatient_appointments(self, facility, day, hour, now):
        """
        Scheduled clinic and same-day-surgery visits.

        These exist because the client names outpatient wait times as one of two
        stated wait-time problems, and because outpatient is the bulk of a real
        network's visit volume. The measure here is appointment time -> provider
        seen, which is a different thing from the ED measures: the clock starts
        at the scheduled time, not at arrival, and a patient who turns up early
        does not get seen early.
        """
        units = self.dims.outpatient_units(facility.facility_id)
        if not units:
            return
        n = self._outpatient_appointment_count(facility, day, hour)
        if not n:
            return
        clinic = [u for u in units if u.unit_code == "OPC"]
        surgical = [u for u in units if u.unit_code == "ASC"]
        # Session load drives the wait: a clinic running 30 appointments an hour
        # against 8 rooms slips, and that slip is the finding.
        rooms = max(1, sum(u.staffed_beds for u in units))
        load = n / rooms

        for _ in range(n):
            use_asc = surgical and self.rng.random() < C.OUTPATIENT_ASC_SHARE
            pool = surgical if use_asc else (clinic or units)
            unit = self.rng.choice(pool)
            appt = now.replace(minute=self.rng.choice([0, 10, 15, 20, 30, 40, 45, 50]))
            patient = self._get_patient()
            enc = Encounter(
                encounter_id=self._next_id("ENC"),
                patient=patient,
                facility_id=facility.facility_id,
                encounter_class="ambulatory" if use_asc else "outpatient",
                act_code="AMB",
                patient_class="O",
                appointment_time=appt,
                outpatient_unit=unit,
            )
            enc.payer = self.dims.pick_payer()
            dx = self.dims.pick_diagnosis("ED" if not use_asc else "IP")
            enc.chief_complaint = dx[1][:60]
            enc.diagnoses.append((1, dx[0], dx[1], dx[4]))

            # no-show: the appointment exists, the patient never arrives
            if self.rng.random() < C.OUTPATIENT_NO_SHOW_RATE:
                enc.is_no_show = True
                enc.ed_disposition = "NO SHOW"
                enc.ed_departure = appt
                self.encounters.append(enc)
                continue

            lo, hi = C.OUTPATIENT_ARRIVAL_OFFSET_MIN
            enc.ed_arrival = appt + timedelta(minutes=self.rng.randint(lo, hi))
            # wait is measured from the appointment, and degrades with load
            delay = self._lognorm_minutes(C.OUTPATIENT_SEEN_DELAY_MIN) * (1.0 + max(0.0, load - 1.0) * 0.6)
            enc.provider_seen_time = appt + timedelta(minutes=delay)
            visit = self._lognorm_minutes(C.OUTPATIENT_VISIT_MIN)
            enc.ed_departure = enc.provider_seen_time + timedelta(minutes=visit)
            enc.ed_disposition = "HOME"

            # a same-day surgery occasionally escalates to a real admission
            if use_asc and self.rng.random() < C.ASC_ADMIT_PROB:
                # ESI is an ED triage score, so an outpatient encounter has none.
                # Escalation involves a clinical assessment, and unit selection
                # needs an acuity, so assign one at that point only.
                enc.esi_acuity = self.rng.choice([2, 3, 3])
                bed = self._choose_unit(facility, enc)
                if bed is not None:
                    enc.admit_decision_time = enc.ed_departure
                    enc.ed_disposition = "ADMITTED"
                    self._admit(enc, bed, enc.ed_departure)
            self.encounters.append(enc)

    # -- ED ---------------------------------------------------------------
    def _ed_arrival(self, facility, arrival, patient=None, forced_cohort=None,
                    is_readmission=False, index_id=None, planned=False):
        patient = patient or self._get_patient()
        enc = Encounter(
            encounter_id=self._next_id("ENC"),
            patient=patient,
            facility_id=facility.facility_id,
            encounter_class="emergency" if facility.has_ed and facility.facility_type != "Urgent Care" else "urgentcare",
            act_code="EMER",
            patient_class="E",
            ed_arrival=arrival,
            is_readmission=is_readmission,
            is_planned_readmission=planned,
            index_encounter_id=index_id,
        )
        enc.payer = self.dims.pick_payer()
        enc.arrival_transport = self.rng.choices(
            list(C.ARRIVAL_TRANSPORT), weights=list(C.ARRIVAL_TRANSPORT.values()))[0]

        # ESI acuity, biased by facility case mix
        weights = [w * (facility.acuity_bias ** (3 - i)) for i, w in enumerate(C.ESI_DISTRIBUTION)]
        enc.esi_acuity = self.rng.choices([1, 2, 3, 4, 5], weights=weights)[0]

        dx = self.dims.pick_diagnosis("ED")
        if forced_cohort and forced_cohort != "OTHER":
            same = [r for r in self.dims.icd10 if r[4] == forced_cohort]
            if same:
                dx = self.rng.choice(same)
        enc.chief_complaint = dx[1][:60]
        enc.diagnoses.append((1, dx[0], dx[1], dx[4]))

        # arrival -> triage
        enc.triage_time = arrival + timedelta(minutes=self._lognorm_minutes(C.TRIAGE_DELAY_MIN))
        enc.triage_vitals = self._triage_vitals(enc)

        # triage -> disposition decision
        workup = self._lognorm_minutes(C.ED_WORKUP_MIN_BY_ESI[enc.esi_acuity])
        decision_at = enc.triage_time + timedelta(minutes=workup)

        # triage -> first physician/APP contact (door-to-doctor). Capped below
        # the decision time: a patient cannot be worked up before being seen.
        seen_delay = self._lognorm_minutes(C.PROVIDER_SEEN_MIN_BY_ESI[enc.esi_acuity])
        enc.provider_seen_time = enc.triage_time + timedelta(
            minutes=min(seen_delay, max(1.0, workup * 0.85)))

        # LWBS / eloped — long waits drive it, which is what makes the KPI real
        if enc.esi_acuity >= 4 and workup > 240 and self.rng.random() < 0.06:
            enc.ed_disposition = "LEFT WITHOUT BEING SEEN"
            enc.ed_departure = decision_at
            # left before a provider ever saw them -- this is the correct null,
            # and it is exactly the case a door-to-doctor average must exclude
            enc.provider_seen_time = None
            self.encounters.append(enc)
            return enc

        admit_prob = C.ADMIT_PROB_BY_ESI[enc.esi_acuity - 1]
        if facility.facility_type == "Urgent Care":
            admit_prob = 0.0
        if is_readmission:
            # a readmission is by definition an inpatient admission; leaving it
            # subject to the ESI admit probability silently divided the
            # readmission rate by ~4
            admit_prob = 1.0
            enc.esi_acuity = min(enc.esi_acuity, self.rng.choice([2, 2, 3]))

        if self.rng.random() < admit_prob:
            enc.admit_decision_time = decision_at
            unit = self._choose_unit(facility, enc)
            if unit is None:
                # no inpatient capacity anywhere -> transfer out
                enc.ed_disposition = "TRANSFER"
                enc.ed_departure = decision_at + timedelta(minutes=self._lognorm_minutes(C.BOARDING_MIN))
                self.encounters.append(enc)
                return enc
            # boarding time inflates when the receiving unit is full.
            # This link is the whole capacity story on the operational dashboard.
            occ = self.beds.occupancy(unit)
            penalty = 1.0 + max(0.0, occ - 0.80) * C.BOARDING_CAPACITY_PENALTY * 5
            board = self._lognorm_minutes(C.BOARDING_MIN) * penalty
            enc.ed_departure = enc.admit_decision_time + timedelta(minutes=board)
            enc.ed_disposition = "ADMITTED"
            self._boarding[facility.facility_id].append(
                (enc.admit_decision_time, enc.ed_departure))
            self._admit(enc, unit, enc.ed_departure)
        else:
            enc.ed_disposition = self.rng.choices(
                ["HOME", "LEFT AGAINST MEDICAL ADVICE", "TRANSFER", "EXPIRED"],
                weights=[0.965, 0.020, 0.013, 0.002])[0]
            enc.ed_departure = decision_at
        self.encounters.append(enc)
        return enc

    def _triage_vitals(self, enc):
        """Triage vitals, degraded with acuity. MIMIC-IV-ED `triage` columns."""
        severity = (6 - enc.esi_acuity) / 5.0
        def v(mean, sd, shift):
            return mean + shift * severity + self.rng.gauss(0, sd)
        return {
            "temperature": round(v(98.2, 0.9, 1.6), 1),      # MIMIC-ED stores degF
            "heartrate": int(v(82, 12, 34)),
            "resprate": int(v(17, 3, 9)),
            "o2sat": min(100, int(v(97.5, 1.6, -7))),
            "sbp": int(v(133, 18, -22)),
            "dbp": int(v(76, 11, -10)),
            "pain": str(self.rng.randint(0, 10)),
            "acuity": enc.esi_acuity,
        }

    def _choose_unit(self, facility, enc):
        """
        Route by acuity, then fall back down the acuity ladder when the
        preferred unit is full — which is how real hospitals behave and how
        capacity pressure propagates into the data.
        """
        units = self.dims.inpatient_units(facility.facility_id)
        if not units:
            return None
        by_code = {u.unit_code: u for u in units}
        if enc.esi_acuity <= 1:
            prefs = ["MICU", "SICU", "CVICU", "SDU", "TELE", "MS"]
        elif enc.esi_acuity == 2:
            prefs = ["SDU", "TELE", "MICU", "MS", "ONC"]
        else:
            prefs = ["MS", "TELE", "ONC", "SDU", "PEDS"]
        for code in prefs:
            u = by_code.get(code)
            if u and self.beds.available(u) > 0:
                return u
        # everything preferred is full — take anything with a bed
        open_units = [u for u in units if self.beds.available(u) > 0]
        return self.rng.choice(open_units) if open_units else None

    # -- inpatient --------------------------------------------------------
    def _admit(self, enc, unit, when):
        facility = next(f for f in self.dims.facilities if f.facility_id == enc.facility_id)
        enc.hadm_id = self._next_id("HADM")
        enc.admittime = when
        enc.encounter_class = "inpatient"
        enc.act_code = "IMP"
        enc.patient_class = "I"
        enc.hospital_service = self.rng.choice([
            "MED", "MED", "MED", "CMED", "SURG", "NSURG", "OMED", "TRAUM",
            "CSURG", "VSURG", "ORTHO", "PSYCH",
        ])
        if enc.ed_arrival:
            enc.admission_type = self.rng.choices(
                ["EW EMER.", "URGENT", "EU OBSERVATION", "OBSERVATION ADMIT", "DIRECT EMER."],
                weights=[0.58, 0.19, 0.10, 0.08, 0.05])[0]
            enc.admission_location = "EMERGENCY ROOM"
            enc.admission_type_code = "E"
        else:
            enc.admission_type = self.rng.choices(
                ["ELECTIVE", "SURGICAL SAME DAY ADMISSION", "DIRECT OBSERVATION", "URGENT"],
                weights=[0.52, 0.28, 0.11, 0.09])[0]
            enc.admission_location = self.rng.choices(
                ["PHYSICIAN REFERRAL", "CLINIC REFERRAL", "TRANSFER FROM HOSPITAL",
                 "AMBULATORY SURGERY TRANSFER", "TRANSFER FROM SKILLED NURSING FACILITY"],
                weights=[0.50, 0.20, 0.14, 0.10, 0.06])[0]
            enc.admission_type_code = "C" if "ELECTIVE" in enc.admission_type else "U"

        # ED-1 exclusion needs a transfer-in flag with a 6-hour lookback
        enc.transferred_in_within_6h = enc.admission_location == "TRANSFER FROM HOSPITAL"

        # principal inpatient diagnosis + comorbidities
        dx = self.dims.pick_diagnosis("IP")
        enc.diagnoses = [(1, dx[0], dx[1], dx[4])]
        for i in range(self.rng.randint(1, 7)):
            c = self.dims.pick_diagnosis("IP")
            if c[0] not in [d[1] for d in enc.diagnoses]:
                enc.diagnoses.append((len(enc.diagnoses) + 1, c[0], c[1], c[4]))

        los = max(0.15, self.rng.lognormvariate(math.log(max(unit.alos_days, 0.2)),
                                               min(unit.alos_sigma / max(unit.alos_days, 1), 1.1)))
        los = min(los, 118.0)   # keep inside the eCQM LOS <= 120 day population
        enc.dischtime = enc.admittime + timedelta(days=los)

        enc.stays.append([unit, enc.admittime, None])
        self.beds.admit(unit)
        self.active.append(enc)

    def _discharge_due(self, now):
        for enc in list(self.active):
            if enc.dischtime and enc.dischtime <= now:
                self._discharge(enc, enc.dischtime)
            elif enc.stays and self.rng.random() < 0.004:
                self._maybe_transfer(enc, now)

    def _maybe_transfer(self, enc, now):
        cur = enc.stays[-1]
        facility = next(f for f in self.dims.facilities if f.facility_id == enc.facility_id)
        units = [u for u in self.dims.inpatient_units(facility.facility_id)
                 if u.unit_id != cur[0].unit_id and self.beds.available(u) > 0]
        if not units:
            return
        nxt = self.rng.choice(units)
        cur[2] = now
        self.beds.release(cur[0])
        enc.stays.append([nxt, now, None])
        self.beds.admit(nxt)

    def _discharge(self, enc, when, forced=False):
        if enc not in self.active:
            return
        for stay in enc.stays:
            if stay[2] is None:
                stay[2] = when
                self.beds.release(stay[0])
        enc.dischtime = when
        enc.discharge_location = self.rng.choices(
            R.DISCHARGE_LOCATION, weights=R.DISCHARGE_LOCATION_WEIGHTS)[0]
        if enc.discharge_location == "DIED":
            enc.hospital_expire_flag = 1
            enc.deathtime = when
        self._assign_drg(enc)
        self.active.remove(enc)
        self._consider_readmission(enc)

    def _assign_drg(self, enc):
        """
        Assign an MS-DRG from the principal diagnosis.

        This is a family lookup plus a severity draw, NOT a grouper. A real
        assignment resolves principal diagnosis, every secondary diagnosis
        graded for CC/MCC, OR procedures and discharge disposition through the
        CMS Definitions Manual. Severity here is weighted by length of stay and
        ICU exposure so the tier at least correlates with how sick the patient
        actually was, rather than being noise.
        """
        if not enc.diagnoses:
            return
        family = R.drg_family_for(enc.diagnoses[0][1])
        tiers = R.MSDRG_FAMILIES.get(family) or R.MSDRG_FAMILIES["OTHER_MEDICAL"]
        icu = any(getattr(u, "is_critical_care", False) for u, _, _ in enc.stays)
        los = enc.los_days or 0.0
        severity = (1.0 if icu else 0.0) + (1.0 if los > 6 else 0.0) \
            + (1.0 if enc.hospital_expire_flag else 0.0)
        by_tier = {t[3]: t for t in tiers}
        if severity >= 2 and "MCC" in by_tier:
            pick = by_tier["MCC"]
        elif severity >= 1:
            pick = by_tier.get("CC") or by_tier.get("MCC") or tiers[-1]
        else:
            pick = by_tier.get("NONE") or tiers[-1]
        enc.drg_code, enc.drg_weight = pick[0], pick[2]

    # -- readmission (CMS HRRP rules) -------------------------------------
    def _consider_readmission(self, enc):
        """
        HRRP index-admission eligibility, verified rules:
          - discharged alive
          - not AMA
          - not primary psychiatric, not rehab, not cancer treatment
        Only one readmission counted per index; planned readmissions excluded
        from the outcome. We schedule the return here; the counting rule is
        enforced in the Gold layer, which is where the client will check it.
        """
        if enc.hospital_expire_flag == 1:
            return
        if enc.discharge_location == "AGAINST ADVICE":
            return
        if enc.hospital_service in ("PSYCH",):
            return
        cohort = enc.principal_diagnosis[3] if enc.principal_diagnosis else "OTHER"
        rate = C.READMISSION_RATE_BY_COHORT.get(cohort, C.READMISSION_RATE_BY_COHORT["OTHER"])
        if self.rng.random() >= rate:
            return
        gap = min(29, max(1, int(self.rng.expovariate(1.0 / C.READMISSION_DAY_WEIGHTS_LAMBDA)) + 1))
        return_date = (enc.dischtime + timedelta(days=gap)).date()
        if return_date >= self.end_date:
            return
        planned = self.rng.random() < C.PLANNED_READMISSION_SHARE
        self._scheduled_returns.append(
            (return_date, enc.patient, cohort, enc.encounter_id, planned, enc.facility_id))

    def _release_scheduled_returns(self, facility, day, hour, now):
        if hour != 9:      # readmissions arrive during the day
            return
        due = [r for r in self._scheduled_returns
               if r[0] == day and r[5] == facility.facility_id]
        for r in due:
            self._scheduled_returns.remove(r)
            _, patient, cohort, index_id, planned, _ = r
            arrival = now.replace(minute=self.rng.randint(0, 59))
            if planned:
                # planned readmission arrives as a direct/elective admission
                unit = self._choose_unit(facility, Encounter(
                    encounter_id="tmp", patient=patient, facility_id=facility.facility_id,
                    encounter_class="inpatient", act_code="IMP", patient_class="I",
                    esi_acuity=3))
                if unit is None:
                    continue
                enc = Encounter(
                    encounter_id=self._next_id("ENC"), patient=patient,
                    facility_id=facility.facility_id, encounter_class="inpatient",
                    act_code="IMP", patient_class="I", esi_acuity=3,
                    is_readmission=True, is_planned_readmission=True,
                    index_encounter_id=index_id)
                enc.payer = self.dims.pick_payer()
                self._admit(enc, unit, arrival)
                self.encounters.append(enc)
            else:
                self._ed_arrival(facility, arrival, patient=patient,
                                 forced_cohort=cohort, is_readmission=True,
                                 index_id=index_id, planned=False)

    # -- bed snapshots ----------------------------------------------------
    def _snapshot_beds(self, now):
        # prune finished boarding intervals, then count what is still boarding
        pending_by_facility = {}
        for fid, intervals in self._boarding.items():
            live = [iv for iv in intervals if iv[1] > now]
            self._boarding[fid] = live
            pending_by_facility[fid] = sum(1 for iv in live if iv[0] <= now)

        bucket = self.snapshots_by_date[now.date()]
        for u in self.dims.units:
            # The bed feed is inpatient capacity for surge planning. ED holding
            # and outpatient rooms are not inpatient beds and NHSN does not
            # count them, so including them would dilute network occupancy with
            # units that are structurally always empty.
            if u.unit_code == "ED" or u.unit_code in C.OUTPATIENT_UNIT_CODES:
                continue
            denom = u.staffed_beds - u.blocked_beds
            occupied = self.beds.occupied[u.unit_id]
            pending = pending_by_facility.get(u.facility_id, 0)
            # Expected discharges in the next four hours. This was hard-coded to
            # zero, which made the column dead and left the surge-planning view
            # with only half its signal: pending_admissions is near-term demand,
            # this is near-term supply.
            horizon = now + timedelta(hours=4)
            leaving = 0
            for enc in self.active:
                for unit, tin, tout in enc.stays:
                    if unit.unit_id == u.unit_id and tout is not None and now <= tout <= horizon:
                        leaving += 1
                        break
            bucket.append({
                "snapshot_datetime": now,
                "facility_id": u.facility_id,
                "unit_id": u.unit_id,
                "unit_code": u.unit_code,
                "licensed_beds": u.licensed_beds,
                "staffed_beds": u.staffed_beds,
                "blocked_beds": u.blocked_beds,
                "occupied_beds": occupied,
                "available_beds": max(0, denom - occupied),
                "pending_admissions": pending,
                "pending_discharges": leaving,
                # divide-by-zero is real here for the urgent care site
                "occupancy_rate": round(occupied / denom, 4) if denom > 0 else None,
                "is_at_capacity": (occupied / denom >= C.CAPACITY_THRESHOLD) if denom > 0 else None,
            })

    def _record_midnight_census(self, day):
        for u in self.dims.units:
            self.daily_census[(day, u.unit_id)] = self.beds.occupied[u.unit_id]

    # -- census lookup used by the staffing emitter -----------------------
    def census(self, day, unit_id):
        return self.daily_census.get((day, unit_id), 0)
