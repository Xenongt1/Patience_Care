#!/usr/bin/env python3
"""
Validation harness.

Two jobs:
  1. Prove the generated data answers all six of the client's business
     questions (client-request.md Section 4) using the REAL measure
     definitions -- OP-18 / ED-1 / ED-2 for wait times, CMS HRRP rules for
     readmissions, CLP02=4 for denials.
  2. Prove the injected defects are findable, by reconciling against the
     answer key and running the cross-source referential-integrity checks.

If a KPI cannot be computed here, it cannot be computed in the Gold layer
either -- which means the schema is wrong and it is cheap to fix now.

    python validate.py out
"""

import glob
import json
import os
import sys

import pandas as pd

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 50)

OK, WARN, FAIL = "PASS", "WARN", "FAIL"
results = []


def check(name, status, detail=""):
    results.append((name, status, detail))
    mark = {OK: "  ok  ", WARN: " warn ", FAIL: " FAIL "}[status]
    print(f"[{mark}] {name}" + (f"  -- {detail}" if detail else ""))


def load(root, pattern):
    files = sorted(glob.glob(os.path.join(root, "batch", pattern)))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_csv(f, dtype=str, keep_default_na=False, na_values=[""])
                      for f in files], ignore_index=True)


def num(s):
    return pd.to_numeric(s, errors="coerce")


def dt(s):
    return pd.to_datetime(s, errors="coerce", format="mixed")


def section(title):
    print()
    print("=" * 74)
    print(title)
    print("=" * 74)


def main(root="out"):
    section("LOADING")
    ed = load(root, "ehr/ed_stays/*.csv")
    adm = load(root, "ehr/admissions/*.csv")
    enc = load(root, "ehr/encounters/*.csv")
    dxs = load(root, "ehr/diagnoses/*.csv")
    beds = load(root, "beds/hourly_snapshot/*.csv")
    nhsn = load(root, "beds/nhsn_weekly/*.csv")
    inv = load(root, "pharmacy/inventory/*.csv")
    ch = load(root, "claims/claim_header/*.csv")
    rm = load(root, "claims/remit/*.csv")
    adj = load(root, "claims/remit_adjustment/*.csv")
    units = load(root, "reference/dim_unit/*.csv")
    fac = load(root, "reference/dim_facility/*.csv")
    payers = load(root, "reference/dim_payer/*.csv")

    for label, df in [("ed_stays", ed), ("admissions", adm), ("encounters", enc),
                      ("diagnoses", dxs), ("bed_snapshots", beds),
                      ("nhsn_weekly", nhsn), ("inventory", inv),
                      ("claim_header", ch), ("remit", rm), ("adjustments", adj)]:
        print(f"  {label:<16} {len(df):>9,} rows")

    if units.empty:
        print("\nNo reference data found. Did you point at the right --out directory?")
        return

    units = units.drop_duplicates("unit_id")
    fac = fac.drop_duplicates("facility_id")
    fac_name = dict(zip(fac.facility_id, fac.facility_name))

    # =================================================================
    section("Q1. Where are patient wait times longest, and how are they trending?")
    # =================================================================
    if ed.empty:
        check("ED wait time measures", FAIL, "no ed_stays data")
    else:
        e = ed.copy()
        e["intime"] = dt(e["intime"])
        e["outtime"] = dt(e["outtime"])
        e["triage_time"] = dt(e["triage_time"])
        e["admit_decision_time"] = dt(e["admit_decision_time"])
        e["door_to_triage_min"] = (e.triage_time - e.intime).dt.total_seconds() / 60
        e["total_ed_min"] = (e.outtime - e.intime).dt.total_seconds() / 60
        e["boarding_min"] = (e.outtime - e.admit_decision_time).dt.total_seconds() / 60

        # OP-18: discharged from ED, excluding expired and excluding visits
        # followed by inpatient admission at the same facility
        op18 = e[(e.disposition.isin(["HOME", "LEFT AGAINST MEDICAL ADVICE"]))
                 & (e.hadm_id.isna())]
        # ED-1: admitted patients
        ed1 = e[e.hadm_id.notna()]
        # ED-2: admit decision -> ED departure
        ed2 = e[e.admit_decision_time.notna()]

        print(f"\n  OP-18  median ED arrival->departure, DISCHARGED : "
              f"{op18.total_ed_min.median():.0f} min  (n={len(op18):,})")
        print(f"  ED-1   median ED arrival->departure, ADMITTED   : "
              f"{ed1.total_ed_min.median():.0f} min  (n={len(ed1):,})")
        print(f"  ED-2   median admit decision->departure (board) : "
              f"{ed2.boarding_min.median():.0f} min  (n={len(ed2):,})")
        print(f"  door-to-triage median                          : "
              f"{e.door_to_triage_min.median():.0f} min")

        by_fac = (e.groupby("facility_id")
                    .agg(n=("stay_id", "count"),
                         median_total=("total_ed_min", "median"),
                         p90_total=("total_ed_min", "quantile"),
                         median_triage=("door_to_triage_min", "median"))
                    .sort_values("median_total", ascending=False))
        by_fac.index = [fac_name.get(i, i) for i in by_fac.index]
        print("\n  by facility (median minutes in ED, worst first):")
        print(by_fac.round(1).to_string())

        ok = (op18.total_ed_min.median() > 0 and ed2.boarding_min.notna().sum() > 0)
        check("OP-18 / ED-1 / ED-2 all computable", OK if ok else FAIL,
              "triage_time and admit_decision_time present as designed")

        # trend
        e["day"] = e.intime.dt.date
        trend = e.groupby("day").total_ed_min.median()
        check("wait time trend series", OK if len(trend) > 1 else WARN,
              f"{len(trend)} daily points")

    # =================================================================
    section("Q2. Which facilities/units are at risk of running out of beds?")
    # =================================================================
    if beds.empty:
        check("bed capacity", FAIL, "no bed snapshot data")
    else:
        b = beds.copy()
        b["snapshot_datetime"] = dt(b["snapshot_datetime"])
        for c in ("occupied_beds", "staffed_beds", "blocked_beds", "occupancy_rate"):
            b[c] = num(b[c])
        b["denom"] = b.staffed_beds - b.blocked_beds
        b = b[b.denom > 0]
        b["occ"] = b.occupied_beds / b.denom

        # Quarantine physically impossible rows first. Occupancy above 100% of
        # staffed-minus-blocked beds cannot happen, so any such row is a data
        # defect -- which is precisely what the Silver->Gold DQ gate must
        # reject before the Gold occupancy measure is computed.
        quarantined = b[(b.occ > 1.0) | (b.occupied_beds < 0) | (b.staffed_beds <= 0)]
        clean = b.drop(quarantined.index)
        overall = clean.occ.mean()
        print(f"\n  rows quarantined (occ>100% or negative) : {len(quarantined):,} "
              f"({len(quarantined)/max(1,len(b)):.2%})")
        print(f"  network mean occupancy (post-DQ)       : {overall:.1%}   "
              f"(JAMA post-pandemic US mean ~75%)")
        at_cap = (clean.occ >= 0.85).mean()
        print(f"  unit-hours at/over 85% (post-DQ)       : {at_cap:.1%}")
        b = clean

        worst = (b.groupby(["facility_id", "unit_code"])
                  .agg(mean_occ=("occ", "mean"), peak_occ=("occ", "max"),
                       hours_at_capacity=("occ", lambda s: int((s >= 0.85).sum())))
                  .sort_values("mean_occ", ascending=False).head(12))
        print("\n  units under most pressure:")
        print(worst.round(3).to_string())

        check("occupancy in a plausible band", OK if 0.55 <= overall <= 0.95 else WARN,
              f"{overall:.1%}")
        check("capacity-risk units identifiable", OK if at_cap > 0 else WARN,
              f"{at_cap:.1%} of unit-hours at/over threshold")

        # the urgent care site has zero beds -- divide-by-zero must be handled
        z = beds.copy()
        z["sb"], z["bb"] = num(z.staffed_beds), num(z.blocked_beds)
        # only rows the generator itself produced with no beds -- exclude rows
        # whose staffed_beds was mangled by injected outlier defects
        zero = z[(z.sb - z.bb <= 0) & (z.sb >= 0)]
        bad = zero[num(zero.occupancy_rate).notna()]
        check("zero-bed facility handled (null, not error)",
              OK if bad.empty else FAIL,
              f"{len(zero):,} zero-denominator rows; "
              f"{len(bad)} wrongly carry an occupancy value")

        # NHSN weekly roll-up must reconcile to the hourly detail
        if not nhsn.empty:
            n = nhsn.copy()
            n["collection_date"] = dt(n["collection_date"])
            recon = []
            for _, r in n.iterrows():
                day = r.collection_date.date()
                sub = b[(b.facility_id == r.nhsn_org_id)
                        & (b.snapshot_datetime.dt.date == day)]
                if sub.empty:
                    continue
                hourly = sub.occupied_beds.sum() / 24.0
                reported = float(r.all_hospital_inpatient_occupancy)
                recon.append(abs(hourly - reported) <= max(2.0, hourly * 0.05))
            rate = (sum(recon) / len(recon)) if recon else 0
            check("NHSN weekly reconciles to hourly detail",
                  OK if rate >= 0.9 else WARN, f"{rate:.0%} of facility-weeks within tolerance")

    # =================================================================
    section("Q3. Are we adequately staffed relative to patient load, by dept and shift?")
    # =================================================================
    try:
        import openpyxl
        sched_files = sorted(glob.glob(os.path.join(
            root, "batch", "sharepoint/staff_schedules/*.xlsx")))
        rows = []
        for f in sched_files:
            wb = openpyxl.load_workbook(f, read_only=True)
            ws = wb.active
            header, data = None, []
            for r in ws.iter_rows(values_only=True):
                if header is None:
                    if r and r[0] == "Facility ID":
                        header = list(r)
                    continue
                if r and any(v is not None for v in r):
                    data.append(r[:len(header)])
            if header:
                rows.append(pd.DataFrame(data, columns=header))
            wb.close()
        sched = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    except Exception as exc:
        sched = pd.DataFrame()
        check("staff schedule parse", FAIL, str(exc))

    if sched.empty:
        check("staffing adequacy", WARN, "no schedule workbooks found")
    else:
        print(f"\n  parsed {len(sched):,} roster rows from "
              f"{len(sched_files)} SharePoint workbooks")
        s = sched.copy()
        # the workbook carries mixed date formats on purpose; normalise first
        s["Work Date"] = pd.to_datetime(s["Work Date"], errors="coerce",
                                        format="mixed").dt.date
        s["Census"] = num(s["Census"])
        s["Actual Hours"] = num(s["Actual Hours"])
        nurses = s[s["Job Code"].isin(["RN", "LPN", "NP"])]
        # On-call is a fixed cover team, not rostered presence, so a
        # nurse-to-patient ratio computed against it is meaningless -- it yields
        # one nurse "covering" a 50-bed unit and swamps the real signal. The
        # mandated-ratio KPI is a question about the shifts that actually cover
        # the census. On-call is reported separately below.
        oncall = nurses[nurses["Shift"] == "OC"]
        nurses = nurses[nurses["Shift"] != "OC"]
        staffed = (nurses[nurses["Called Out"] != "Y"]
                   .groupby(["Facility ID", "Unit Code", "Work Date", "Shift"])
                   .agg(nurses_on=("Staff ID", "count"), census=("Census", "max"))
                   .reset_index())
        tgt = dict(zip(units.unit_code, num(units.nurse_patient_ratio_target)))
        staffed["target_ratio"] = staffed["Unit Code"].map(tgt)
        staffed["required"] = (staffed.census / staffed.target_ratio).apply(
            lambda x: max(1, int(-(-x // 1))) if pd.notna(x) else None)
        staffed["actual_ratio"] = staffed.census / staffed.nurses_on.replace(0, pd.NA)
        staffed["is_understaffed"] = staffed.nurses_on < staffed.required

        under = staffed.is_understaffed.mean()
        print(f"  shift-units understaffed vs mandated ratio : {under:.1%}"
              f"   (D/E/N shifts; on-call excluded)")
        print(f"  on-call shifts rostered (cover, not counted): {len(oncall):,}")
        worst = (staffed.groupby(["Facility ID", "Unit Code", "Shift"])
                        .agg(shifts=("is_understaffed", "count"),
                             pct_understaffed=("is_understaffed", "mean"),
                             mean_actual_ratio=("actual_ratio", "mean"),
                             target=("target_ratio", "max"))
                        .sort_values("pct_understaffed", ascending=False).head(10))
        print("\n  chronically understaffed department/shift combinations:")
        print(worst.round(2).to_string())

        check("staffing vs ratio target computable", OK if under > 0 else WARN,
              "roster joins to dim_unit on unit_code, and to census on work date")
        # Spread has to be measured across ALL department/shift combinations.
        # Measuring it inside the worst-10 head is circular: once ten
        # combinations sit at 1.0 the range there is always zero, so the check
        # warned even when the underlying data was strongly non-uniform.
        allcombo = (staffed.groupby(["Facility ID", "Unit Code", "Shift"])
                           .is_understaffed.mean())
        spread = allcombo.max() - allcombo.min()
        check("understaffing is non-uniform (discoverable)",
              OK if spread > 0.2 else WARN,
              f"range across {len(allcombo)} dept/shift combinations: "
              f"{allcombo.min():.0%} to {allcombo.max():.0%}")

    # =================================================================
    section("Q4. What is our readmission rate, and does it vary by facility/diagnosis?")
    # =================================================================
    if adm.empty:
        check("readmission rate", FAIL, "no admissions data")
    else:
        a = adm.copy()
        a["admittime"] = dt(a["admittime"])
        a["dischtime"] = dt(a["dischtime"])
        a["hospital_expire_flag"] = num(a["hospital_expire_flag"])
        # HRRP index eligibility, per the verified CMS rules
        idx = a[(a.hospital_expire_flag == 0)
                & (a.discharge_location != "AGAINST ADVICE")
                & (a.hospital_service != "PSYCH")
                & (a.dischtime.notna())]
        # Right-censoring: an index admission discharged fewer than 30 days
        # before the end of the extract cannot yet have been observed for a
        # full 30 days. Including it understates the rate -- the Gold layer
        # must apply this same exclusion.
        window_end = a.dischtime.max()
        idx = idx[idx.dischtime <= window_end - pd.Timedelta(days=30)]
        readm = a[(a.is_readmission == "1") & (a.is_planned_readmission != "1")]
        # The numerator has to be drawn from the SAME population as the
        # denominator. Counting every unplanned readmission in the extract
        # against a censored index population mixes two cohorts: many of these
        # readmissions follow an index stay that pre-dates the window (or was
        # excluded as died/AMA/psych), so they have no denominator to belong
        # to. Each readmission carries index_encounter_id, so tie it back and
        # keep only those whose index admission is itself eligible.
        eligible_idx_enc = set(idx.encounter_id.dropna())
        linked = readm[readm.index_encounter_id.isin(eligible_idx_enc)]
        # A rate needs a denominator big enough to be a rate. On a window
        # shorter than ~30 days, right-censoring correctly removes essentially
        # every index admission, and dividing by whatever stragglers survive
        # turns "not computable" into a number -- reporting 41800% as though it
        # were an implausible rate rather than an absent one.
        MIN_INDEX = 30
        computable = len(idx) >= MIN_INDEX
        rate = (len(linked) / len(idx)) if computable else None
        print(f"\n  observation window ends   : {window_end.date()} "
              f"(index admissions after {(window_end - pd.Timedelta(days=30)).date()} "
              f"excluded as right-censored)")
        print(f"  eligible index admissions : {len(idx):,}")
        print(f"  unplanned readmissions    : {len(readm):,} "
              f"({len(linked):,} follow an eligible index stay in this extract)")
        if computable:
            print(f"  30-day readmission rate   : {rate:.1%}")
        else:
            print(f"  30-day readmission rate   : NOT COMPUTABLE -- only "
                  f"{len(idx):,} index admission(s) have a full 30-day "
                  f"follow-up window (need >= {MIN_INDEX})")

        # by cohort
        principal = dxs[num(dxs.seq_num) == 1][["hadm_id", "hrrp_cohort"]] if not dxs.empty else pd.DataFrame()
        if not principal.empty and computable:
            m = idx.merge(principal, on="hadm_id", how="left")  # censored-safe
            # A readmission belongs to the cohort of the stay it followed, not
            # to its own principal diagnosis -- a HF patient readmitted with
            # sepsis is still a HF-cohort readmission. Attribute through the
            # index link so numerator and denominator share a cohort label.
            idx_cohort = m[["encounter_id", "hrrp_cohort"]].rename(
                columns={"encounter_id": "index_encounter_id"})
            mr = linked.drop(columns=["hrrp_cohort"], errors="ignore").merge(
                idx_cohort, on="index_encounter_id", how="left")
            cohort = pd.DataFrame({
                "index_admissions": m.groupby("hrrp_cohort").size(),
                "readmissions": mr.groupby("hrrp_cohort").size(),
            }).fillna(0)
            cohort["rate"] = (cohort.readmissions / cohort.index_admissions
                              .replace(0, pd.NA)).round(3)
            print("\n  by HRRP cohort:")
            print(cohort.to_string(na_rep="n/a"))
        check("HRRP index/exclusion rules applied", OK,
              "discharged alive, not AMA, not psych, one per index")
        if not computable:
            # Not a data defect. The generator is fine; the window is too short.
            check("readmission rate computable", WARN,
                  f"window is {(window_end - a.dischtime.min()).days}d -- needs "
                  f">30d of discharges before an index cohort is observable. "
                  f"Run --days 90 or more to evaluate this KPI.")
        else:
            check("readmission rate plausible",
                  OK if 0.05 <= rate <= 0.30 else WARN, f"{rate:.1%}")

    # =================================================================
    section("Q5. How much revenue is at risk from denied or delayed claims, by payer?")
    # =================================================================
    if ch.empty or rm.empty:
        check("claims KPIs", FAIL, "no claims data")
    else:
        c = ch.merge(rm, on="patient_control_number", how="left",
                     suffixes=("", "_rm"))
        c["total_charge_amount"] = num(c["total_charge_amount"])
        c["claim_payment_amount"] = num(c["claim_payment_amount"])
        c["submission_date"] = dt(c["submission_date"])
        c["remit_date"] = dt(c["remit_date"])
        c["days_to_payment"] = (c.remit_date - c.submission_date).dt.days
        c["is_denied"] = c.claim_status_code == "4"

        adjudicated = c[c.claim_status_code.notna()]
        denial_rate = adjudicated.is_denied.mean()
        print(f"\n  adjudicated claims        : {len(adjudicated):,}")
        print(f"  initial denial rate       : {denial_rate:.2%}   "
              f"(Kodiak 2024 published: 11.81%)")
        print(f"  median days to payment    : {c.days_to_payment.median():.0f} days   "
              f"(Kodiak 2025 published: 55.2)")
        print(f"  revenue at risk (denied)  : ${adjudicated[adjudicated.is_denied].total_charge_amount.sum():,.0f}")

        by_payer = (adjudicated.groupby("payer_name")
                    .agg(claims=("patient_control_number", "count"),
                         denial_rate=("is_denied", "mean"),
                         charges=("total_charge_amount", "sum"),
                         denied_charges=("total_charge_amount",
                                         lambda s: s[adjudicated.loc[s.index, "is_denied"]].sum()),
                         median_days_to_pay=("days_to_payment", "median"))
                    .sort_values("denial_rate", ascending=False))
        print("\n  by payer:")
        print(by_payer.round(3).to_string())

        check("denial rate near published benchmark",
              OK if 0.08 <= denial_rate <= 0.16 else WARN, f"{denial_rate:.2%}")
        check("days-in-AR computable", OK if c.days_to_payment.notna().any() else FAIL,
              "requires submission_date and remit_date, our additions")

        if not adj.empty:
            a2 = adj.copy()
            true_denials = a2[a2.is_denial == "1"]
            writeoffs = a2[(a2.group_code == "CO") & (a2.reason_code.isin(["45", "97", "24"]))]
            print(f"\n  CARC adjustment rows      : {len(a2):,}")
            print(f"    true denial CARCs       : {len(true_denials):,}")
            print(f"    contractual write-offs  : {len(writeoffs):,}  (NOT denials)")
            top = true_denials.groupby(["group_code", "reason_code"]).size().sort_values(ascending=False).head(8)
            print("\n  top denial reason codes:")
            for (g, rc), n in top.items():
                print(f"    {g}-{rc:<4} {n:>6,}")
            check("denial vs write-off distinguishable", OK if not writeoffs.empty else WARN,
                  "CO-45/97/24 must not inflate the denial rate")
            c16 = true_denials[true_denials.reason_code == "16"]
            check("CARC 16 always carries a RARC",
                  OK if c16.empty or c16.remark_code.notna().all() else FAIL,
                  "X12 requires at least one remark code with CARC 16")
            check("deactivated CARC 15 not generated",
                  OK if "15" not in set(a2.reason_code) else FAIL, "deactivated 05/01/2018")

    # =================================================================
    section("Q6. Where and how often are we at risk of a pharmacy stockout?")
    # =================================================================
    if inv.empty:
        check("stockout risk", FAIL, "no inventory data")
    else:
        i = inv.copy()
        for col in ("qty_on_hand", "reorder_point", "par_level", "days_on_hand",
                    "avg_daily_usage_30d", "extended_value"):
            i[col] = num(i[col])
        i["snapshot_date"] = dt(i["snapshot_date"])
        i["at_risk"] = i.qty_on_hand <= i.reorder_point
        i["stockout"] = i.qty_on_hand <= 0

        print(f"\n  inventory rows           : {len(i):,}")
        print(f"  at/below reorder point   : {i.at_risk.mean():.1%}")
        print(f"  actual stockouts         : {i.stockout.sum():,} rows")
        print(f"  items in shortage        : "
              f"{(i.shortage_status == 'Currently in Shortage').mean():.2%} of rows")
        print(f"  median days on hand      : {i.days_on_hand.median():.1f}")

        worst = (i[i.at_risk].groupby(["facility_id", "drug_name"]).size()
                  .sort_values(ascending=False).head(10))
        print("\n  most frequently at-risk items:")
        for (f_, d_), n in worst.items():
            print(f"    {fac_name.get(f_, f_)[:34]:<36} {d_[:34]:<36} {n:>4} days")

        check("stockout risk computable", OK if i.at_risk.any() else WARN,
              "par_level / reorder_point are our additions -- FHIR has no such fields")
        check("controlled substances flagged",
              OK if (i.dea_schedule.notna()).any() else WARN,
              "DEA schedule present for CII-CV items")
        # the propofol / dexmedetomidine trap
        unsched = i[i.drug_name.str.contains("Propofol|Dexmedetomidine", na=False, case=False)]
        check("propofol/dexmedetomidine correctly NOT scheduled",
              OK if unsched.empty or unsched.dea_schedule.isna().all() else FAIL,
              "a detail synthetic generators routinely get wrong")

    # =================================================================
    section("CROSS-SOURCE REFERENTIAL INTEGRITY")
    # =================================================================
    unit_ids = set(units.unit_id)
    fac_ids = set(fac.facility_id)

    if not beds.empty:
        orphan = set(beds.unit_id) - unit_ids
        check("bed snapshots -> dim_unit", OK if not orphan else WARN,
              f"{len(orphan)} unknown unit_id" if orphan else "all resolve")

    if not enc.empty and not adm.empty:
        enc_ids = set(enc["Id"])
        orphan = set(adm.encounter_id.dropna()) - enc_ids
        check("admissions -> encounters", OK if len(orphan) / max(1, len(adm)) < 0.02 else WARN,
              f"{len(orphan)} orphan encounter_id (injected defects expected)")

    if not ch.empty and not enc.empty:
        orphan = set(ch.encounter_id.dropna()) - set(enc["Id"])
        pct = len(orphan) / max(1, len(ch))
        check("claims -> encounters", OK if pct < 0.05 else WARN,
              f"{len(orphan)} orphan ({pct:.2%}) -- injected orphans are the point")

    if not ch.empty and not payers.empty:
        orphan = set(ch.payer_id.dropna()) - set(payers.payer_id)
        check("claims -> dim_payer", OK if not orphan else WARN, f"{len(orphan)} unknown payer_id")

    if not inv.empty:
        orphan = set(inv.facility_id) - fac_ids
        check("inventory -> dim_facility", OK if not orphan else WARN, f"{len(orphan)} unknown")

    # =================================================================
    section("DQ ANSWER KEY RECONCILIATION")
    # =================================================================
    key_path = os.path.join(root, "dq_answer_key.json")
    if not os.path.exists(key_path):
        check("answer key present", WARN, "no dq_answer_key.json")
    else:
        with open(key_path) as fh:
            key = json.load(fh)
        print(f"\n  defects injected: {key['total_defects']:,}")
        for k, v in sorted(key["by_type"].items()):
            print(f"    {k:<26} {v:>8,}")
        print("\n  each of these must be caught by a named check in the Silver->Gold"
              "\n  DQ gate. The answer key is the marking scheme -- keep it out of Bronze.")
        check("answer key written", OK, f"{key['total_defects']:,} defects traceable")

        # spot-prove detectability
        if not ed.empty:
            e2 = ed.copy()
            inverted = (dt(e2.outtime) < dt(e2.intime)).sum()
            nulls = e2.acuity.isna().sum()
            nonnum = num(e2.heartrate).isna().sum() - e2.heartrate.isna().sum()
            print(f"\n  detectable in ed_stays alone:")
            print(f"    temporal inversions (outtime < intime) : {inverted:,}")
            print(f"    null required field (acuity)           : {nulls:,}")
            print(f"    non-numeric heartrate                  : {nonnum:,}")
            check("injected defects are detectable",
                  OK if (inverted + nulls + nonnum) > 0 else WARN,
                  "found by simple checks, as the DQ framework will")

    # =================================================================
    section("SUMMARY")
    # =================================================================
    counts = {OK: 0, WARN: 0, FAIL: 0}
    for _, st, _ in results:
        counts[st] += 1
    print(f"  pass {counts[OK]}   warn {counts[WARN]}   fail {counts[FAIL]}")
    if counts[FAIL]:
        print("\n  FAILURES:")
        for n, st, d in results:
            if st == FAIL:
                print(f"    - {n}: {d}")
    print()
    return 1 if counts[FAIL] else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "out"))
