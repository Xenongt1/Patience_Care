#!/usr/bin/env python3
"""
Meridian Health Network — synthetic source data generator.

    python run.py --days 7                        # smoke test, local output
    python run.py --days 7 --chaos                 # Phase 5 break-it test
    python run.py --days 7 --no-defects            # clean baseline

    # full 18 months of batch history, vitals only for the last 45 days.
    # ~1.2 GB batch + ~1.5 GB gzipped streams. Without --stream-days that
    # same run writes ~310 GB of raw vitals JSONL.
    python run.py --days 548 --start 2025-02-01 --stream-days 45

    # push batch to a Fabric lakehouse
    python run.py --days 7 --onelake-workspace "Meridian-DEV" \
                           --onelake-lakehouse "lh_bronze"

    # push stream to a Fabric Eventstream Kafka endpoint
    python run.py --days 7 --kafka-bootstrap "<server>:9093" \
                           --kafka-connection-string "Endpoint=sb://..." \
                           --kafka-topic "es_meridian"

Run with no cloud flags and everything lands under ./out/ so you can inspect
every file before a Fabric workspace exists.
"""

import argparse
import json
import os
import sys
import time
from datetime import date, datetime, timedelta

from meridian import config as C
from meridian.defects import DefectInjector
from meridian.dimensions import Dimensions
from meridian.emitters import Emitters
from meridian.simulate import Simulation
from meridian.sinks import (LocalBatchSink, LocalStreamSink,
                            OneLakeBatchSink, FabricEventstreamSink)


def parse_args():
    p = argparse.ArgumentParser(description="Meridian synthetic data generator")
    p.add_argument("--days", type=int, default=7,
                   help="days of history to generate (default 7 = smoke test)")
    p.add_argument("--start", type=str, default=None,
                   help="start date YYYY-MM-DD (default: today - days)")
    p.add_argument("--out", type=str, default="out", help="local output root")
    p.add_argument("--seed", type=int, default=C.SEED)
    p.add_argument("--chaos", action="store_true",
                   help="amplify defect rates for the Phase 5 break-it test")
    p.add_argument("--no-defects", action="store_true",
                   help="generate a clean baseline with no injected defects")
    p.add_argument("--no-streams", action="store_true",
                   help="skip the two streaming sources (much faster)")
    p.add_argument("--warmup", type=int, default=14,
                   help="days simulated BEFORE --start to fill the beds, not "
                        "emitted (default 14). Without this the hospital opens "
                        "empty and the first ~3 days understate census.")
    p.add_argument("--stream-days", type=int, default=None,
                   help="emit the two streams only for the LAST N days of the "
                        "window (default: all). Batch feeds always cover the "
                        "full window. Use this to get 18 months of history "
                        "without 300 GB of vitals.")
    p.add_argument("--no-gzip-streams", action="store_true",
                   help="write the local stream archive as plain .jsonl "
                        "instead of .jsonl.gz (17x larger)")
    p.add_argument("--onelake-workspace", type=str, default=None)
    p.add_argument("--onelake-lakehouse", type=str, default=None)
    p.add_argument("--onelake-prefix", type=str, default="landing")
    p.add_argument("--kafka-bootstrap", type=str, default=None)
    p.add_argument("--kafka-connection-string", type=str, default=None)
    p.add_argument("--kafka-topic", type=str, default=None)
    return p.parse_args()


def main():
    args = parse_args()
    start = (datetime.strptime(args.start, "%Y-%m-%d").date() if args.start
             else date.today() - timedelta(days=args.days))
    end = start + timedelta(days=args.days)

    # Simulate before the window opens so beds, LOS tails and the 30-day
    # readmission lookback are already populated on the first emitted day.
    warmup = max(0, args.warmup)
    sim_start = start - timedelta(days=warmup)

    # Streams are gated to a trailing slice of the window; batch is not.
    stream_from = (end - timedelta(days=args.stream_days)
                   if args.stream_days else start)

    print(f"Meridian synthetic data generator")
    print(f"  window   : {start} .. {end} ({args.days} days)")
    print(f"  warmup   : {warmup} days simulated from {sim_start} (not emitted)"
          if warmup else "  warmup   : NONE -- first ~3 days will understate census")
    print(f"  seed     : {args.seed}")
    print(f"  defects  : {'off' if args.no_defects else ('CHAOS' if args.chaos else 'normal')}")

    t0 = time.time()

    # ---- sinks -------------------------------------------------------------
    if args.onelake_workspace and args.onelake_lakehouse:
        batch = OneLakeBatchSink(args.onelake_workspace, args.onelake_lakehouse,
                                 args.onelake_prefix)
        print(f"  batch    : OneLake {args.onelake_workspace}/{args.onelake_lakehouse}")
    else:
        batch = LocalBatchSink(os.path.join(args.out, "batch"))
        print(f"  batch    : local {os.path.join(args.out, 'batch')}")

    archive = LocalStreamSink(os.path.join(args.out, "stream"),
                              compress=not args.no_gzip_streams)
    if args.kafka_bootstrap and args.kafka_connection_string:
        stream = FabricEventstreamSink(args.kafka_bootstrap,
                                       args.kafka_connection_string,
                                       args.kafka_topic, archive_sink=archive)
        print(f"  stream   : Fabric Eventstream Kafka -> {args.kafka_topic}")
    else:
        stream = archive
        ext = "jsonl" if args.no_gzip_streams else "jsonl.gz"
        print(f"  stream   : local {ext} {os.path.join(args.out, 'stream')}")
    if args.no_streams:
        print(f"  stream   : SKIPPED (--no-streams)")
    elif args.stream_days:
        print(f"  stream   : last {args.stream_days} days only "
              f"({stream_from} .. {end})")
    print()

    # ---- build -------------------------------------------------------------
    print("[1/4] building conformed dimensions ...")
    dims = Dimensions(args.seed)
    print(f"      {len(dims.facilities)} facilities, {len(dims.units)} units, "
          f"{len(dims.staff)} staff, {len(dims.drugs)} formulary items, "
          f"{len(dims.icd10)} diagnosis codes")

    print("[2/4] running patient-journey simulation ...")
    sim = Simulation(sim_start, args.days + warmup, dims=dims, seed=args.seed)

    def progress(done, total):
        bar = "#" * int(28 * done / total)
        sys.stdout.write(f"\r      [{bar:<28}] day {done}/{total}")
        sys.stdout.flush()

    sim.run(progress=progress)
    print()

    # Count only the reportable window. Warmup encounters exist in the
    # simulation (they hold beds, and they are the index admissions the early
    # readmissions point back to) but they are not part of the deliverable.
    def opened(e):
        return e.ed_arrival or e.admittime

    in_window = [e for e in sim.encounters
                 if opened(e) and opened(e).date() >= start]
    ip = sum(1 for e in in_window if e.is_inpatient)
    ed = sum(1 for e in in_window if e.ed_arrival)
    readm = sum(1 for e in in_window if e.is_readmission)
    print(f"      {len(in_window):,} encounters | {ed:,} ED | {ip:,} inpatient "
          f"| {readm:,} readmissions | {len(dims.patients):,} distinct patients")
    if warmup:
        print(f"      (+{len(sim.encounters) - len(in_window):,} warmup encounters "
              f"simulated before {start}, not counted)")

    print("[3/4] emitting source feeds ...")
    injector = DefectInjector(args.seed, chaos=args.chaos,
                              enabled=not args.no_defects)
    # every feed emits on run_date - 1, so that is the true floor of the deliverable
    em = Emitters(sim, dims, batch, stream, injector, args.seed,
                  emit_from=start - timedelta(days=1))

    staff_changes = []
    for d in range(args.days + 1):
        run_date = start + timedelta(days=d)
        if d == 0 or run_date.weekday() == 0:
            # Churn the staff dimension before emitting it, so successive weekly
            # snapshots genuinely differ and there is SCD-2 history to model.
            # Skipped on the first snapshot: that one is the opening baseline.
            if d != 0:
                # offsets are measured from the same epoch the emitter uses, so
                # a churn event dated "this week" lands on or before run_date
                staff_changes.extend(
                    dims.apply_staff_churn((run_date - (start - timedelta(days=1))).days))
            em.emit_dimensions(run_date)
            em.emit_code_sets(run_date)
        em.emit_ehr(run_date)
        em.emit_claims(run_date)
        em.emit_inventory(run_date)
        em.emit_bed_capacity(run_date)
        if not args.no_streams and run_date >= stream_from:
            em.emit_vitals(run_date)
            em.emit_prescriptions(run_date)
        if run_date.weekday() == 6:
            em.emit_nhsn_weekly(run_date)
        if run_date.weekday() == 0:
            em.emit_staff_schedule(run_date)
        sys.stdout.write(f"\r      day {d + 1}/{args.days + 1}")
        sys.stdout.flush()
    print()

    stream.close()

    print("[4/4] writing DQ answer key and run manifest ...")
    os.makedirs(args.out, exist_ok=True)

    # The answer key is the marking scheme, NOT source data.
    # It must never land in the Bronze zone.
    with open(os.path.join(args.out, "dq_answer_key.json"), "w") as fh:
        json.dump({
            "generated_at": datetime.now().isoformat(),
            "seed": args.seed,
            "chaos": args.chaos,
            "total_defects": len(injector.log),
            "by_type": injector.summary(),
            "defects": injector.log,
        }, fh, indent=2, default=str)

    manifest = {
        "generated_at": datetime.now().isoformat(),
        "window": {"start": str(start), "end": str(end), "days": args.days},
        "warmup": {"days": warmup, "simulated_from": str(sim_start)},
        "stream_window": {
            "emitted": not args.no_streams,
            "start": None if args.no_streams else str(stream_from),
            "days": None if args.no_streams else (args.stream_days or args.days),
        },
        "stream_compressed": not args.no_gzip_streams,
        "seed": args.seed,
        "defect_mode": "off" if args.no_defects else ("chaos" if args.chaos else "normal"),
        "dimensions": {
            "facilities": len(dims.facilities), "units": len(dims.units),
            "staff": len(dims.staff), "drugs": len(dims.drugs),
            "patients": len(dims.patients), "icd10_codes": len(dims.icd10),
        },
        "simulation": {
            "encounters": len(in_window), "ed_encounters": ed,
            "inpatient_admissions": ip, "readmissions": readm,
            "warmup_encounters": len(sim.encounters) - len(in_window),
        },
        "staff_dimension_changes": {
            "total": len(staff_changes),
            "hires": sum(1 for k, _ in staff_changes if k == "hire"),
            "terminations": sum(1 for k, _ in staff_changes if k == "terminate"),
            "unit_transfers": sum(1 for k, _ in staff_changes if k == "transfer"),
        },
        "emitted": dict(em.stats),
        "stream_counts": getattr(stream, "counts", {}),
        "batch_files": len(getattr(batch, "written", [])),
        "defects_injected": len(injector.log),
    }
    with open(os.path.join(args.out, "run_manifest.json"), "w") as fh:
        json.dump(manifest, fh, indent=2, default=str)

    # ---- summary -----------------------------------------------------------
    print()
    print("=" * 66)
    print(f"done in {time.time() - t0:.1f}s")
    print("-" * 66)
    for k, v in sorted(em.stats.items()):
        print(f"  {k:<26} {v:>12,}")
    print("-" * 66)
    print(f"  {'batch files written':<26} {len(getattr(batch, 'written', [])):>12,}")
    print(f"  {'defects injected':<26} {len(injector.log):>12,}")
    for k, v in sorted(injector.summary().items()):
        print(f"      {k:<22} {v:>12,}")
    print("=" * 66)

    # On-disk sizes, because the vitals feed is the one thing here big enough
    # to be a problem and it should never be a surprise after the fact.
    def _dirsize(root):
        return sum(os.path.getsize(os.path.join(dp, f))
                   for dp, _, fs in os.walk(root) for f in fs)

    print(f"\noutput: {args.out}/")
    for sub, note in (("batch", "upload to OneLake Files (or pass --onelake-* to push)"),
                      ("stream", "replay into Eventstream (or pass --kafka-* to push)")):
        path = os.path.join(args.out, sub)
        if os.path.isdir(path):
            print(f"  {sub + '/':<10}{_dirsize(path) / 1024**3:7.2f} GB  -> {note}")
    for topic, path in sorted(getattr(archive, "paths", {}).items()):
        if os.path.exists(path):
            print(f"      {os.path.basename(path):<32} "
                  f"{os.path.getsize(path) / 1024**3:6.2f} GB  "
                  f"{archive.counts.get(topic, 0):>12,} events")
    print(f"  dq_answer_key.json  -> KEEP OUT OF BRONZE. This is the DQ marking scheme.")


if __name__ == "__main__":
    main()
