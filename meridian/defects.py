"""
Defect injection with an answer key.

The client requires a documented data quality framework with visible pass/fail
results, and work-plan task 5.2 requires deliberately breaking something. A
perfectly clean generator makes both undemonstrable.

Every defect injected is recorded, so in Phase 5 you can prove the DQ framework
caught them rather than asserting it. Keep the answer key OUT of the Bronze
landing zone -- it is the marking scheme, not source data.
"""

import random
from datetime import timedelta

from . import config as C


class DefectInjector:
    def __init__(self, seed=C.SEED, chaos=False, enabled=True):
        self.rng = random.Random(seed + 99)
        self.mult = C.CHAOS_MULTIPLIER if chaos else 1.0
        self.enabled = enabled
        self.log = []          # the answer key

    def _hit(self, kind):
        if not self.enabled:
            return False
        return self.rng.random() < C.DEFECT_RATES[kind] * self.mult

    def _record(self, source, kind, key, field, original, mutated, expected_check):
        self.log.append({
            "source": source, "defect_type": kind, "row_key": key,
            "field": field, "original_value": original, "mutated_value": mutated,
            "expected_dq_check": expected_check,
        })

    # -- file-level failures ----------------------------------------------
    #
    # Every other defect class here is row-level: the file always arrives, on
    # time, complete. The client's success criteria require pipelines that "can
    # be demonstrated to recover from a failed run without manual data-fixing",
    # and a run cannot be shown to recover from a failure that never happens.
    # These three are what a freshness check and a completeness check are for.
    #
    # Applied by the sink, not by mutating rows, because the failure is in the
    # delivery rather than the content.

    def file_failure(self, path, rows):
        """
        Decide whether this file drop fails, and how.

        Returns (action, payload):
          ("ok",        rows)   deliver normally
          ("missing",   None)   the file never arrives
          ("truncated", subset) the file arrives short
        """
        if not self.enabled or not rows:
            return "ok", rows

        if self._hit("missing_file"):
            self._record(path, "missing_file", path, None,
                         f"{len(rows)} rows expected", "no file delivered",
                         "freshness_check")
            return "missing", None

        if self._hit("truncated_file"):
            # short enough to trip a >=95%-of-expected rule, not so short that
            # it looks like a different file
            keep = max(1, int(len(rows) * self.rng.uniform(0.55, 0.93)))
            self._record(path, "truncated_file", path, None,
                         f"{len(rows)} rows", f"{keep} rows delivered",
                         "completeness_check")
            return "truncated", rows[:keep]

        return "ok", rows

    # -- row-level batch mutations ----------------------------------------
    def mutate_row(self, source, row, key_field, required_fields=(),
                   coded_fields=(), numeric_fields=(), date_pairs=()):
        """
        Applies at most a couple of defects to one row. Returns (row, dup_row)
        where dup_row is a duplicate to append if the duplicate defect fired.
        """
        if not self.enabled:
            return row, None
        key = row.get(key_field)
        dup = None

        if required_fields and self._hit("null_required_field"):
            f = self.rng.choice(list(required_fields))
            if row.get(f) is not None:
                self._record(source, "null_required_field", key, f, row[f], None,
                             "completeness: required field not null")
                row[f] = None

        if coded_fields and self._hit("invalid_code_value"):
            f = self.rng.choice(list(coded_fields))
            if row.get(f) is not None:
                bad = self.rng.choice(["ZZZ", "UNKNOWN_CODE", "999", "-", "N/A"])
                self._record(source, "invalid_code_value", key, f, row[f], bad,
                             "validity: value in permitted domain")
                row[f] = bad

        if numeric_fields and self._hit("type_nonconformance"):
            f = self.rng.choice(list(numeric_fields))
            if row.get(f) is not None:
                bad = f"{row[f]} mg"
                self._record(source, "type_nonconformance", key, f, row[f], bad,
                             "type conformance: numeric parse")
                row[f] = bad

        if numeric_fields and self._hit("outlier_numeric"):
            f = self.rng.choice(list(numeric_fields))
            v = row.get(f)
            if isinstance(v, (int, float)):
                bad = v * self.rng.choice([-1, 1000, 10000])
                self._record(source, "outlier_numeric", key, f, v, bad,
                             "validity: range / plausibility")
                row[f] = bad

        for start_f, end_f in date_pairs:
            if self._hit("temporal_inversion"):
                s, e = row.get(start_f), row.get(end_f)
                if s is not None and e is not None:
                    self._record(source, "temporal_inversion", key,
                                 f"{start_f}/{end_f}", f"{s} -> {e}", f"{e} -> {s}",
                                 "temporal validity: start <= end")
                    row[start_f], row[end_f] = e, s

        if self._hit("duplicate_row"):
            dup = dict(row)
            self._record(source, "duplicate_row", key, key_field, key, key,
                         "uniqueness: primary key distinct")

        return row, dup

    def orphan_key(self, source, row, key_field, fk_field):
        """Point a foreign key at something that does not exist."""
        if not self.enabled or not self._hit("orphan_foreign_key"):
            return row
        original = row.get(fk_field)
        if original is None:
            return row
        bad = f"{original}-ORPHAN"
        self._record(source, "orphan_foreign_key", row.get(key_field), fk_field,
                     original, bad, "referential integrity: FK resolves")
        row[fk_field] = bad
        return row

    # -- stream mutations -------------------------------------------------
    def mutate_event(self, source, event):
        """Returns (events_to_send). May duplicate or delay."""
        if not self.enabled:
            return [event]
        out = [event]

        if self._hit("duplicate_event_id"):
            self._record(source, "duplicate_event_id", event["event_id"],
                         "event_id", event["event_id"], event["event_id"],
                         "uniqueness: dedupe on stable event_id")
            out.append(dict(event))

        if self._hit("late_event"):
            delay = self.rng.choice([90, 300, 900, 3600])
            original = event["ingest_time"]
            event["ingest_time"] = event["ingest_time"] + timedelta(seconds=delay)
            self._record(source, "late_event", event["event_id"], "ingest_time",
                         original, event["ingest_time"],
                         "late-data handling: watermark / event-time policy")

        if self._hit("null_required_field"):
            p = event.get("payload", {})
            if "value_num" in p and p["value_num"] is not None:
                self._record(source, "null_required_field", event["event_id"],
                             "payload.value_num", p["value_num"], None,
                             "completeness: required field not null")
                p["value_num"] = None

        return out

    def summary(self):
        counts = {}
        for d in self.log:
            counts[d["defect_type"]] = counts.get(d["defect_type"], 0) + 1
        return counts
