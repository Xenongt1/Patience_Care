"""
Output sinks.

Batch  : local filesystem  ->  OneLake Files (ADLS Gen2 API)
Stream : local JSONL       ->  Fabric Eventstream Kafka endpoint

Both are behind one interface so the generator does not care where the data
lands. That means you can generate and inspect everything before any Fabric
workspace exists, then flip a flag.

OneLake speaks the ADLS Gen2 API at https://onelake.dfs.fabric.microsoft.com --
workspace is the filesystem, path is <Lakehouse>.Lakehouse/Files/<path>.
Fabric Eventstream exposes a Kafka endpoint on a published custom endpoint
source: bootstrap server + topic + SASL_SSL/PLAIN with username
"$ConnectionString". Both verified against Microsoft Learn.
"""

import csv
import gzip
import io
import json
import os
from datetime import date, datetime
from decimal import Decimal


def _serialize(v):
    if v is None:
        return ""
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, date):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, Decimal):
        return str(v)
    return v


def _json_default(v):
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    raise TypeError(f"not JSON serialisable: {type(v)}")


# ---------------------------------------------------------------------------
# Batch sinks
# ---------------------------------------------------------------------------

class BatchSink:
    def write_csv(self, path, rows, fieldnames=None):
        raise NotImplementedError

    def write_bytes(self, path, data: bytes):
        raise NotImplementedError

    def close(self):
        pass


class LocalBatchSink(BatchSink):
    """Mirrors the target OneLake layout on local disk."""

    def __init__(self, root):
        self.root = root
        self.written = []

    def _full(self, path):
        full = os.path.join(self.root, path)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        return full

    def write_csv(self, path, rows, fieldnames=None):
        # No rows and no known column order -- nothing writable. With explicit
        # fieldnames an empty feed still lands as a header-only file, so a
        # legitimately quiet day is distinguishable from a partition that never
        # arrived. Callers that pass fieldnames are opting into that.
        if not rows and not fieldnames:
            return 0
        fieldnames = fieldnames or list(rows[0].keys())
        full = self._full(path)
        with open(full, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            for r in rows:
                w.writerow({k: _serialize(r.get(k)) for k in fieldnames})
        self.written.append((path, len(rows)))
        return len(rows)

    def write_bytes(self, path, data: bytes):
        full = self._full(path)
        with open(full, "wb") as fh:
            fh.write(data)
        self.written.append((path, len(data)))
        return len(data)


class OneLakeBatchSink(BatchSink):
    """
    Writes into a Fabric lakehouse's Files area.

    Requires:  pip install azure-storage-file-datalake azure-identity
    Auth:      DefaultAzureCredential (az login, or env service principal)
    """

    def __init__(self, workspace, lakehouse, prefix="landing"):
        from azure.identity import DefaultAzureCredential
        from azure.storage.filedatalake import DataLakeServiceClient
        self.prefix = prefix
        self.lakehouse = lakehouse
        self.client = DataLakeServiceClient(
            "https://onelake.dfs.fabric.microsoft.com",
            credential=DefaultAzureCredential(),
        )
        self.fs = self.client.get_file_system_client(workspace)
        self.written = []

    def _target(self, path):
        return f"{self.lakehouse}.Lakehouse/Files/{self.prefix}/{path}"

    def write_bytes(self, path, data: bytes):
        fc = self.fs.get_file_client(self._target(path))
        fc.upload_data(data, overwrite=True)
        self.written.append((path, len(data)))
        return len(data)

    def write_csv(self, path, rows, fieldnames=None):
        # No rows and no known column order -- nothing writable. With explicit
        # fieldnames an empty feed still lands as a header-only file, so a
        # legitimately quiet day is distinguishable from a partition that never
        # arrived. Callers that pass fieldnames are opting into that.
        if not rows and not fieldnames:
            return 0
        fieldnames = fieldnames or list(rows[0].keys())
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: _serialize(r.get(k)) for k in fieldnames})
        self.write_bytes(path, buf.getvalue().encode("utf-8"))
        return len(rows)


# ---------------------------------------------------------------------------
# Stream sinks
# ---------------------------------------------------------------------------

class StreamSink:
    def send(self, topic, key, event):
        raise NotImplementedError

    def flush(self):
        pass

    def close(self):
        pass


class LocalStreamSink(StreamSink):
    """
    JSONL per topic. Also doubles as the raw replay archive.

    Compressed by default, because the vitals feed is by far the largest
    artefact this generator produces and it is pathologically compressible:
    every event repeats the same 14 envelope keys to carry a single number.
    Measured on a 122 MB vitals sample, gzip level 6 gives 17.4x (570 MB/day
    of raw vitals becomes 33 MB/day) at ~177 MB/s. Level 1 is 2.4x faster but
    only 13.4x, which is the wrong trade when disk is the binding constraint.

    Spark, pandas and Fabric all read .jsonl.gz transparently. The cost is that
    gzip is not splittable, so a single huge member cannot be read in parallel
    shards -- irrelevant at the per-run sizes here.
    """

    GZIP_LEVEL = 6

    def __init__(self, root, compress=True):
        self.root = root
        self.compress = compress
        os.makedirs(root, exist_ok=True)
        self._files = {}
        self.counts = {}
        self.paths = {}

    def send(self, topic, key, event):
        if topic not in self._files:
            ext = ".jsonl.gz" if self.compress else ".jsonl"
            path = os.path.join(self.root, f"{topic}{ext}")
            self._files[topic] = (
                gzip.open(path, "wt", encoding="utf-8", compresslevel=self.GZIP_LEVEL)
                if self.compress else open(path, "w", encoding="utf-8"))
            self.paths[topic] = path
            self.counts[topic] = 0
        self._files[topic].write(json.dumps(event, default=_json_default) + "\n")
        self.counts[topic] += 1

    def close(self):
        for fh in self._files.values():
            fh.close()


class FabricEventstreamSink(StreamSink):
    """
    Fabric Eventstream Kafka endpoint.

    Get these three values from the Eventstream UI: add a custom endpoint
    source, publish, select the source tile, then the Kafka tab ->
    SAS Key Authentication page.

    Requires:  pip install confluent-kafka
    """

    def __init__(self, bootstrap_server, connection_string, default_topic,
                 archive_sink: StreamSink = None):
        from confluent_kafka import Producer
        self.producer = Producer({
            "bootstrap.servers": bootstrap_server,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": "$ConnectionString",
            "sasl.password": connection_string,
            "client.id": "meridian-datagen",
            "linger.ms": 50,
            "batch.num.messages": 1000,
        })
        self.default_topic = default_topic
        # Eventstream has no Event Hubs Capture equivalent, so we keep our own
        # raw archive for the replay path. Alternatively attach a second
        # Lakehouse destination on the Eventstream, which is more Fabric-native.
        self.archive = archive_sink
        self.counts = {}

    def send(self, topic, key, event):
        payload = json.dumps(event, default=_json_default).encode("utf-8")
        self.producer.produce(self.default_topic or topic,
                              key=str(key).encode(), value=payload)
        self.counts[topic] = self.counts.get(topic, 0) + 1
        if self.counts[topic] % 5000 == 0:
            self.producer.poll(0)
        if self.archive:
            self.archive.send(topic, key, event)

    def flush(self):
        self.producer.flush(30)

    def close(self):
        self.flush()
        if self.archive:
            self.archive.close()
