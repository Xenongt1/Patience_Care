# Fabric/Azure Services — Detailed Functions and Batch-vs-Streaming Comparison

## Status

- **Scope:** Explains what each proposed service does and why it is positioned where it is in
  `patient-care-architecture-diagram-draft.drawio`, then compares the batch/historical path to
  the near-real-time operational path service-by-service.
- **Source of truth for requirements:** `client-request.md`
- **Source of truth for the proposed design:** `docs/architecture/diagram-plan.md`
- **Status:** Proposed design explanation only. Nothing described here is provisioned yet — see
  the "Proposed, not built" note in `diagram-plan.md`.

---

## 1. Batch / Historical Path — service functions

This path exists to answer questions that don't need to be current to the minute: staffing
trends, financial performance, readmissions, historical bed utilization. Its job is
**trustworthiness and completeness**, not speed.

### Dataflow Gen2 (SharePoint Folder connector)
- **What it is:** A low-code, Power Query–based data preparation item inside Fabric's Data
  Factory experience, purpose-built to connect to a SharePoint Online document library folder.
- **Function here:** Picks up staff-schedule files that are manually managed internally and
  dropped into the secure document library. Handles the connector-level auth to SharePoint and
  a first pass of shaping before landing data in Bronze.
- **Why this tool and not the pipeline Copy activity:** SharePoint document libraries are
  file-and-folder oriented with metadata (versions, checked-out state) that Dataflow Gen2's
  SharePoint connector understands natively; a generic Copy activity does not.

### Fabric Data Factory (Pipeline Copy activity)
- **What it is:** The orchestration/pipeline engine inside Fabric (the successor to Azure Data
  Factory pipelines, now native to a Fabric workspace).
- **Function here:** Lands the *system-generated* extracts — EHR encounters/admissions, claims
  and payment files, pharmacy inventory levels, bed-capacity snapshots — arriving from secure
  cloud file storage (blob/ADLS/SFTP-style endpoints) into Bronze. Also owns the schedule,
  retries, checkpoints, and the gate that blocks a Gold publish when data quality fails.
- **Why separate from Dataflow Gen2:** These sources are already structured extracts, not loose
  documents, so a straight Copy activity is simpler, cheaper, and easier to monitor than a
  low-code dataflow.

### Bronze Lakehouse (Fabric Lakehouse, source-aligned)
- **What it is:** A Fabric Lakehouse item — Delta tables plus a file area, backed by OneLake.
- **Function here:** The immutable, source-aligned landing zone. Every batch record keeps its
  original shape plus ingestion metadata (source, ingestion UTC, schema version, file
  version/checksum, correlation ID, pipeline run ID) so any downstream layer can be rebuilt from
  here without re-pulling the source.

### Silver Lakehouse (cleanse / conform / dedupe / tokenize)
- **What it is:** A second Fabric Lakehouse, populated by deterministic Fabric Spark/SQL
  notebooks.
- **Function here:** Standardizes field names and types, deduplicates records, tokenizes direct
  identifiers (patient name, DOB, government IDs) so downstream consumers work with pseudonymous
  keys, and produces the accepted/rejected split that feeds the quality gate.

### DQ Gate + Quarantine
- **What it is:** Not a separate product — a checkpoint implemented as notebook logic plus
  Delta quarantine tables, sitting between Silver and Gold.
- **Function here:** Runs completeness, validity, uniqueness, freshness, and referential-integrity
  checks. Records that fail are written to quarantine with a reason code instead of being
  silently dropped; run-level pass/fail metrics are persisted and tied to the pipeline run ID.
  Critical failures block Gold publication.

### Gold Lakehouse (facts / dimensions / KPI tables)
- **What it is:** The third Fabric Lakehouse in the medallion chain.
- **Function here:** Holds the conformed, dimensional model — patient wait-time fact, bed
  occupancy fact, staffing-vs-load fact, readmission fact, claims/denial fact, pharmacy
  stockout-risk fact, plus shared dimensions (facility, department, unit, shift, date, diagnosis,
  payer, medication). This is the single authoritative layer both dashboards are supposed to
  reconcile back to.

### Direct Lake semantic model
- **What it is:** A Power BI/Fabric semantic-model storage mode that reads Delta Parquet files
  in OneLake directly into memory, without a traditional Import refresh or a live query round
  trip per request.
- **Function here:** Backs the **executive report**. Because Gold only changes when a batch run
  completes, Direct Lake gives near-import-speed query performance without needing to manage a
  refresh schedule — it just picks up new Gold table versions.

### Executive report + app (Power BI)
- **Function here:** The audience-facing layer for C-suite/network administrators: wait time,
  occupancy, staffing, readmissions, claims/financial risk, rolled up network → facility. Row-
  level/object-level security (RLS/OLS) restricts what each viewer can see.

---

## 2. Near-Real-Time Operational Path — service functions

This path exists to answer "what's happening *right now*" — bedside vitals, prescription
events, minute-level operational risk. Its job is **low latency and durability of the raw
event stream**, with correctness caught up asynchronously.

### Azure Event Hubs (Kafka endpoint)
- **What it is:** An Azure-native, high-throughput event ingestion service that exposes a
  Kafka-compatible protocol endpoint.
- **Function here:** The landing point for two continuous producers — bedside vital-sign
  monitors and pharmacy prescription-issuance events — published over the Kafka protocol so
  existing Kafka producer libraries can be reused unchanged.

### Event Hubs Capture → ADLS Gen2 archive
- **What it is:** A built-in Event Hubs feature that automatically writes every ingested event
  to Azure Data Lake Storage Gen2 in its raw form, at no extra coding cost.
- **Function here:** The **durable replay archive**. If the hot path (Eventhouse) needs to be
  rebuilt, or a downstream bug requires reprocessing, this is the source of truth to replay from
  — the hot store's own retention window is not relied on for recovery.

### OneLake shortcut (Capture archive → Bronze)
- **What it is:** A OneLake feature that creates a reference/pointer to data sitting in another
  location (here, the ADLS Gen2 Capture archive) without physically copying it.
- **Function here:** Lets the *same* captured event data be treated as a Bronze source for
  historical/batch-style processing, so streaming events eventually flow into the same medallion
  lineage as batch records, for long-term analytics and reconciliation.

### Fabric Eventstream
- **What it is:** A no/low-code Fabric item for connecting to streaming sources and routing/
  lightly transforming events before they land somewhere else.
- **Function here:** Sits between Event Hubs and Eventhouse. Connects to the Kafka endpoint,
  does light shaping (e.g., splitting vitals vs. prescription events, basic enrichment), and
  routes the result into the KQL database.

### Fabric Eventhouse / KQL database
- **What it is:** A Real-Time Intelligence item purpose-built for high-ingest, low-latency
  event analytics, queried with Kusto Query Language (KQL).
- **Function here:** The **hot store**. Validates and deduplicates incoming events using stable
  event IDs and event time, separates malformed/late events into a KQL quarantine, joins events
  against effective-dated reference snapshots (latest staffing/bed data pushed from Gold), and
  maintains materialized aggregates for current operational risk.

### KQL quarantine
- **What it is:** Quarantine tables/functions inside the same KQL database, not a separate
  product.
- **Function here:** Catches malformed, invalid, or too-late events with reason codes and event
  IDs, mirroring the role Silver's quarantine plays in the batch path but at streaming latency.

### DirectQuery semantic model (over Eventhouse)
- **What it is:** A semantic-model storage mode that sends a live query to the underlying
  source (here, Eventhouse's SQL/KQL endpoint) every time a report visual refreshes, instead of
  caching data in the model.
- **Function here:** Backs the **operational/clinical report**. Because operational risk needs
  to reflect events from the last few minutes, DirectQuery trades some query latency for data
  that is current at request time — the opposite tradeoff from Direct Lake on the batch side.

### Operational / clinical report (Power BI)
- **Function here:** The audience-facing layer for operations managers, nursing supervisors,
  pharmacy managers: capacity, staffing, stockouts, and critical-event analytics, rolled up
  facility → unit → shift, each tile labeled with its own freshness/source cadence.

### Action notifications
- **Function here:** A logical box, not a single named product — represents alerts fired when
  freshness, pipeline, DQ, or operational thresholds are breached. In the proposed design this
  is realized through Fabric Activator rules (see cross-cutting section below), not a separate
  service.

---

## 3. Batch vs. Real-Time: side-by-side comparison

| Dimension | Batch / Historical Path | Near-Real-Time Operational Path |
|---|---|---|
| **Trigger** | Scheduled (daily/weekly) pipeline run | Continuous event stream, always running |
| **Ingestion service** | Dataflow Gen2 (documents) / Fabric Data Factory Copy activity (extracts) | Azure Event Hubs (Kafka endpoint) |
| **Durable raw copy** | Bronze Lakehouse itself is the immutable copy | Event Hubs Capture to ADLS Gen2 (separate from the hot store) |
| **Routing/shaping layer** | Fabric Data Factory pipeline | Fabric Eventstream |
| **Primary processing engine** | Fabric Spark/SQL notebooks (batch compute) | KQL functions and update policies inside Eventhouse (streaming compute) |
| **Storage engine** | Delta Lakehouses (Bronze/Silver/Gold) on OneLake | Eventhouse / KQL database (column store tuned for time-series ingest) |
| **Quality handling** | Silver→Gold DQ gate; quarantine as Delta tables with reason codes; can **block publication** | KQL quarantine tables keyed by event ID/event time; validation is continuous, not gate-and-block |
| **Deduplication key** | Source record identifiers / checksums | Stable event IDs + event time |
| **Reference data crossover** | N/A — Gold is the reference source | Gold's latest staffing/bed reference snapshots are pushed into effective-dated KQL lookup tables and joined at query time |
| **Serving semantic model** | Direct Lake (reads Delta files directly, no live query per request) | DirectQuery (live query per request against Eventhouse) |
| **Latency profile** | Minutes-to-hours (bound by batch schedule), but very cheap per query | Minutes end-to-end, but each report interaction costs a live query |
| **Report audience** | Executive report + app (C-suite, network admins) | Operational/clinical report (ops managers, nursing supervisors, pharmacy) |
| **Recovery/replay mechanism** | Rerun pipeline from checkpoint; Bronze is already replayable | Controlled replay job reads from Event Hubs Capture archive and republishes with original event IDs to rebuild the hot path without duplicating already-accepted events |
| **Authoritative reconciliation** | Gold *is* the authority | Eventhouse aggregates are provisional until reconciled against Gold once the same events complete the batch medallion path |
| **Failure mode handling** | Alert + retry from safe checkpoint (pipeline-native) | Continuously running job is monitored, not stopped/started by batch pipelines; malformed events quarantined, not discarded |
| **Freshness ceiling** | Bound by source contract (daily/weekly) — cannot be made "live" without new source feeds (ADT/census, staffing-actual) | Bound by event delivery + Eventstream/Eventhouse processing — genuinely minutes-level for vitals and prescriptions only |

### The key structural difference

The two paths are not just "slow vs. fast" versions of the same pipeline — they use **different
storage engines and different semantic-model modes because of it**:

- Batch treats **correctness before availability**: nothing reaches Gold until it passes the DQ
  gate, and Direct Lake only ever serves gate-approved data.
- Streaming treats **availability before full reconciliation**: events are validated and
  deduplicated in near-real time, but the operational report's numbers are explicitly
  provisional until the same events later complete the batch path and reconcile against Gold.
  This is why the diagram/plan requires every operational tile to be labeled with its own
  freshness status rather than presented as uniformly "live."

A second structural difference: **not all "operational-looking" domains actually have a
real-time source today**. Bed capacity and staffing are shown feeding both paths, but the
diagram plan flags that current source contracts only provide those as daily/weekly snapshots —
so the operational report's bed/staffing tiles inherit batch freshness until an ADT/census feed
and a staffing-actual feed are contracted, even though they sit visually in the "real-time" half
of the diagram.

---

## 4. Cross-cutting services — how each treats the two paths differently

These aren't part of either path exclusively, but they behave differently depending on which
path they're watching:

| Service | Batch path role | Streaming path role |
|---|---|---|
| **Fabric pipelines (orchestration)** | Owns schedule, retries, checkpoints, Gold gate | Not used to start/stop the stream — streaming jobs run continuously and are only monitored, not orchestrated on a schedule |
| **Monitoring Hub / Workspace Monitoring** | Tracks pipeline/notebook run history and duration | Tracks Eventstream and Eventhouse ingestion health via the same Workspace Monitoring Eventhouse |
| **Fabric Activator** | Fires on pipeline failure or DQ gate failure | Fires on operational thresholds (e.g., critical vital-sign rate, stockout risk) surfaced through Eventhouse |
| **Azure Monitor + Action Groups** | Not typically needed — Fabric-native monitoring covers batch | Watches Event Hubs infrastructure itself (throughput, throttling, availability) since that's an Azure resource outside Fabric's own monitoring |
| **Microsoft Purview** | Classifies/tracks lineage for Bronze→Silver→Gold tables | Classifies/tracks lineage for Eventhouse tables and the OneLake shortcut boundary |
| **Microsoft Entra ID / OneLake security / RLS-OLS** | Same identity model, applied to Lakehouse and Direct Lake semantic model | Same identity model, applied to Eventhouse and DirectQuery semantic model |
| **Git + Fabric deployment pipelines** | Promotes Lakehouse, notebook, and Direct Lake model *definitions* Dev→UAT→Prod | Promotes Eventstream, Eventhouse, and DirectQuery model *definitions* Dev→UAT→Prod — connection bindings must be re-pointed per environment either way |

---

## References

- `client-request.md`
- `docs/architecture/diagram-plan.md`
- `docs/architecture/patient-care-architecture-diagram-draft.drawio`
- [Microsoft Fabric medallion architecture](https://learn.microsoft.com/en-us/fabric/onelake/onelake-medallion-lakehouse-architecture)
- [Direct Lake overview](https://learn.microsoft.com/en-us/fabric/fundamentals/direct-lake-overview)
- [Microsoft Fabric Real-Time Intelligence overview](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/overview)
- [What is Fabric Activator?](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/data-activator/activator-introduction)
- [Azure Event Hubs for Apache Kafka](https://learn.microsoft.com/en-us/azure/event-hubs/azure-event-hubs-apache-kafka-overview)

## Change Log

| Date | Status | Change |
|---|---|---|
| 2026-08-12 | Draft | Initial detailed service explanation and batch-vs-streaming comparison |
