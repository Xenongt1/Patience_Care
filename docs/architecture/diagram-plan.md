# Architecture Diagram Plan

## Status

- **Artifact:** Design brief for the platform architecture diagram
- **Platform:** Proposed Microsoft Fabric and Azure architecture
- **Diagram baseline decision:** Microsoft Fabric and Azure selected by the project owner on
  2026-07-24 for this design phase
- **Source of truth:** `client-request.md`
- **Related guidance:** `AGENTS.md`
- **Status:** Proposed; subject to architecture, security, compliance, and cost review

## Purpose

This document defines the content, structure, assumptions, and visual approach for the Meridian
Health Network patient care and hospital operations analytics architecture diagram. The diagram
must show an end-to-end production design covering batch and streaming data paths, medallion data
layers, analytics serving, governance, operations, and controlled promotion across environments.

The architecture is intentionally presented as a proposed Microsoft Fabric and Azure design. The
diagram baseline is approved for design work, but production implementation remains subject to
security, compliance, capability, cost, and architecture review.

## Business Drivers

The diagram must make it possible to trace the architecture to these required outcomes:

- Measure patient wait times consistently across the network.
- Provide a near-real-time view of bed capacity and operational risk.
- Compare staffing coverage with patient load by facility, department, and shift.
- Analyze readmissions by facility and diagnosis.
- Identify denied or delayed claims and revenue at risk.
- Identify current and historical pharmacy stockout risk.
- Refresh scheduled sources without manual intervention.
- Reflect vitals and prescription events in operational views within minutes.
- Recover failed or duplicate ingestion without manual data correction.
- Provide visible, attributable data-quality results.
- Support separate executive and operational/clinical reporting experiences.

## Confirmed Requirements

The following requirements come directly from `client-request.md`:

- Batch file ingestion from a secure document library and secure cloud file storage.
- Near-real-time vital-sign and prescription event ingestion through a Kafka-compatible service.
- Bronze/raw, silver/cleansed, and gold/curated data layers.
- Automated orchestration, monitoring, alerting, retry, and recovery.
- Completeness, validity, and referential-integrity checks with visible outcomes.
- Separate development, test/UAT, and production environments.
- Controlled promotion so untested changes do not reach production.
- Executive and operational/clinical dashboards over curated data.
- Least-privilege access, protection of PHI/PII, and auditable governance.
- Documentation for architecture, data models, governance, and operations.

## Proposed Architecture Decisions

These are diagram assumptions, not yet confirmed implementation decisions:

- Use Microsoft Fabric as the primary analytics platform and OneLake as its logical data plane.
- Use separate Fabric workspaces for development, UAT, and production.
- Use one Bronze, Silver, and Gold Lakehouse per environment initially. Split layers into separate
  workspaces only if access boundaries or scaling requirements justify the added complexity.
- Use Fabric Eventstream's own Kafka-compatible custom endpoint as the ingestion point for vitals and
  prescription events — **not** Azure Event Hubs. Decided 2026-08-26, superseding the Azure Event
  Hubs / Event Hubs Capture design below. `GENERATOR-README.md` flagged this explicitly: Event Hubs
  adds an Azure resource, cost, and a hop with no benefit over Eventstream's native endpoint for this
  workload. See "Real-Time Ingestion Path (superseded design)" below for what this replaces.
- Use a second Eventstream destination writing raw events to Bronze Lakehouse Delta tables (one per
  topic) as the durable replay archive — the Fabric-native equivalent of Event Hubs Capture, with no
  extra Azure resource and no OneLake shortcut hop required.
- Use Fabric Eventstream for streaming connection, routing, and filtering, and Eventhouse with a KQL
  database for hot operational analytics, quarantine (malformed/invalid/excessively-late events with
  reason codes), and dedup (materialized views keyed on the generator's deterministic event_id).
- Use Dataflow Gen2 with the SharePoint Folder connector for SharePoint Online document-library
  files. Use Fabric Data Factory pipeline Copy activity for supported cloud-file sources.
- Use Fabric Spark and SQL notebooks for deterministic medallion transformations and quarantine.
- Use a Direct Lake semantic model over Gold for the executive report.
- Use a DirectQuery semantic model over Eventhouse for the operational report.
- Use Microsoft Entra groups, workspace identities, OneLake security, and semantic-model RLS/OLS.
- Use Git-backed development and Fabric deployment pipelines for definition promotion.

## Architecture Decision Context

Microsoft Fabric and Azure are the selected baseline for this design phase because they provide an
integrated path across OneLake medallion storage, Spark/SQL transformation, Real-Time Intelligence,
Power BI semantic models, Microsoft Entra identities, and deployment pipelines. This reduces the
number of independently operated analytics services while preserving distinct batch and streaming
paths.

Alternatives considered at this level are a cloud-agnostic logical design, an AWS-native analytics
stack, and an Azure composition centered on separately operated services such as Databricks and
Azure Data Explorer. They remain valid alternatives if Fabric cannot meet validated residency,
private-connectivity, disaster-recovery, workload-isolation, feature-maturity, or cost requirements.

The main Fabric tradeoffs to validate are tenant and region capability, preview-item deployment
support, Eventstream and private-network behavior, Workspace Monitoring retention/connectivity,
capacity contention, and the operational coupling created by using one SaaS analytics platform.
This section records the current decision context; a formal ADR should supersede it before build.

## Freshness Boundary and Source Gaps

The currently confirmed source description supports minutes-level freshness only for vital-sign and
prescription-issuance events. Bed capacity, encounters/admissions, staffing schedules, and pharmacy
inventory are described as daily or weekly files. Therefore:

- DirectQuery cannot make batch source data current; the operational report must show freshness by
  domain and must not present stale bed or staffing data as live.
- A near-real-time ADT/census/bed-state feed is required to provide a genuinely current bed-capacity
  view.
- A staffing-actual or clock/assignment change feed is required to provide genuinely current staffing
  coverage rather than scheduled staffing.
- Prescription issuance does not prove dispensing or decrement inventory. Current stockout risk
  needs inventory movements or more frequent inventory snapshots.
- These enhanced feeds are open source-contract requirements. Until provided, the associated report
  tiles inherit the daily/weekly source cadence.

## Architecture Views

### Primary View: End-to-End Data Platform

The initial `.drawio` artifact will contain a single end-to-end production view. It will organize
components from left to right:

1. Source systems.
2. Batch and streaming ingestion.
3. Durable raw storage and Fabric Bronze.
4. Silver cleansing, conformance, and quarantine.
5. Gold dimensional models and KPI tables.
6. Hot operational analytics.
7. Semantic models, reports, and report audiences.
8. Cross-cutting governance, identity, observability, orchestration, and delivery controls.

### Follow-On Views

Create these only if the primary diagram becomes too dense or a review needs deeper detail:

- Environment topology and deployment promotion.
- Network and private connectivity.
- Identity, authorization, masking, and audit controls.
- Batch restart, streaming replay, and disaster-recovery paths.
- KPI lineage from source contracts to semantic measures.

## Component Inventory

| Zone | Logical capability | Proposed service | Diagram role |
|---|---|---|---|
| Sources | Clinical records | EHR extracts | Batch patient, encounter, admission, and diagnosis data |
| Sources | Workforce | Staff scheduling system | Batch schedules and shift assignments |
| Sources | Finance | Billing and claims clearinghouse | Batch claims, denials, and payments |
| Sources | Pharmacy | Pharmacy management system | Batch inventory plus prescription events |
| Sources | Capacity | Bed management snapshots | Batch facility, unit, and bed-state records |
| Sources | Vitals | Bedside monitoring devices | Near-real-time vital-sign events |
| Sources | Current patient flow | Proposed ADT/census/bed-state feed | Required to make capacity operationally current |
| Sources | Current staffing | Proposed staffing-actual feed | Required to make staffing coverage operationally current |
| Batch ingestion | Document library | Dataflow Gen2 SharePoint Folder connector | Land SharePoint Online document-library files |
| Batch ingestion | Cloud files | Fabric Data Factory pipeline Copy activity | Land ADLS/Blob/S3/SFTP extracts after source confirmation |
| Batch retention | Immutable source copy | Source version retention or Graph/Logic Apps staging to ADLS Gen2 | Preserve exact inputs when SharePoint retention is insufficient |
| Streaming ingress | Kafka-compatible endpoint | Fabric Eventstream custom endpoint | Two logical topics (vitals, prescriptions), no Azure Event Hubs |
| Streaming archive | Replay store | Eventstream second destination to Bronze Lakehouse Delta tables | Durable replay and recovery archive, Fabric-native |
| Streaming routing | Stream connection and routing | Fabric Eventstream | Route, filter by event type, and lightly shape event streams |
| Hot analytics | Operational event store | Fabric Eventhouse and KQL database | Valid events, quarantine, reference data, and aggregates |
| Data plane | Logical analytics storage | Microsoft OneLake | Shared Fabric data plane across Lakehouse items |
| Bronze | Immutable source-aligned layer | Fabric Bronze Lakehouse | Raw batch files, raw streamed events (Eventstream archive destination), metadata |
| Silver | Cleansed and conformed layer | Fabric Silver Lakehouse | Standardized entities, deduplication, tokenized identifiers |
| Gold | Curated dimensional layer | Fabric Gold Lakehouse | Facts, dimensions, KPI tables, reconciliation outputs |
| Processing | Transformations | Fabric Spark/SQL notebooks | Deterministic validation, conformance, and publication |
| Quality | DQ gate and quarantine | Delta/KQL quality and quarantine tables | Checks, reason codes, run metrics, publication gate |
| Batch serving | Executive analytics | Direct Lake semantic model | Governed measures over Gold |
| Hot serving | Operational analytics | DirectQuery semantic model | Near-real-time measures over Eventhouse |
| Consumption | Executive dashboard | Power BI report and app audience | Network and facility headline KPIs and trends |
| Consumption | Operational dashboard | Power BI report and app audience | Facility/unit/shift risks requiring action |
| Orchestration | Schedules and dependencies | Fabric Data Factory pipelines | Cadence, retries, checkpoints, and Gold publication gate |
| Monitoring | Fabric job events | Real-Time hub/Eventstream plus Fabric Activator | Route job, freshness, quality, and event-threshold rules to approved destinations |
| Monitoring | Fabric telemetry | Monitoring Hub and Workspace Monitoring to a dedicated monitoring Eventhouse | Investigation, dashboards, retention, and operational evidence; not a notification service by itself |
| Monitoring | Streaming ingress | Azure Monitor alert rules plus Action Groups | Eventstream throughput, availability, throttling, and on-call routing |
| Governance | Catalog and lineage | Microsoft Purview | Classification, lineage, ownership, and discovery |
| Identity | Authentication and authorization | Microsoft Entra ID | Groups, managed/workspace identities, least privilege |
| Secrets | External secrets/certificates | Azure Key Vault where needed | Secret and certificate references; never values on the diagram |
| Delivery | Versioning and promotion | Git plus Fabric deployment pipelines | Pull-request approval and Dev to UAT to Prod promotion |
| Audit | Activity evidence | Entra, Fabric/Power BI, Azure resource, and deployment logs | Coverage matrix, export, retention, and known gaps required |

## Planned Data Flows

### Batch Path

1. EHR, staffing, claims, pharmacy inventory, and bed-capacity extracts arrive through SharePoint
   Online or an approved cloud-file endpoint.
2. Exact source-file versions are retained at the source or staged immutably before transformation.
   Dataflow Gen2 or a Fabric Data Factory pipeline lands source-aligned data in Bronze with source,
   ingestion UTC, schema version, file version or checksum, correlation ID, and pipeline run ID.
3. Notebooks validate schemas, standardize fields, deduplicate records, tokenize identifiers where
   required, and write valid rows to Silver.
4. Invalid rows are written to quarantine with actionable reason codes. Run-level quality metrics are
   persisted and tied to the pipeline run.
5. A quality gate blocks Gold publication for critical failures.
6. Deterministic transformations publish conformed facts, dimensions, KPI tables, and reconciliation
   results to Gold.
7. A Direct Lake semantic model supplies the executive Power BI report and reusable governed
   measures.

### Streaming Path

1. Bedside monitors and the pharmacy system publish events directly to Fabric Eventstream's
   Kafka-compatible custom endpoint, as two logical topics (`patient-vitals`,
   `prescription-events`) — no Azure Event Hubs in front of it. Proposed ADT/bed-state and
   staffing-actual feeds join this path only after source contracts are approved.
2. Eventstream filters the combined stream by `event_type` into two derived streams and fans each
   out to two destinations: the Eventhouse (hot path) and a Bronze Lakehouse Delta table (durable
   replay archive — the Fabric-native equivalent of Event Hubs Capture, no OneLake shortcut needed
   since Eventstream writes into OneLake directly).
3. KQL raw tables land the envelope plus the JSON payload as a `dynamic` column. Update-policy
   functions expand each raw row into a typed table.
4. Separate, non-transactional update policies flag rows with unparseable required fields or
   excessive event-time/ingest-time skew into per-stream quarantine tables with reason codes,
   so a validation bug can never block the main raw landing.
5. Materialized views deduplicate on the generator's deterministic `event_id` (`arg_max` by
   `ingest_time`) — at-least-once delivery redelivers the same id rather than minting a new one, so
   dedup does not need a separate identity scheme.
6. A scheduled pipeline copy/upsert loads approved Gold reference snapshots into effective-dated KQL
   lookup tables. Query-time joins select the reference valid for the event or reporting time.
   Materialized views aggregate event facts or immutable keys; reference corrections trigger an
   explicit correction/rebuild rather than being assumed to recalculate existing aggregates.
7. A DirectQuery semantic model supplies the operational Power BI report. Automatic page refresh or
   change detection, supported capacity settings, visual query latency, and concurrency must be
   configured and tested to meet the minutes-level objective.
8. Operational aggregates use a shared metric contract and are reconciled to the authoritative Gold
   outputs when the same events complete the historical medallion path.

### Control and Recovery Paths

- Batch pipeline failure triggers an alert and retries from a safe checkpoint.
- Fabric job events route through Real-Time hub/Eventstream to Fabric Activator rules and approved
  notification destinations. Workspace Monitoring persists telemetry to a dedicated monitoring
  Eventhouse for investigation.
- Azure Monitor alert rules route Eventstream health alerts (throughput, throttling) through Azure
  Monitor Action Groups; they do not route through Fabric Activator by default.
- Duplicate files or events are detected using source identifiers, checksums, and stable event IDs.
- Streaming replay reads from the Bronze Lakehouse archive tables rather than relying on the
  Eventhouse hot-store retention window. A controlled replay job republishes archived events with
  their stable event IDs to rebuild the hot path without duplicating accepted events.
- Malformed or incompatible data is quarantined, not silently discarded.
- Streaming jobs are continuously monitored; they are not started and stopped by batch pipelines.
- Gold publication records input versions and pipeline runs to support reconciliation and rollback.
- The operational report labels each domain with its source timestamp and freshness status. Hot
  indicators are provisional until reconciled to Gold.

## Medallion Data Domains

### Bronze

- Source-aligned patient, encounter, admission, claim, payment, schedule, inventory, bed, vital, and
  prescription records.
- Immutable or equivalently replayable payloads.
- Ingestion, source, schema, event-time, timezone, and correlation metadata.

### Silver

- Conformed patient tokens, facilities, departments, units, staff, encounters, diagnoses, payers,
  medications, beds, and shifts.
- Standardized timestamps stored in UTC with source timezone information retained.
- Accepted records, quarantined records, DQ results, and cross-source identity mappings held under
  stricter access controls.

### Gold

- Patient wait-time fact.
- Bed occupancy and capacity snapshots.
- Staffing coverage and patient-load fact.
- Readmission cohort and outcome fact.
- Claims status, denial, delay, and revenue-at-risk fact.
- Pharmacy inventory and stockout-risk fact.
- Facility, department, unit, shift, date/time, diagnosis, payer, and medication dimensions.
- Reconciled KPI tables and semantic measures for both reporting audiences.

## Quality Gates

The diagram will show a visible quality gate between Silver and Gold and a quarantine path. The
minimum checks are:

- Completeness of required source and business fields.
- Validity of codes, ranges, timestamps, and state transitions.
- Uniqueness of source records and event identifiers where applicable.
- Freshness against each source contract and reporting target.
- Referential integrity among patients, staff, facilities, departments, encounters, claims,
  prescriptions, beds, and medications.
- Reconciliation of accepted, quarantined, duplicated, and published record counts.

## Security and Governance

The primary diagram must make these controls visible without exposing PHI/PII:

- Separate Dev, UAT, and Prod workspaces and environment-specific identities.
- Microsoft Entra group-based access and managed/workspace identities.
- Least-privilege workspace and item permissions.
- OneLake security plus semantic-model row-level and object-level security.
- Tokenization or masking of direct identifiers outside restricted clinical use cases.
- Encryption in transit and at rest.
- Microsoft Purview classification, ownership, and lineage.
- An explicit audit coverage matrix spanning Entra, Fabric/Power BI activities, Azure resources,
  deployments, pipeline runs, and data-quality outcomes, including known workload-level read gaps.
- Export and retention controls for audit evidence plus privileged-access monitoring and escalation.
- Approved retention, correction, and deletion handling.

Controls are described as HIPAA-aligned or HIPAA-consistent pending formal organizational review.
The operational report is an analytics aid and must not be presented as a validated primary clinical
alarm system.

## Environment and Promotion Model

The diagram will include a compact promotion lane:

```text
Git branch and pull request -> Development workspace -> UAT workspace -> Production workspace
```

- Definitions move through Git and Fabric deployment pipelines after review and automated checks.
- Data, secrets, permissions, and credentials do not move as ordinary deployment-pipeline content.
- Environment-specific connections and Direct Lake bindings must be parameterized or rebound.
- Each environment is an isolated stack including Fabric workspaces, Eventstream, replay storage,
  identities, connections, monitoring, and source endpoints or safe substitutes.
- Development and UAT use synthetic or approved de-identified fixtures and cannot connect to
  production PHI sources by default.
- Production access and capacity are isolated. Development and UAT may initially share a
  non-production capacity only if security boundaries and representative testing remain adequate.

## Visual Layout

- Use a wide landscape canvas with source systems on the left and consumers on the right.
- Separate batch and streaming lanes while showing where they converge for historical analytics.
- Place Bronze, Silver, and Gold inside a prominent OneLake/Fabric boundary.
- Place Eventhouse as a parallel hot path connected to the operational semantic model.
- Show mixed source freshness and the proposed ADT/bed/staffing feed gap explicitly.
- Show governance, security, orchestration, observability, and delivery as cross-cutting bands.
- Use short edge labels such as `daily/weekly`, `Kafka`, `capture`, `validate`, `DQ gate`,
  `Direct Lake`, and `DirectQuery`.
- Use no patient names, identifiers, realistic clinical values, secrets, tenant names, or connection
  strings.
- Mark proposed service choices visually and include a short legend.

## Planned Diagram Artifact

- **Path:** `docs/architecture/fabric-platform.drawio`
- **Format:** Editable draw.io XML
- **Page:** End-to-End Architecture
- **Initial scope:** Logical and service-level architecture; no subnet-level network design
- **Icon approach:** Bundled diagrams.net Azure icons where verified; simple labeled Fabric shapes
  where a standard built-in Fabric stencil cannot be relied on

## Open Decisions

- Confirm Microsoft Fabric as the selected platform through an architecture decision record.
- Confirm SharePoint Online versus SharePoint Server and its immutable retention guarantees.
- Confirm each cloud-file provider, protocol, schedule, and network route.
- Define streaming event rates, message sizes, partitioning, retention, RPO/RTO, and late-event
  tolerance.
- Confirm ADT/census/bed-state, staffing-actual, and inventory-movement feeds or accept batch-limited
  freshness for those operational domains.
- Select the Fabric region and validate residency, availability, and disaster-recovery requirements.
- Define patient identity matching, tokenization, re-identification, and master-data ownership.
- Approve KPI definitions, grains, exclusions, and reconciliation authorities.
- Decide whether private endpoints are mandatory for each service and resolve limitations in Fabric
  monitoring and event connectivity.
- Select Fabric capacity tiers and Eventstream throughput level after representative performance
  testing.
- Decide whether Microsoft Purview Data Quality supplements the pipeline-native quality gate.
- Validate the exact tenant support matrix for deployment of semantic models, reports, Eventstreams,
  and connection bindings.
- Validate Power BI automatic page refresh/change detection settings and capacity limits.
- Approve the common metric contract and reconciliation tolerances between Eventhouse indicators and
  authoritative Gold KPIs.
- Complete an audit-source coverage matrix, retention/export design, and compensating controls for
  reads not captured by workload-level audit events.
- Validate the Microsoft BAA/DPA, each selected service and preview feature's eligibility and audit
  scope, shared responsibilities, incident/breach handling, and required audit retention before PHI
  is admitted.

## Acceptance Criteria

The diagram is ready for architecture review when it:

- Covers every required source and both ingestion modes.
- Shows replayable Bronze, cleansed Silver, curated Gold, and the hot operational path.
- Shows quality checks, quarantine, retries, replay, monitoring, and alerts.
- Shows separate executive and operational reporting paths.
- Shows identity, access controls, lineage, audit, and environment isolation.
- Shows Dev to UAT to Prod promotion without implying that data or secrets are promoted.
- Distinguishes confirmed requirements from proposed platform choices.
- Contains no real or realistic PHI/PII, credentials, or confidential environment details.

## References

- `client-request.md`
- `AGENTS.md`
- [Microsoft Fabric Eventstreams overview](https://learn.microsoft.com/en-us/fabric/real-time-intelligence/event-streams/overview)
- [SharePoint Folder connector](https://learn.microsoft.com/en-us/fabric/data-factory/connector-sharepoint-folder)
- [Azure Event Hubs for Apache Kafka](https://learn.microsoft.com/en-us/azure/event-hubs/azure-event-hubs-apache-kafka-overview)
- [Microsoft Fabric medallion architecture](https://learn.microsoft.com/en-us/fabric/onelake/onelake-medallion-lakehouse-architecture)
- [Direct Lake overview](https://learn.microsoft.com/en-us/fabric/fundamentals/direct-lake-overview)
- [Fabric deployment pipelines](https://learn.microsoft.com/en-us/fabric/cicd/deployment-pipelines/intro-to-deployment-pipelines)
- [Microsoft Purview and Fabric](https://learn.microsoft.com/en-us/fabric/governance/microsoft-purview-fabric)

## Change Log

| Date | Status | Change |
|---|---|---|
| 2026-07-24 | Proposed | Initial Fabric/Azure architecture diagram plan based on `client-request.md` |
