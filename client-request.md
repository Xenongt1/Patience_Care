# Project Request: Patient Care & Hospital Operations Analytics Platform

**From:** Office of the Chief Information Officer, Meridian Health Network
**To:** Data Engineering Project Team
**Subject:** Request for Proposal — Integrated Patient Care & Operations Analytics Platform

---

## 1. Who We Are

Meridian Health Network operates seven facilities across the Northeast, West, Midwest, and
South — general hospitals, a teaching hospital, a regional hospital, a community hospital, and
an urgent care center — serving several million patient visits a year. Our facilities run on a
mix of electronic health record (EHR) systems, bedside patient-monitoring equipment, a pharmacy
management system, a billing/claims clearinghouse, and a staff scheduling system. Each system
does its job well in isolation, but none of them talk to each other, and none of them give
leadership a unified view of how the network is actually performing.

## 2. The Business Problem

Our clinical and operations leadership are flying partially blind:

- **Patient wait times** in emergency and outpatient departments are trending in the wrong
  direction, and we don't have a reliable, network-wide way to measure or track them.
- **Staffing allocation** is largely manual. We suspect certain departments and shifts are
  chronically understaffed relative to patient load, but we can't currently prove it or act on
  it in a timely way.
- **Bed capacity** across our facilities is managed locally, unit by unit. There is no
  network-wide, near-real-time picture of where capacity is tight and where it is available,
  which matters enormously for transfers and surge planning.
- **Financial leakage** — denied claims, delayed payments, and avoidable readmissions — is
  eating into margins, and finance can only see it weeks after the fact via manual reporting.
- **Regulatory and accreditation reporting** (HIPAA and related quality-of-care reporting
  obligations) currently requires significant manual effort to assemble from disconnected
  sources, which is slow and error-prone.
- **Pharmacy stockouts** disrupt care delivery and we only find out after the fact.

We need a modern, integrated analytics platform that brings clinical, operational, and
financial data together so both executives and front-line operational/clinical managers can
see what's happening — and act on it — instead of reconstructing it after the fact.

## 3. Project Objectives

We are asking the data engineering team to design and build an end-to-end analytics platform
that:

1. Integrates data from across our clinical, operational, and financial systems into a single,
   trustworthy, well-governed platform.
2. Supports **both** near-real-time operational monitoring (patient flow, vitals, bed capacity)
   and **batch/historical analysis** (staffing trends, financial performance, outcomes).
3. Gives our executive team a network-wide view of performance, and gives our operational and
   clinical managers the detailed, drillable view they need to act day to day.
4. Is built with proper data governance from day one, given the sensitivity of the data
   involved (see Section 6).
5. Is delivered in a way we can trust to run in production — not just a one-off analysis —
   with a clear path from a development environment through testing to a live environment.

## 4. Success Criteria

We will consider this project successful if the delivered platform can demonstrate:

- **Data freshness:** Daily and weekly operational/financial data sources are refreshed on
  their expected cadence with no manual intervention; the real-time patient monitoring and
  prescription feeds are reflected in operational views within minutes of an event occurring.
- **Reliability:** Data pipelines run on an automated schedule with monitoring/alerting on
  failure, and can be demonstrated to recover from a failed run without manual data-fixing.
- **Data quality:** A documented data quality framework is in place, with automated checks
  (completeness, validity, referential integrity between patients/staff/facilities/claims) and
  visible pass/fail results — not just "the numbers looked fine to me."
- **Answers to our business questions**, at minimum:
  - Where are patient wait times longest, and how are they trending?
  - Which facilities/units are at risk of running out of bed capacity, and when?
  - Are we adequately staffed relative to patient load, by department and shift?
  - What is our readmission rate, and does it vary by facility or diagnosis?
  - How much revenue is at risk from denied or delayed claims, and with which payers?
  - Where and how often are we at risk of a pharmacy stockout?
- **Two working dashboards** (detailed in Section 5) that leadership and operational managers
  can actually use, not a static report.
- **A promotion path** from a development environment, through a testing/validation
  environment, to a live production environment — demonstrated, not just described.
- **Documentation** sufficient for our own IT staff to understand, operate, and extend the
  platform after handover.

## 5. Required Dashboards

We require **at least two** dashboards, each serving a different audience:

### 5.1 Executive Dashboard

Audience: C-suite and network administrators (CMO, COO, CFO-level stakeholders).

This dashboard must answer, at a glance and over time: how is the network performing overall —
patient wait times, bed occupancy/capacity, readmission rate, staffing ratios, and
claims/financial performance — and is that performance improving or degrading, network-wide and
by facility? It should work as an executive-summary view suitable for a board or leadership
meeting (headline numbers and trends leadership can act on, not a wall of tables), and as a
living view leadership returns to weekly or monthly rather than a one-time snapshot.

### 5.2 Operational / Clinical Dashboard

Audience: Operations managers, nursing supervisors, department heads, pharmacy managers.

This dashboard must answer: where, specifically (facility, department/unit, shift), are we at
risk right now — approaching bed capacity limits, understaffed relative to patient load, at risk
of a drug stockout, or seeing an unusually high rate of critical patient-monitoring alerts — and
how does that detail roll up to (or explain) the executive-level numbers? It should let an
operational or clinical manager go from a leadership-level number down to the facility/
department/shift detail behind it, and should be framed around what needs action today, not just
what happened historically.

**A note on both dashboards:** teams have full creative freedom over layout, visualization
choices, and design — we are not prescribing screen-by-screen requirements. What matters is
whether the dashboard actually answers the business questions above, clearly and correctly;
insight and answer quality are what will be evaluated, not visual polish.

## 6. Data Sources and Governance Expectations

At a high level, the platform will need to bring together:

- **Batch file drops** on daily/weekly cadences — patient visit/admission records, staff
  schedules, billing and claims extracts, pharmacy inventory levels, and bed capacity snapshots.
  We expect these to arrive via a mix of a secure internal document library (for
  internally-managed operational documents such as staff schedules) and secure cloud file
  storage (for system-generated extracts from our clinical, financial, and pharmacy systems).
- **Real-time event streams** — a continuous feed of patient vital-sign readings from bedside
  monitoring devices, and a continuous feed of prescription-issuance events from our pharmacy
  system, delivered via a real-time event streaming platform (we anticipate a Kafka-compatible
  managed service, but the specific technology choice is yours to propose and justify).

We are **not** prescribing the internal technical design (data models, storage layout, or
specific transformation logic) — that is what we're asking you to design and propose. What we
do require:

- This data includes **protected health information and other personally identifiable
  information** (patient names, dates of birth, contact details, government identifiers,
  diagnoses, and similar). Any platform handling it must reflect HIPAA-consistent data
  governance principles: access should be limited to what each user role actually needs,
  sensitive fields should be protected appropriately for each audience, and there should be a
  clear, auditable story for who can see what.
- We expect a documented approach to data quality, environment separation (so untested changes
  never reach a live system used for real reporting), and operational monitoring of the
  pipelines themselves.
- Nothing in this request should be read as dictating your architecture — we are hiring you for
  your data engineering expertise. We do expect you to be able to explain and justify the
  choices you make, particularly around governance of sensitive data.

## 7. Expected Deliverables

At the end of this engagement, we expect:

1. An end-to-end, working data pipeline — not a proof of concept — covering all identified data
   sources.
2. Both batch and streaming ingestion, built and demonstrated against our actual source
   patterns described above.
3. A layered data lake (raw / cleansed / curated, following bronze/silver/gold best practice)
   with a clear, documented data quality framework between the cleansed and curated layers.
4. Automated orchestration of the full pipeline, on a schedule, with monitoring/alerting.
5. Three properly separated environments — development, testing/UAT, and production — with a
   controlled, demonstrated process for promoting changes between them.
6. At minimum, the two dashboards described in Section 5, built on top of the curated data
   layer.
7. Complete documentation: architecture, data model, governance/access approach, and
   operational runbook.
8. A final presentation to our leadership team walking through the platform, the KPIs it
   answers, and how it meets the success criteria in Section 4.

We look forward to your proposed architecture and project plan.

— Office of the CIO, Meridian Health Network
