# Fabric Items — Content (not infrastructure)

This folder holds the actual *content* of Fabric items for the real-time pipeline: KQL DDL,
Eventstream topology, notebook code, pipeline activity definitions, semantic model TMDL, and
report definitions.

**This is deliberately separate from `../terraform/`.** Terraform (via the `microsoft/fabric`
provider) creates the empty item *shells* (an Eventhouse, an Eventstream, a Notebook, a Pipeline)
in `../terraform/modules/fabric-realtime-items/`; the files in here are deployed onto those shells
using the Fabric CLI (`fab`) or `fabric-cicd`, since the Terraform provider does not yet reliably
manage item-content-level detail (KQL scripts, Eventstream operators, notebook cells).

Grounded in `../meridian_rt_lineage.docx` — exact table names, field lists, and validation/DQ
rules referenced throughout this folder's eventual content come from that document.

## Layout

- `eventhouse/kql/` — table DDL, ingestion mappings, update policies, materialized views,
  reference-table shells (`kql_vitals_raw`, `kql_prescriptions_raw`, `kql_quarantine`,
  `kql_agg_*`, `ref_*`)
- `eventstream/` — routing/validation topology per stream (patient-vitals, prescription-events)
- `notebooks/` — Spark Structured Streaming archive writers (Bronze JSONL to ADLS Gen2/OneLake)
  and the daily reference-snapshot loader
- `pipelines/` — Fabric Data Pipeline definitions (daily reference refresh orchestration)
- `semantic-model/` — DirectQuery semantic model over the Eventhouse (RLS/OLS)
- `reports/` — the operational Power BI dashboard

## Deploy

Via the project venv's Fabric CLI: `.venv\Scripts\fab.exe` — see `../scripts/fab-deploy.ps1` once
that wrapper is written.

## Status

Scaffolding only — no content exists yet. This is authored after the corresponding Terraform
shells exist (see the plan's build sequence, steps 3–4).
