# Eventhouse KQL

KQL scripts for the Eventhouse/KQL Database created by
`../../../terraform/modules/fabric-realtime-items/`. Intended files (per the plan):

- `01_tables.kql` — `kql_vitals_raw`, `kql_prescriptions_raw`, `kql_quarantine`
- `02_ingestion_mappings.kql`
- `03_update_policies.kql` — validation/routing from Eventstream, `event_id` dedup
- `04_materialized_views.kql` — `kql_agg_vitals_5min`, `kql_agg_dept_vitals_current`,
  `kql_agg_prescriptions_hourly`, `kql_agg_controlled_substance_audit`, `kql_agg_facility_risk`
- `05_reference_tables.kql` — `ref_facilities`, `ref_units`, `ref_patients`, `ref_staff`,
  `ref_drugs`, `ref_bed_capacity` (read-only, loaded daily from batch Gold)

Not yet written.
