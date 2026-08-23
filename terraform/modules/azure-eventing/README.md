# Module: azure-eventing

Azure Event Hubs for the two real-time streams defined in `meridian_rt_lineage.docx`:

- Event Hubs Namespace
- Event Hub `meridian-patient-vitals` and `meridian-prescription-events`, partitioned by
  `facility_id`
- Consumer groups: one for Fabric Eventstream (Silver routing), one for the Fabric notebook
  archive writer (Bronze JSONL) — kept separate per the lineage doc's requirement that archival
  not depend on Eventstream's own read position
- Private endpoint into the `azure-foundation` subnet
- RBAC: Event Hubs Data Receiver scoped to each identity/consumer group pairing

Depends on: `azure-foundation` (subnet, DNS zone). Not yet implemented.
