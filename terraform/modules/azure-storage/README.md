# Module: azure-storage

ADLS Gen2 storage for the Bronze raw-archive layer:

- Storage account (hierarchical namespace enabled)
- `rt-raw-archive` container, with `vitals/` and `prescriptions/` path prefixes written by the
  Fabric notebook archive writer
- Lifecycle management policy: 90-day hot retention for general events; controlled-substance
  prescription events need a 7-year retention path scoped by blob prefix/tag — implementation
  detail to work out when this module is built
- Private endpoint into the `azure-foundation` subnet
- RBAC: Storage Blob Data Contributor for the archive-writer identity

Depends on: `azure-foundation` (subnet, DNS zone). Not yet implemented.
