# Module: fabric-realtime-items

Fabric item *shells* for the real-time pipeline, created via the `microsoft/fabric` provider and
wired to the workspace from `fabric-platform`:

- Eventhouse + KQL Database (table/view DDL lives in `../../../fabric-items/eventhouse/kql/`,
  deployed separately via `fab`)
- Eventstream ×2 (topology defined in `../../../fabric-items/eventstream/`)
- Notebook ×1–2 for the archive writer(s) (content in `../../../fabric-items/notebooks/`)
- Data Pipeline for the daily reference-snapshot loader (content in
  `../../../fabric-items/pipelines/`)

This module only creates the containers; item content is authored and deployed separately — see
`../../../fabric-items/README.md`. Depends on: `fabric-platform` (workspace). Not yet implemented.
