# Eventstream Topology

Routing/validation topology for the two Fabric Eventstreams (`patient-vitals-topology.json`,
`prescription-events-topology.json`): required-field checks, timestamp parsing, schema_version
checks, and dual output (valid → Eventhouse KQL tables, invalid → `kql_quarantine`).

Not yet written.
