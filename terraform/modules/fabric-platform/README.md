# Module: fabric-platform

Fabric tenant-level platform resources, via the `microsoft/fabric` and `azurerm` providers:

- `azurerm_fabric_capacity` (SKU is a cost decision to confirm when this module is built)
- Fabric workspace, assigned to that capacity
- Workspace role assignments for the identities created in `azure-foundation`
- Optionally: git integration from the workspace back to this repo

Depends on: `azure-foundation` (identities). Not yet implemented.
