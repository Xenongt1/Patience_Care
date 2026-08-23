# Module: azure-foundation

Shared baseline resources that every other module depends on:

- Resource Group
- VNet + subnet(s) for private endpoints
- Private DNS zones (`privatelink.servicebus.windows.net`, `privatelink.dfs.core.windows.net`,
  `privatelink.vaultcore.azure.net`)
- Key Vault
- Managed identities (archive-writer notebook, Eventstream/pipeline access)
- Azure Monitor Action Group (target for alerting in later phases)

Not yet implemented — this README is scaffolding ahead of the `.tf` files.
