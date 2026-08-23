# Environment: prod

Root Terraform configuration for the prod environment. Composes the shared modules under
`../../modules/` with prod-specific variables (larger Fabric capacity SKU, stricter RBAC/retention
settings) and its own state backend.

Not yet implemented.
