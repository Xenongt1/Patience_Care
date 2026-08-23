# Environment: dev

Root Terraform configuration for the dev environment. Composes the shared modules under
`../../modules/` with dev-specific variables and its own state backend.

Not yet implemented — will contain `backend.tf`, `main.tf`, `variables.tf`, `outputs.tf`, and
`terraform.tfvars` once module implementation begins (see the plan's build sequence: this is the
first environment built out, module by module, before `test`/`prod` are stood up from the same
modules).
