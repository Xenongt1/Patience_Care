# Terraform — Meridian Real-Time Infrastructure

Infrastructure-as-code for the real-time streaming layer (Event Hubs, ADLS Gen2 archive,
Fabric capacity/workspace, and Fabric item shells). See `../fabric-items/README.md` for the
Fabric item *content* (KQL, notebooks, Eventstream topology) that gets deployed on top of the
shells this project creates — that content is managed via `fab`/`fabric-cicd`, not Terraform.

Grounded in `../meridian_rt_lineage.docx` and the plan at
`C:\Users\User2\.claude\plans\inherited-gliding-tower.md`.

## Layout

- `modules/` — reusable modules, one per resource domain (see each module's own README).
- `envs/dev`, `envs/test`, `envs/prod` — one root module per environment, each composing the
  shared modules with environment-specific variables and its own state.

## Prerequisites

- `az login` (Azure CLI, already installed) against the target subscription/tenant.
- Fabric CLI (`fab`) authenticated — available via the project venv: `.venv\Scripts\fab.exe auth login`.
- Terraform (already installed) — run `terraform init` from inside an `envs/<env>` directory,
  not from the repo root.

## Status

Scaffolding only — no `.tf` files exist yet. Module and environment implementation starts next,
in the order described in the plan's "Build sequence" section.
