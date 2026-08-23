---
name: aws-diagram-creator
description: Create or edit clean, professional AWS architecture diagrams as draw.io (.drawio) files, following the user's established visual preferences. Use whenever the user asks to draw, create, update, or fix an AWS architecture / infrastructure / network diagram.
---

# AWS Diagram Creator

Produce neat, uncluttered AWS architecture diagrams as **draw.io (`.drawio`) files**. The
output is always a single `.drawio` file the user opens in draw.io / the VS Code Draw.io
extension. Derive the architecture from whatever the user points at (CloudFormation /
Terraform / prose) and ask when something is genuinely ambiguous.

## Non-negotiable preferences

These come from the user directly. Honor them by default on every diagram.

1. **Output is a `.drawio` file.** Never deliver an image or ASCII as the deliverable.
2. **Clean and easy on the eye.** Light, pastel container backgrounds; no walls of deep
   color. Let the small AWS service icons carry the only saturated color.
3. **Minimal text.** Only the text needed to explain an operation. No verbose annotations
   on the canvas. Use short edge labels (`:8080`, `trigger`, `deploy`, `pull image`).
4. **All text is independent, freely-movable text boxes.** Nodes and edges carry an empty
   `value=""`. Every label — node names, container titles, edge labels — is its own
   `text;html=1;strokeColor=none;fillColor=none;...` cell positioned near its target. The
   user wants to drag any text freely without moving the shape. (Trade-off they accept:
   moving a node does not move its label.)
5. **Edge labels never sit on the arrow.** Place each label off the line — above for
   horizontal segments, to the side for vertical ones. With decoupled text boxes (pref #4)
   this is automatic; if ever using real edge labels, add an `<mxPoint ... as="offset"/>`.
6. **Official AWS group containers.** AWS Cloud, Region, VPC, AZs, and subnets use the
   **official draw.io AWS group shapes** (`shape=mxgraph.aws4.group` with the matching
   `grIcon=mxgraph.aws4.group_*`). They carry the correct AWS corner icon and border color,
   so prefer them over plain rectangles. **Roundness is NOT a priority** — these group shapes
   draw square corners and that is fine; do **not** add `rounded`/`absoluteArcSize`/`arcSize`
   to fight it. Keep `pointerEvents=0` so the group doesn't swallow clicks; children may stay
   `parent="1"` (flat) since the group is purely visual. A purely *logical* boundary with no
   AWS equivalent (e.g. "ECS Service") can still be a plain dashed rectangle.
7. **Consolidate to reduce clutter.** Collapse repetitive resources into one labeled node
   (e.g. 6 interface VPC endpoints → one "VPC Endpoints" node per AZ listing the services).

## Use the CURRENT (2023) AWS resource icons — not the old flat ones

This is a hard requirement from the user: use the **current draw.io AWS icon set** (refreshed
2023-01-31), not the older flat-fill icons. The current resource icons are a rounded square
with a **vertical gradient** (lighter at the top, darker — almost black — at the bottom) and a
**white** glyph. That light-to-dark gradient is the "pink and black" look the user expects;
a flat single `fillColor` with `gradientColor=none` looks washed out and is the outdated style
to avoid.

**Current resourceIcon style template** (fill these in per service):

```
sketch=0;points=[[0,0,0],[0.25,0,0],[0.5,0,0],[0.75,0,0],[1,0,0],[0,1,0],[0.25,1,0],[0.5,1,0],[0.75,1,0],[1,1,0],[0,0.25,0],[0,0.5,0],[0,0.75,0],[1,0.25,0],[1,0.5,0],[1,0.75,0]];outlineConnect=0;fontColor=#232F3E;gradientColor=<LIGHT>;gradientDirection=north;fillColor=<DARK>;strokeColor=#ffffff;dashed=0;verticalLabelPosition=bottom;verticalAlign=top;align=center;html=1;fontSize=11;aspect=fixed;shape=mxgraph.aws4.resourceIcon;resIcon=mxgraph.aws4.<name>
```

Keep `strokeColor=#ffffff` (the standard white glyph). `gradientDirection=north` puts the
light shade on top.

### resIcon name gotchas (verified)

Some `resIcon` values render as a blank colored square (no glyph). The 2023 refresh also
renamed several icons with a `_2` suffix. Verified working choices:

| Need            | DO NOT use                            | USE instead                                          |
|-----------------|---------------------------------------|------------------------------------------------------|
| IAM role        | `identity_and_access_management_iam`  | `shape=mxgraph.aws4.role` (flat icon, ~78×44, no resIcon) |
| S3 bucket       | `simple_storage_service`              | `resIcon=mxgraph.aws4.s3`                             |
| CloudWatch      | `cloudwatch`                          | `resIcon=mxgraph.aws4.cloudwatch_2`                  |
| CloudWatch alarm| `cloudwatch_alarm` (blank tile here)  | `resIcon=mxgraph.aws4.cloudwatch_2` (gradient tile — see below) |
| Auto Scaling    | `application_auto_scaling`            | `shape=mxgraph.aws4.auto_scaling2` (flat, no resIcon) |
| ECS             | `elastic_container_service` (blank)   | `resIcon=mxgraph.aws4.ecs`                           |
| ECR             | `elastic_container_registry` (blank)  | `resIcon=mxgraph.aws4.ecr`                           |

**This user's draw.io renders the SHORT (pre-2023) resIcon names, not the long 2023-refresh
names.** Verified from their committed diagrams: `ecr`, `ecs`, `cloudwatch_2`, `cloudfront`,
`rds`, `rds_proxy`, `elasticache`, `s3`, `fargate`, `secrets_manager`, `eventbridge`,
`codepipeline`, `codedeploy`, `application_load_balancer`, `internet_gateway`, `endpoints` all
render. The long names (`elastic_container_registry`, `elastic_container_service`,
`cloudwatch_alarm`) render as **blank colored tiles** in their environment — do NOT use them.
The gradient styling is unrelated to blanking; a blank tile is always a bad resIcon name.

**User's preferred icons (use these by default):**
- **ECS** — `resIcon=mxgraph.aws4.ecs`, Compute orange gradient
  (`fillColor=#D05C17;gradientColor=#F78E04`), 52×52. Use the short name — the long
  `elastic_container_service` renders blank here.
- **ECR** — `resIcon=mxgraph.aws4.ecr`, same Compute orange gradient, 52×52. Short name only —
  `elastic_container_registry` renders blank here.
- **CloudWatch alarm** — the **gradient resourceIcon tile** `resIcon=mxgraph.aws4.cloudwatch_2`
  with the Management & Governance pink gradient (`fillColor=#BC1356;gradientColor=#F34482;
  gradientDirection=north`), `strokeColor=#ffffff`, 52×52. The user prefers a gradient tile over
  the flat `shape=mxgraph.aws4.alarm` bell symbol; `cloudwatch_alarm` blanks, so `cloudwatch_2`
  is the gradient tile that renders.

When unsure whether an icon renders, keep the node `id` stable so attached edges/labels survive
an icon swap. If one renders blank, tell the user and offer to let them drop the correct icon
in from the draw.io shape picker (they often paste one near the bad node); then copy that
cell's `shape`/`resIcon` onto the original cell and delete the placeholder. Flat icons like
`role` and `auto_scaling2` are symbol-only (no gradient tile) and render their glyph in
`fillColor`.

## Visual style reference

**Containers** — use the **official AWS group shapes** (square corners are expected; do not
round them — see pref #6). Keep the group `value=""` and put its title in a separate text box
next to the corner icon (pref #4). Shared base style:

```
sketch=0;outlineConnect=0;gradientColor=none;html=1;whiteSpace=wrap;fontSize=12;container=1;pointerEvents=0;collapsible=0;recursiveResize=0;shape=mxgraph.aws4.group;verticalAlign=top;align=left;spacingLeft=30;<PER-CONTAINER>
```

Per-container `<PER-CONTAINER>` overrides (`grIcon` + colors):
- AWS Cloud — `grIcon=mxgraph.aws4.group_aws_cloud_alt;strokeColor=#232F3E;fillColor=none;fontColor=#232F3E;dashed=0`
- Region — `grIcon=mxgraph.aws4.group_region;strokeColor=#00A4A6;fillColor=none;fontColor=#00A4A6;dashed=1`
- VPC — `grIcon=mxgraph.aws4.group_vpc;strokeColor=#8C4FFF;fillColor=none;fontColor=#8C4FFF;dashed=0`
- Availability Zone — `grIcon=mxgraph.aws4.group_availability_zone;strokeColor=#147EBA;fillColor=none;fontColor=#147EBA;dashed=1`
- Public subnet — `grIcon=mxgraph.aws4.group_public_subnet;strokeColor=#7AA116;fillColor=#E9F3E6;fontColor=#248814;dashed=0`
- Private subnet — `grIcon=mxgraph.aws4.group_private_subnet;strokeColor=#00A4A6;fillColor=#E6F6F7;fontColor=#147EBA;dashed=0`
- Security group (if shown on canvas) — `grIcon=mxgraph.aws4.group_security_group;strokeColor=#DD3522;fillColor=none;fontColor=#DD3522;dashed=0`
- Logical boundary with no AWS equivalent (e.g. ECS Service) — plain dashed rectangle, no group shape: `dashed=1;dashPattern=6 4;strokeColor=#ED7100;fillColor=none`

**Icon category gradient pairs** — `fillColor` (DARK, bottom) / `gradientColor` (LIGHT, top),
verified against current draw.io diagrams:
- Compute / Containers (ECR, ECS, Fargate, EC2) — `#D05C17` / `#F78E04` (orange)
- Networking & Content Delivery (ALB, NAT, IGW, endpoints, CloudFront) — `#5A30B5` / `#945DF2` (purple)
- Storage (S3) — `#277116` / `#60A337` (green)
- Security, Identity & Compliance (IAM) — `#C7131F` / `#F54749` (red)
- Management & Governance (CloudWatch, alarms) — `#BC1356` / `#F34482` (pink)
- Application Integration (EventBridge, SNS, SQS, Step Functions) — `#BC1356` / `#FF4F8B` (pink; note the lighter top differs from Management)
- Developer Tools (CodePipeline, CodeDeploy, CodeBuild) — `#3334B9` / `#4D72F3` (blue)

(Group/container border colors below are separate from these icon colors and unchanged.)

**Edges** — `edgeStyle=orthogonalEdgeStyle;rounded=1;endArrow=block;endFill=1`:
- Primary flow — `strokeColor=#5A6B7B;strokeWidth=1.5`
- Secondary/util (logs, scaling, artifacts) — `strokeColor=#9AA5B1;strokeWidth=1.2`
- PrivateLink / VPC-endpoint paths — `dashed=1;strokeColor=#8C4FFF`
- OIDC / IAM trust — `dashed=1;strokeColor=#DD344C`
- Keep the node `id` on both ends (`source=`/`target=`) so edges reflow when nodes move.
  Detached edges with fixed `sourcePoint`/`targetPoint` drift and mis-route — reattach them.

**Text boxes** — `text;html=1;strokeColor=none;fillColor=none;align=center;whiteSpace=wrap;fontSize=11;fontColor=<match>`.
Node labels sit ~2px below the icon; container titles top-left inside; edge labels at the
segment midpoint, offset off the line. Match label `fontColor` to its edge/zone color.

## Modeling guidance

- **Service-level vs per-resource arrows.** Operations that act on a *service* (CodeDeploy
  blue/green, Auto Scaling, CloudWatch logs for ECS) point at a single **service boundary**
  enclosing the tasks — not at one arbitrary task. Per-instance flows (ALB → each task,
  task → its AZ's VPC endpoints) connect to the individual resources. Avoid the lopsided
  look of a service-level arrow touching only one of several identical resources.
- **Multi-AZ** infra: show two AZs side by side, each with public (NAT, ALB span) and private
  (tasks, endpoints) subnets.
- **CI/CD** typically flows left→right across the top; the request path flows top→bottom into
  the VPC. Keep the two from tangling.

## Starter template

Begin from a known-good skeleton and adapt. Layer order: containers first (back), then
icons, then edges, then all text boxes (front). Keep `pageWidth`/`pageHeight` generous.

```xml
<mxfile host="app.diagrams.net">
  <diagram id="arch" name="Architecture">
    <mxGraphModel dx="1400" dy="900" grid="0" pageWidth="2000" pageHeight="1240" math="0" shadow="0">
      <root>
        <mxCell id="0" /><mxCell id="1" parent="0" />
        <!-- containers (official AWS group shapes; square corners are fine) -->
        <!-- icons (value="") -->
        <!-- edges (value="", source/target by id) -->
        <!-- free-floating text boxes -->
      </root>
    </mxGraphModel>
  </diagram>
</mxfile>
```

If the current project already contains a reviewed `.drawio` diagram, use it as an additional
layout reference while preserving the official AWS group shapes and other rules above.

## Maintenance

This skill is a living document. **Whenever the user gives new feedback about how they want
AWS diagrams done** (a color, an icon fix, a layout rule, a text convention), update this
file so the preference persists. Treat the user's word as authoritative over the defaults
written here.

## References

Provenance for the current (2023) icon styles and verified category gradient pairs above:

- New AWS icons release (2023-01-31) — https://github.com/jgraph/drawio/issues/3336
- Finding outdated AWS icons in draw.io — https://kevinhakanson.com/2023-03-04-finding-outdated-aws-icons-in-drawio-files/
- AWS Architecture Icons (official) — https://aws.amazon.com/architecture/icons/
- Real diagrams used to extract verbatim style strings:
  - https://github.com/Abhiek187/aws-shop/blob/main/architecture-diagram.drawio (CloudWatch `cloudwatch_2`, S3, IAM gradients)
  - https://github.com/garystafford/draw-io/blob/master/ECR.drawio (Containers/ECR/ECS, ELB, Developer Tools)
  - https://github.com/aws-samples/aws-organizations-tag-inventory/blob/main/architecture.drawio (Application Integration / Step Functions, EventBridge)
- aws4 shape-name reference — https://github.com/DayuanJiang/next-ai-draw-io/blob/main/docs/shape-libraries/aws4.md
