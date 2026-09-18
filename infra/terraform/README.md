# ChainRoute Terraform

Infrastructure-as-code for deploying ChainRoute (Go API server, Go worker,
C++ router, Postgres, message broker, ALB, and an optional observability
stack) onto AWS ECS Fargate. See the design doc
(`docs/superpowers/specs/2026-09-17-containerization-deployment-design.md`)
and implementation plan
(`docs/superpowers/plans/2026-09-17-containerization-deployment-implementation.md`)
for the full rationale behind every tradeoff summarized below.

## Module tree

```
infra/terraform/
├── versions.tf                  # shared required_providers (aws ~> 5.0, random ~> 3.6)
├── modules/
│   ├── networking/               # VPC, subnets, NAT, security groups, Cloud Map namespace
│   ├── database/                 # RDS Postgres + Secrets Manager credentials
│   ├── messaging/                 # single-node Redpanda on Fargate (via ecs-service)
│   ├── observability/             # OTel Collector, Jaeger, Prometheus, Grafana (via ecs-service)
│   ├── ecs-service/                # reusable Fargate task/service module, used by every app
│   │                                and infra service in this tree
│   └── alb/                       # public ALB (the only 0.0.0.0/0 ingress point) + go-server target group
└── environments/
    └── dev/                       # root module: provider block, ECS cluster, IAM roles,
                                     # ECR repos, and the module calls that wire everything
                                     # above together for one concrete environment
```

`environments/dev` is the only root module in this tree today. A future
`environments/prod` would be a sibling directory reusing the same modules
with its own `terraform.tfvars`, its own state (see Remote state below),
and likely different toggles (e.g. `enable_observability_stack = false` if
a shared/central observability stack is used instead, or a non-default
`db_instance_class`).

`environments/dev/versions.tf` is a symlink to `../../versions.tf`: each
Terraform root module only loads `.tf` files from its own directory, not
from parent directories, so the shared provider-version pins are made
available to the environment root this way rather than duplicated.

## Running locally

```bash
cd infra/terraform
terraform fmt -recursive -check -diff   # verify formatting; drop -check -diff to fix in place

cd environments/dev
terraform init -backend=false            # downloads providers; no AWS credentials needed
terraform validate                       # checks HCL syntax/references; no AWS credentials needed
```

To validate against a filled-in tfvars file locally (never commit the
result):

```bash
cp terraform.tfvars.example terraform.tfvars   # git-ignored
terraform validate
rm terraform.tfvars
```

`terraform plan` additionally requires real AWS credentials (it makes
read-only API calls to reconcile state) and `terraform apply` will create
real, billable AWS resources.

**Never run `terraform apply` (or `plan` for real) without a real AWS
account, real credentials configured for it, and explicit authorization
from whoever owns that account/bill.** No credentials are configured in
this repo or in the CI/dev environment this was built in, by design.

## Remote state

`environments/dev/backend.tf` documents (commented out) how to point this
environment at an S3 + DynamoDB remote backend once one exists. Local state
is used by default so `init`/`validate` don't require any pre-existing AWS
resources.

## Redpanda vs. MSK

`modules/messaging` runs a single-node Redpanda broker as one Fargate task
rather than Amazon MSK or MSK Serverless. This mirrors the local Docker
Compose setup exactly and costs a single small Fargate task, at the cost of
having no replication/HA — a task restart loses in-flight uncommitted data,
the same way a local dev restart would. This is an explicit, disclosed
portfolio-scale tradeoff, not an oversight; the production upgrade path is
MSK Serverless or a multi-broker Redpanda cluster across AZs. See design
doc §4.1 for the full reasoning.
