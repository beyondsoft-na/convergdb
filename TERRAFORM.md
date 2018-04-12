# Background

Terraform maintains its own state file reflecting its view of the
infrastructure. Using a remote backend such as S3 is important, especially
in multi-user scenarios.

# Description of operations

ConvergDB uses Terraform to manage infrastructure. The S3 remote backend
for Terraform requires bootstrapping, to create an S3 bucket and DynamoDB
table) for state management. ConvergDB therefore provides a `bootstrap`
directory, alongside the main `terraform` directory.

The `bootstrap` directory contains a `bootstrap.tf` file which specifies an
S3 bucket and DynamoDB table. When applied to an AWS account using the
`terraform init` and `terraform apply` commands, it will create the
necessary S3 bucket and DynamoDB table. This process will also write a
`terraform.tf` file in the top-level `terraform` directory, which contains
the backend configuration to be used by the main deployment. It also
creates a `variables.tf` file containing two variables, `region` and
`admin_bucket`, for use by the main deployment.

# Step-by-step

1. After running `convergdb generate` (or equivalent), first bootstrap Terraform:

```bash
$ cd bootstrap
$ terraform init
...
$ terraform plan -out tf.plan
var.region
  Enter a value: us-west-2
...
$ terraform apply tf.plan
...
```

This process creates a unique S3 bucket and DynamoDB table, which are used by
the main Terraform deployment to manage the infrastructure state.

2. After bootstrapping Terraform, deploy the rest of the infrastructure.

```bash
$ cd ../terraform
$ terraform init
...
Successfully configured the backend "s3"!
...
$ terraform plan -out tf.plan
...
$ terraform apply tf.plan
...
```