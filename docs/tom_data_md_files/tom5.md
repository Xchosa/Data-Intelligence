# Part 5: Infrastructure as Code (Optional)

Infrastructure can be automated!

## Goal  

Provision Databricks resources using Terraform to ensure reproducibility and consistency.

## Scope

- Use Terraform [Source](https://developer.hashicorp.com/terraform/tutorials)
- Create `/terraform` structure at the root directory - [Source](https://medium.com/@mariammbello/terraform-beginner-cheat-sheet-understanding-the-basics-and-structure-881a496d6211)
- Authenticate to Databricks using Terraform & provision dev schema inside your catalog (💡While service principals are the recommended authentication method for production use, you'll use personal access tokens due to Databricks Free Edition limitations) [Source]((https://registry.terraform.io/providers/databricks/databricks/latest/docs))
  - Define `databricks_host` and `databricks_token` variables in `variables.tf`
  - Configure the Databricks provider in `provider.tf` using the host and token arguments
  - Create a `terraform.tfvars` file to store your workspace URL and personal access token (ensure this file is added to `.gitignore`)
  - Use `terraform init` to initialize the provider and verify authentication
  - Use `terraform plan` and `terraform apply` to deploy the resources
- Provision:
  - [notebooks](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/notebook)
  - [jobs](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/job)
  - [pipelines](https://registry.terraform.io/providers/databricks/databricks/latest/docs/resources/pipeline)
- Follow Terraform best practices
  - Use variables to parameterize your configurations, making them reusable across environments
  - Leverage data sources to reference existing Databricks resources dynamically
  - Implement proper naming conventions for resources to ensure clarity and maintainability
  - Consider using `terraform fmt` to maintain consistent formatting

## Outcome

- Clean code & structure
- `provider.tf`, `variables.tf`, `main.tf`
- Successful `terraform init/plan/apply`  
- Working authentication to Databricks workspace
- Dev schema provisioned in your catalog
- Notebooks, jobs, and pipeline deployed via Terraform
- .gitignore including terraform.tfvars and .env files

## Did you know that…?

Infrastructure as Code (IaC) is the practice of managing and provisioning computing infrastructure through machine-readable definition files, rather than through manual configuration or interactive tools. By treating infrastructure configuration as code, organizations can version control their infrastructure, apply software development best practices, and ensure consistency across environments.

Terraform is a popular open-source IaC tool that enables you to define cloud and on-premises resources in human-readable configuration files that you can version, reuse, and share. With Terraform, you can manage the entire lifecycle of your infrastructure using a declarative approach, where you specify the desired state of your infrastructure and Terraform determines how to achieve it.

In the context of Databricks and building production-grade intelligence platforms, IaC becomes particularly valuable as it allows you to codify the creation and configuration of workspaces, clusters, jobs, notebooks, Unity Catalog resources, and access controls. This ensures that development, testing, and production environments are identical, reduces manual errors, and accelerates the deployment process.

Continuous Integration and Continuous Deployment (CI/CD) pipelines complement IaC by automating the testing and deployment of both infrastructure and application code. By integrating IaC with CI/CD, you can automatically validate infrastructure changes, run tests, and deploy updates to multiple environments with confidence, ensuring that your data pipelines and infrastructure remain reliable and maintainable.

