# Part 6 : CI/CD Pipelines (Optional)

## Goal

Automate deployment of Terraform-managed Databricks infrastructure using GitLab CI/CD.

## Scope

- Implement CI/CD pipeline
  - Configure Terraform state backend in GitLab [Source](https://developer.hashicorp.com/terraform/language/state)
  - Store secure CI variables (masked) [Source](https://docs.gitlab.com/ci/variables/)
  - Create `.gitlab-ci.yml` pipeline with validation, planning, and deployment stages [Source](https://docs.gitlab.com/ci/yaml/)
- Deploy to production
  - Add production deployment stage into `.gitlab-ci.yml`:
    - Separate production and development variables [Source](https://developer.hashicorp.com/terraform/language/values/variables)
    - Implement mechanism against unauthorized deployments  [Source](https://docs.gitlab.com/ci/yaml/#environment)
      - implement Manual approval gates
      - restrict pipeline to protected branches (e.g., `main` or `production`)

## Outcome

- GitLab backend configuration for Terraform state
- Fully working and safe CI/CD pipeline  
- Automated infrastructure validation and deployment  
- Production deployment with safeguards  
- Deployment + rollback documentation  

## Did you know that…?

Continuous Integration and Continuous Deployment (CI/CD) pipelines are essential practices in modern software. They automate the process of testing, building, and deploying code changes, ensuring that your data pipelines and infrastructure remain reliable, maintainable, and rapidly deployable.

CI/CD pipelines complement Infrastructure as Code by automating the validation and deployment of both infrastructure and application code. By integrating automated testing and deployment workflows, you can catch errors early, maintain consistency across environments, and accelerate delivery cycles with confidence.
