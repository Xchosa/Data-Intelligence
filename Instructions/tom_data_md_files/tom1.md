# Part 1: Fundamentals

This part covers the foundational concepts and skills essential for building production-grade intelligence platforms.

## Goal  

Set up your own Databricks playground and master the core components required to build a production-grade lakehouse solution.

## Scope  

### Create

a [Databricks Free Edition](https://docs.databricks.com/en/getting-started/community-edition.html) workspace  

### Explore

#### Platform Basics & UI

- Databricks UI and workspace navigation - [Source](https://docs.databricks.com/en/workspace/index.html)

#### Compute & Development environment

- Compute clusters (creation, configuration, autoscaling, and cluster policies) - [Source](https://docs.databricks.com/en/compute/index.html)
- Notebooks fundamentals (cells, magic commands, visualizations) - [Source](https://docs.databricks.com/en/notebooks/index.html)
- Git folders and version control integration - [Source](https://docs.databricks.com/en/repos/index.html)

#### Data Management & Storage

- Unity Catalog - [Source](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
- Databricks volumes - [Source](https://docs.databricks.com/en/volumes/index.html)
- Delta Lake fundamentals - [Source](https://docs.databricks.com/en/delta/index.html)

#### Pipelines, Orchestration & Security

- Data ingestion patterns (batch vs streaming) - [Source](https://docs.databricks.com/en/ingestion/index.html)
- Lakeflow Spark Declarative Pipelines (SDPs) - [Source](https://docs.databricks.com/en/dlt/index.html)
- Lakeflow Jobs - [Source](https://docs.databricks.com/en/jobs/index.html)
- Secret scopes - [Source](https://docs.databricks.com/en/security/secrets/secret-scopes.html)

### Study Lufthansa OpenAPI datasets

- Reference Data (Countries, Cities, Airports, Airlines, Aircraft) - [Source](https://developer.lufthansa.com/docs/read/api_details/Reference_Data)
- Flight Status by Route - [Source](https://developer.lufthansa.com/docs/read/api_details/operations/Flight_Status_by_City_Pair)

If you want to explore more data on your own, you can look at the vast APIs that LH OpenAPI offers.

- [developer docs](https://developer.lufthansa.com/docs)
- [developer page](https://developer.lufthansa.com/page)

### Learn Mermaid & ER (Entity-Relationship) modeling

- Design an ER model for the Lufthansa domain using Mermaid

## Expected Outcome  

- Working Databricks workspace
- Personal summary of key concepts
- Mermaid ER diagram including:
  - [Reference Data](https://developer.lufthansa.com/docs/read/api_details/Reference_Data) (Countries, Cities, Airports, Airlines, Aircraft)
  - [Flight Status by Route](https://developer.lufthansa.com/docs/read/api_details/operations/Flight_Status_by_City_Pair)
  - Clear overview of entities, relationships between the entities and constraints.

## Bonus  

Choose and justify a modeling strategy (e.g., Star Schema, Normalized Model, Data Vault). Argument and document assumptions and trade-offs.
[Recommended Source](https://www.databricks.com/blog/2022/06/24/data-warehousing-modeling-techniques-and-their-implementation-on-the-databricks-lakehouse-platform.html)

## Did you know that…?

### Mermaid

Mermaid code can be used in a variety of contexts, including software development, project management, and more. It is particularly useful for creating diagrams and flowcharts in documentation, as it allows developers and other technical professionals to quickly and easily create visual representations of complex concepts and processes.
