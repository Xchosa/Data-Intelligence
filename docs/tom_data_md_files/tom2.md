# Part 2: Data Ingestion

Data ingestion is the process of importing, transforming, and loading data from various sources into a storage system, such as a data warehouse, a data lake, or a database. This process involves collecting data from disparate sources, cleaning and validating it, and converting it into a format that can be easily analyzed and processed.

Data ingestion is a critical step in the data pipeline because it sets the foundation for the subsequent data processing and analysis. The ingestion process can be done in real-time or batch mode depending on the nature of the data, the processing requirements, and the business needs.

## Goal  

Design and implement a reliable ingestion layer that extracts Lufthansa OpenAPI data and stores raw datasets in Unity Catalog Volumes.

## Scope  

- Store raw data in Unity Catalog volumes:
  `/Volumes/<catalog>/<schema>/<volume>/`  
- Configure pipelines with scheduled triggers for data extraction (automated Pipepline for ingestion).
- Ingest operational data:
  - Flight Status by Route (Daily schedule)
- Ingest reference data (Monthly schedule):
  - Airports  
  - Cities  
  - Countries  
  - Airlines  
  - Aircraft  
- Validate that the extracted data provides dynamic flight state information, including flight operations and on-time status.

## Hint

To know which flights operate on a particular day or whether a particular flight is on-time, you'll find it under 'operations'

## Expected Outcome  

- Automated ingestion pipelines  
- Scheduled execution (daily/monthly)
- Raw data persisted in volumes
- Execution logs + validation proof  

## Bonus  

- Use [Databricks Asset Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles)
- Choose a strategy to set up a modular, well-structured codebase
- Create Git repository with branching strategy (recommended: Gitlab)
- Provide README with setup instructions
