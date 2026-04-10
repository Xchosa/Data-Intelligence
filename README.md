# Lufthansa_Data

The 'Lufthansa_Data' project implements a **Medallion Architecture lakehouse pipeline** for processing Lufthansa flight data from the API through Bronze, Silver, and Gold layers using Databricks Bundles.

## Data Layers

| Layer                 | Input           | Processing               | Output                                                                                           |
| --------------------- | --------------- | ------------------------ | ------------------------------------------------------------------------------------------------ |
| **Raw Landing** | Lufthansa API   | File extraction          | UC Volume (JSON files)                                                                           |
| **Bronze**      | UC Volume files | Auto Loader ingestion    | `bronze.bronze_departures`, `bronze.bronze_table_*`                                          |
| **Silver**      | Bronze tables   | Clean, validate, conform | `silver.silver_departures`, `silver.silver_departures_quarantine`, `silver.silver_table_*` |
| **Gold**        | Silver tables   | Join & aggregate         | `gold.gold_flight_details`                                                                     |

## Getting started

Choose how you want to work on this project:

(a) Directly in your Databricks workspace, see
    https://docs.databricks.com/dev-tools/bundles/workspace.

(b) Locally with an IDE like Cursor or VS Code, see
    https://docs.databricks.com/dev-tools/vscode-ext.html.

(c) With command line tools, see https://docs.databricks.com/dev-tools/cli/databricks-cli.html

If you're developing with an IDE, dependencies for this project should be installed using uv:

* Make sure you have the UV package manager installed.
  It's an alternative to tools like pip: https://docs.astral.sh/uv/getting-started/installation/.
* Run `uv sync --dev` to install the project's dependencies.

# Using this project using the CLI

The Databricks workspace and IDE extensions provide a graphical interface for working
with this project. It's also possible to interact with it directly using the CLI:

1. Authenticate to your Databricks workspace, if you have not done so already:

   ```
   $ databricks configure
   ```
2. To deploy a development copy of this project, type:

   ```
   $ databricks bundle deploy --target dev
   ```

   (Note that "dev" is the default target, so the `--target` parameter
   is optional here.)

   This deploys everything that's defined for this project.
   For example, the default template would deploy a pipeline called
   `[dev yourname] Lufthansa_Data_etl` to your workspace.
   You can find that resource by opening your workpace and clicking on **Jobs & Pipelines**.
3. Similarly, to deploy a production copy, type:

   ```
   $ databricks bundle deploy --target prod
   ```

   Note the default template has a includes a job that runs the pipeline every day
   (defined in resources/sample_job.job.yml). The schedule
   is paused when deploying in development mode (see
   https://docs.databricks.com/dev-tools/bundles/deployment-modes.html).
4. To run a job or pipeline, use the "run" command:

   ```
   $ databricks bundle run
   ```
5. Finally, to run tests locally, use `pytest`:

   ```
   $ uv run pytest
   ```

# Project Structure

Lufthansa_Data/
├── src/                                          # Python source code
│   ├── extract_data/                             # Raw Landing & Bronze Layer
│   │   ├── main.py                               # API extraction orchestration
│   │   ├── config.py                             # Configuration management
│   │   ├── bronze_table_flight_departures.py     # Bronze departures ingestion
│   │   └── bronze_table_resources.py             # Bronze reference data ingestion
│   │
│   ├── silver/                                   # Silver Layer (cleaning & validation)
│   │   ├── silver_tables_departure.py            # Silver departures transformation
│   │   ├── silver_tables_cities.py               # Silver cities transformation
│   │   ├── silver_tables_countries.py            # Silver countries transformation
│   │   ├── silver_tables_airports.py             # Silver airports transformation
│   │   ├── silver_tables_airlines.py             # Silver airlines transformation
│   │   └── silver_tables_aircrafts.py            # Silver aircrafts transformation
│   │
│   ├── gold/                                     # Gold Layer (analytics & KPIs)
│   │   └── gold_service_by_country.py            # Gold flight details aggregation
│   │
│   └── Lufthansa_Data/                           # Shared utility modules
│       ├── __init__.py
│       ├── schemas.py                            # Data schemas definitions
│       ├── transformations.py                    # Shared transformation logic
│       └── validators.py                         # Data quality validators
│
├── resources/                                    # Databricks Bundle resources
│   ├── jobs/                                     # Job definitions (YAML)
│   │   ├── Batch_operational_data.yml            # Batch operational data job
│   │   ├── Batch_reference_Data.yml              # Batch reference data job
│   │   └── orchestrate_pipeline_bronze_to_gold.yml # Multi-task orchestration
│   │
│   └── pipelines/                                # DLT Pipeline definitions (YAML)
│       ├── bronze-ingest-pipeline-flight_departures.yml
│       ├── bronze-ingest-pipeline-resources.yml
│       ├── bronze-pipeline-logs.yml
│       ├── silver-ingest-pipeline-departure.yml
│       ├── silver-ingest-pipeline-resources.yml
│       ├── gold-airline-delays.yml
│       ├── gold-cargo.yml
│       ├── gold-delays.yml
│       └── gold-services-and-more.yml
│
├── databricks.yml                                # Bundle configuration (dev/prod targets)
├── pyproject.toml                                # Python project & dependency config
├── README.md                                     # Project overview
└── docs/
    └── Repository_structure_and_architecture.md  # Detailed architecture documentation
