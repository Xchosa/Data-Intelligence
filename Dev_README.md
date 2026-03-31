https://docs.databricks.com/aws/en/dev-tools/bundles/work-tasks#validate

bundle validate
databricks bundle validate

databricks bundle validate -t dev
databricks bundle deploy -t dev

databricks bundle validate -t prod
databricks bundle deploy -t prod

every 4 hours the flight departures of FRA und MUC get extracted
since the data comes in 4 hours blocks



many bronze tables 

create one singlee source of turth for all depature data

for a sngle silver table 

Simplicity for gold 

-> no need to union tables repeatedly in every downstream query or gold table definiton
