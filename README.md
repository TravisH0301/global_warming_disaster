# Impact of Global Warming on Australia
- [Data Architecture](#data-architecture)
- [Data Workflow](#data-workflow)
- [Data Modelling](#data-modelling)
- [Data Insights](#data-insights)
- [Future Improvements](#future-improvements)
- [Data Sources](#data-sources)

The escalating impacts of global warming are becoming increasingly evident in various parts of the world, with Australia being no exception. In recent years, the country has experienced significant temperature anomalies and natural disasters, raising concerns about the direct and indirect consequences of climate change. 

This project aims to investigate the relationship between global warming and the frequency and intensity of these events in Australia by building a comprehensive data architecture on AWS. By analyzing various datasets, including the global temperature index, temperature anomalies, and natural disaster occurrences, this seeks to uncover key insights that can help us better understand the implications of climate change for Australia's future.

## Data Architecture
<img src="https://github.com/TravisH0301/global_warming_disaster/blob/master/image/data_architecture.jpg" width="800">

- **Terraform**: Terraform builds S3 buckets and uploads data processing dependencies into the buckets.
- **AWS Lambda**: Lambda extracts raw data and loads it into the ingress bucket in S3.
Given the simplicity of this data process, Lambda was chosen to minimise the running cost.
- **AWS Glue**: Glue runs Spark jobs to transform the data through bronze, silver and gold buckets, storing in Delta Lake format. Additionally, Glue crawlers maintain the data catalog of delta tables. AWS EMR and AWS Batch were considered but not chosen due to the nature of light workloads and Glue's pre-configured Spark environment.
- **Amazon S3 & Delta Lake**: S3 serves as both data storage and lakehouse. Using Delta Lake and its medallion architecture, the lakehouse is established, eliminating the need for a separate data warehouse.
- **Apache Airflow**: Airflow orchestrates the entire data workflow by triggering the Lambda function and Glue jobs. 
- **Amazon Athena**: Athena serves as an query engine.
- **Amazon Quicksight**: Quicksight is used to analyse and visualise data insights.

## Data Workflow
<img src="https://github.com/TravisH0301/global_warming_disaster/blob/master/image/airflow_dag.jpg" width="600">

- **ingress_loader**: A Lambda function fetches raw data from data sources via API and loads it
into the ingress layer in CSV format.
- **ingress_to_bronze**: A Glue job reads the raw data and stores it in the bronze layer as delta
tables.
- **bronze_to_silver**: A Glue job transforms the raw data and loads it into the silver layer in
Delta Lake format.
- **silver_to_gold**: A Glue job consolidates, denormalises and aggregates the transformed data and
stores the delta tables in the gold layer.

## Data Modelling
<Data Modelling>

- **Ingress layer**: Serves as the ingestion layer, holding raw data in CSV format.
- **Bronze layer**: Contains raw data in Delta Lake format.
- **Silver layer**: Holds transformed delta tables as individual entities.
- **Gold layer**: Contains consumption-ready delta tables for data analysis.

## Data Insights
The analysis reveals several noteworthy findings that emphasise the impact of global warming on Australia, while acknowledging that climate change is not the sole cause of the observed changes:

*Click image for higher resolution*
<img src="https://github.com/TravisH0301/global_warming_disaster/blob/master/image/dashboard.jpg" width="1000">

1. A notable correlation exists among the global temperature index, temperature anomalies, and natural disaster occurrences in Australia. All of these metrics have been increasing over time, with the rate of change becoming more pronounced in recent years.
2. Australia has experienced a steeper incline in the yearly change of temperature anomalies. This trend suggests that the country is experiencing significant temperature shifts, which may contribute to the increased frequency and intensity of natural disasters.
3. Natural disaster occurrences in Australia are largely dominated by floods and bushfires, followed by severe storms. These events are becoming more frequent and severe, possibly due to a combination of global warming and other environmental factors.

It is essential to recognise that global climate change is not the sole cause of the observed changes in Australia. Other factors, such as land-use changes, urbanization, and local environmental conditions, may also play a role in driving temperature anomalies and natural disaster occurrences.

## Future Improvements
- **CI/CD**: Implement CI/CD with a Git repository to deploy changes in the source code to the data workflow
- **Data quality layer**: Add a data quality/validation layer to the data workflow to ensure data meets the specified SLA.

## Data Sources
- **Global temperature index** <br>
https://climate.nasa.gov/vital-signs/global-temperature/
- **Australian temperature anomalies** <br>
http://www.bom.gov.au/web01/ncc/www/cli_chg/timeseries/tmax/allmonths/aus/latest.txt
- **Australian natrual disaster** <br>
https://data.gov.au/dataset/ds-dga-26e2ebff-6cd5-4631-9653-18b56526e354/details?q=disaster
