## Crawler DAG – Airflow

This DAG is part of the data collection and processing pipeline ecosystem.
Its purpose is to orchestrate the automated execution of crawlers responsible for extracting, transforming, and loading data into an AWS data lake, ensuring the process runs on a scheduled, monitored, and scalable basis.

## Structure

The DAG is designed to run in environments using Apache Airflow as the main orchestrator.
It creates external tables in AWS Athena and triggers containerized crawlers configured to extract and process data.
The resulting datasets are stored in Amazon S3 in Parquet format, optimizing analytical performance and storage efficiency.
