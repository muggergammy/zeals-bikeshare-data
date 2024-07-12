# Bikeshare ETL Project

# Structure
zeals-bikeshare-etl/
|-- dags/
|   |-- hourly_rideshare.py        # Airflow DAG definition
|-- scripts/
|   |-- rideshare_hourly_extract.py        spark and python code for extracting data and creating external table
|-- service_account.json    # Google service account credentials
|-- Dockerfile                  # Docker configuration for Airflow environment
|-- docker-compose.yml          # Docker Compose setup for Airflow services
|-- requirements.txt            # Python dependencies
|-- README.md                   # Project documentation

## Project Description

This project implements an ETL pipeline using Apache Airflow, Spark and Python to manage the end-to-end data flow for Bikeshare data. The pipeline extracts data from a public dataset in BigQuery, transforms and stores the data in Google Cloud Storage (GCS), 
and creates an external table in BigQuery for querying and analysis.

## Prerequisites

- Docker
- Apache Spark
- Apache Airflow
- Python 3.x

## Setup Instructions

1. **Clone the repository:**

	git clone https://github.com/your-username/bikeshare-etl.git
	cd bikeshare-etl

2. **Build the Docker Image:**
	docker build -t bikeshare-etl

3. **Start Airflow Services:**
	docker-compose up

4. **Access Airflow Web Interface:**
	http://localhost:8080

5. ** Set Up Airflow Connections:**
	Navigate to Admin > Connections in the Airflow UI.
	Click on "Create"
	Configure a new connection with the following details:
	Conn ID: google_cloud_default
	Conn Type: Google Cloud Platform
	Keyfile Path: /opt/airflow//service_account.json

7. **Trigger the DAG:**:
	In the Airflow UI, navigate to the DAGs list.
	Find bikeshare_etl and click on "Trigger DAG" to start the ETL pipeline.
