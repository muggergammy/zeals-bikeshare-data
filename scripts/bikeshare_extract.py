from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from pyspark.sql.functions import col, date_format, hour
from google.cloud import bigquery

def get_schema(df):
    schema = df.schema
    spark_schema = []
    for field in schema.fields:
        name = field.name
        spark_type = field.dataType.simpleString()

        # Generating schema based on the dataframe
        if spark_type == "string":
            bq_type = "STRING"
        elif spark_type == "timestamp":
            bq_type = "TIMESTAMP"
        elif spark_type == "float":
            bq_type = "FLOAT"
        elif spark_type == "double":
            bq_type = "FLOAT"
        elif spark_type == "integer":
            bq_type = "INTEGER"
        else:
            bq_type = "STRING"

        spark_schema.append((name, bq_type))

    return spark_schema

def create_external_table(bq_schema, bq_table_name, spark_schema):
    client = bigquery.Client()

    # Create the external table configuration
    table_ref = client.dataset(bq_schema).table(bq_table_name)
    external_config = bigquery.ExternalConfig("PARQUET")
    external_config.source_uris = [f'gs://zeals-assessment/{bq_schema}/{bq_table_name}']
    table = bigquery.Table(table_ref, schema=spark_schema, external_data_configuration=external_config)

    # Create or update the external table
    client.create_table(table, exists_ok=True)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Bikeshare ETL") \
    .getOrCreate()

# Calculate the previous day date
previous_day = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')

# BigQuery Details
bq_schema = "austin_bikeshare"
gcs_bucket="gs://bigquery-public-data/"
bq_table_name = "bikeshare_trips"
soruce_table='bq-public-rideshare'

df = spark.read \
    .format("bigquery") \
    .option("table", soruce_table) \
    .load()

# Filter data for the previous day
df_filtered = df.filter(df.start_time.startswith(previous_day))

spark_schema = get_schema(df_filtered)

#creasting hour and date fields for partition
df_filtered = df_filtered.withColumn("date", date_format(col("start_time"), "yyyy-MM-dd")) \
                         .withColumn("hour", hour(col("start_time")))
# GCS path for destination
gcs_path = f"gs://zeals-assessment/public/bikeshare/{previous_day}/data.parquet"


# Create or update the external table in BigQuery
create_external_table(bq_schema, bq_table_name, spark_schema)

df_filtered.write \
    .format('bigquery') \
    .option("temporaryGcsBucket", gcs_bucket) \
    .option("partitionField", "date") \
    .option("partitionType", "DAY") \
    .option("timePartitioning.field", "hour") \
    .option("timePartitioning.type", "HOUR") \
    .option("writeDisposition", "WRITE_APPEND") \  
    .option("table", bq_table) \
    .save()

spark.stop()
