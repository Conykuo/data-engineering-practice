from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    unix_timestamp,
    sum as _sum,
    date_format,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)
import great_expectations as ge
import inspect


# Create a SparkSession
spark = SparkSession.builder.appName("BikeRideDuration").getOrCreate()

# Get the Great Expectation Data Context
context = ge.get_context()

# Define the schema based on the provided CSV structure
schema = StructType([
    StructField("ride_id", StringType(), True),
    StructField("rideable_type", StringType(), True),
    StructField("started_at", StringType(), True),
    StructField("ended_at", StringType(), True),
    StructField("start_station_name", StringType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_name", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("start_lat", DoubleType(), True),
    StructField("start_lng", DoubleType(), True),
    StructField("end_lat", DoubleType(), True),
    StructField("end_lng", DoubleType(), True),
    StructField("member_casual", StringType(), True),
])

input_csv_path = "data/202306-divvy-tripdata.csv"

df = spark.read.csv(
    input_csv_path,
    header=True,
    schema=schema,
    mode="DROPMALFORMED"
)

df = df.withColumn(
    "started_at", to_timestamp(col("started_at"), "yyyy-MM-dd HH:mm:ss")
).withColumn(
    "ended_at", to_timestamp(col("ended_at"), "yyyy-MM-dd HH:mm:ss")
)

df = df.withColumn(
    "duration_seconds",
    unix_timestamp(col("ended_at")) - unix_timestamp(col("started_at"))
)

df = df.withColumn(
    "date", date_format(col("started_at"), "yyyy-MM-dd")
)

daily_durations = df.groupBy("date").agg(
    _sum("duration_seconds").alias("total_duration_seconds")
)

# Create a greate expectation validator
datasource = context.data_sources.add_spark(name='datasource')
data_asset = datasource.add_dataframe_asset(name="my_asset")
batch_request = data_asset.build_batch_request(options={"dataframe": df})
validator = context.get_validator(batch_request=batch_request)
result = validator.expect_column_values_to_be_between(
    column="duration_seconds", min_value=0, max_value=86400
)
if not result["success"]:
    raise Exception("Data quality check failed. Ride duration should be between 0 and 86400.")


output_parquet_path = "results/output_file.parquet"
daily_durations.write.mode("overwrite").parquet(output_parquet_path)


