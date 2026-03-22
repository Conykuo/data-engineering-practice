from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from zipfile import ZipFile
import os


def read_file(spark, file_path):
    with ZipFile(file_path, 'r') as zip_ref:
        with zip_ref.open(zip_ref.namelist()[0]) as f:
            content = f.read().decode('utf-8').splitlines()
            rdd = spark.sparkContext.parallelize(content)
            df = spark.read.csv(rdd, header=True, inferSchema=True)
    return df


def add_col(df, file_path):
    df = df.withColumn('source_file', F.lit(os.path.basename(file_path)))
    return df


def extract_date(df):
    date_pattern = r"(\d{4}-\d{2}-\d{2})"
    extracted_df = df.withColumn('file_date', F.to_date(F.regexp_extract(F.col('source_file'), date_pattern, 1)))
    return extracted_df


def name_brand(df):
    df = df.withColumn(
        'brand',
        F.when(F.col('model').contains(" "), F.split(df['model'], " ")[0])
        .otherwise('Unknown')
    )
    return df


def add_storage_ranking(df):
    # make sure 'model' in new df only contain unique value.
    model_capacity = df.select('model', 'capacity_bytes').groupBy('model').agg(
        F.max('capacity_bytes').alias('capacity_bytes'))
    # add rank
    window = Window.orderBy(F.desc('capacity_bytes'))
    model_capacity = model_capacity.withColumn('storage_ranking', F.rank().over(window)).drop('capacity_bytes')
    df = df.join(model_capacity, on='model', how='left')
    return df


def add_primary_key(df):
    # print(f"model distinct count{df.select('model').distinct().count()}") #66 unique value
    # print(f"serial_number distinct count{df.select('serial_number').distinct().count()}")  #206954 unique value
    # print(f"model and serial number distinct count{df.select('model','serial_number').distinct().count()}") #206954 unique value
    df = df.withColumn('primary_key', F.md5(F.col('serial_number')))
    return df


def main():
    spark = SparkSession.builder.appName("Exercise7").enableHiveSupport().getOrCreate()
    # your code her
    file_path = 'data/hard-drive-2022-01-01-failures.csv.zip'

    df = read_file(spark,file_path)
    df = add_col(df,file_path)
    df = extract_date(df)
    df = name_brand(df)
    df = add_storage_ranking(df)
    df = add_primary_key(df)
    df.limit(5).show()



if __name__ == "__main__":
    main()
