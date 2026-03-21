import pathlib
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import zipfile
from pathlib import Path
from datetime import datetime, timedelta
import os, shutil

def main():
    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()

    # your code here
    dfs = []
    for zip_file in Path('/app/code/data/').glob('*.zip'):
        with zipfile.ZipFile(zip_file) as zip_ref: # zip_ref is a zipfile object
            file = zip_ref.namelist()[0] # get the file name in the zip
            print(file)
            with zip_ref.open(file) as csv_file:
                content = csv_file.read().decode('utf-8')
                rdd = spark.sparkContext.parallelize(content.splitlines())
                df = spark.read.csv(rdd, header=True, inferSchema=True)
                dfs.append(df)

    # print(f"df[0] col name: {dfs[0].columns}")
    # print(f"df[1] col name: {dfs[1].columns}") # this is the file we'll use to asnwer the questions
    df = dfs[1]

    # 1. What is the average trip duration per day?
    def avg_trip_duration_per_day(df,folder):
        df1 = df.withColumn('tripduration',regexp_replace(df['tripduration'],',','').cast('float'))
        df1 = df1.withColumn('start_date',to_date(df1['start_time'])).groupBy('start_date').agg(
            round(avg('tripduration'),2).alias('average_trip_duration'),
        ).orderBy('start_date')
        df1.coalesce(1).write.csv(f"{folder}/trip_duration",header=True)

    # 2. How many trips were taken each day?
    def trips_taken_per_day(df,folder):
        df1 = (df.withColumn('start_date',to_date(df['start_time'])).
         groupBy('start_date').agg(count('trip_id').alias('total_trips')).orderBy('start_date'))
        df1.coalesce(1).write.csv(f"{folder}/trips_taken_per_day",header=True)

    # 3. What was the most popular starting trip station for each month?
    def popular_trip_station_each_month(df,folder):
        df1 = df.withColumn('month',month(df['start_time'])).groupBy('month','from_station_name').agg(count('trip_id').alias('total_trips'))
        window = Window.partitionBy('month').orderBy(col('total_trips').desc())
        df1 = df1.withColumn('rank',rank().over(window)).filter(col('rank')==1)
        df1.coalesce(1).write.csv(f"{folder}/popular_trip_station_each_month",header=True)

    # 4. What were the top 3 trip stations each day for the last two weeks?
    def top_3_station(df,folder):
        max_date = df.withColumn('date',to_date(df['start_time'])).select(max('date').alias('max_date')).first()[0]
        two_weeks_ago = max_date - timedelta(days=14)
        df1 = (df.withColumn('date',to_date(df['start_time'])).filter(col('date') >= lit(two_weeks_ago)).
         groupBy('date','to_station_name').agg(count('to_station_name').alias('station_count')))
        # create a window: group by day and count station and get top 3
        window = Window.partitionBy('date').orderBy(col('station_count').desc())
        df1 = df1.withColumn('rank',rank().over(window)).filter(col('rank')<4).orderBy('date')
        df1.coalesce(1).write.csv(f"{folder}/top_3_station",header=True)

    # 5. Do Males or Females take longer trips on average?
    def male_or_female_longer_trip(df,folder):
        df1 = df.withColumn('tripduration', regexp_replace(df['tripduration'], ',', '').cast('float'))
        df1 = (df1.filter(col('gender')!='').groupBy('gender').agg(avg('tripduration').alias('trip_duration')).
         orderBy(col('trip_duration').desc()))
        df1.coalesce(1).write.csv(f"{folder}/male_or_female_longer_trip",header=True)

    # 6. What is the top 10 ages of those that take the longest trips, and shortest?
    def top_10_age_of_longest_trip(df,folder):
        df1 = df.withColumn('tripduration', regexp_replace(df['tripduration'], ',', '').cast('float'))
        df1 = df1.withColumn('age',(months_between(current_date(), to_date(col('birthyear').cast('string'), 'yyyy')) / 12).cast(
                          'int'))
        # order by trip duration, get top 10 and last 10
        longest = df1.filter(col('age').isNotNull()).orderBy(col('tripduration').desc()).limit(10)
        longest.coalesce(1).write.csv(f"{folder}/top_10_age_of_longest_trip",header=True)
        shortest = df1.filter(col('age').isNotNull()).orderBy(col('tripduration')).limit(10)
        shortest.coalesce(1).write.csv(f"{folder}/top_10_age_of_shortest_trip",header=True)

    if not pathlib.Path('/app/code/data/reports').exists():
        pathlib.Path('/app/code/data/reports').mkdir()

    avg_trip_duration_per_day(df,'/app/code/data/reports')
    trips_taken_per_day(df,'/app/code/data/reports')
    popular_trip_station_each_month(df,'/app/code/data/reports')
    top_3_station(df,'/app/code/data/reports')
    male_or_female_longer_trip(df,'/app/code/data/reports')
    top_10_age_of_longest_trip(df,'/app/code/data/reports')

    for file_path in Path('/app/code/data/reports').rglob('*.csv'):
        destination_dir = file_path.parent.parent
        destination_path = destination_dir / f"{file_path.parent.name}.csv"
        file_path.rename(destination_path)

    for file_path in Path('/app/code/data/reports').rglob('*'):
        if file_path.is_dir():
            shutil.rmtree(file_path)








if __name__ == "__main__":
    main()
