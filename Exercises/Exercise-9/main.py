import polars as pl
from datetime import datetime


def main():
    lazy_df = pl.scan_csv('data/202306-divvy-tripdata.csv',schema_overrides=
    {'end_station_id':pl.String,
     'started_at': pl.Datetime,
     'ended_at': pl.Datetime
     })

    # 2. Count the number bike rides per day.
    number_bike_per_day = lazy_df.with_columns(pl.col('started_at').dt.date().alias('started_at_date')).group_by('started_at_date').agg(
        pl.len().alias('total_bike_rides')
    ).sort('started_at_date')
    # print(number_bike_per_day.head(10).collect())

    # 3. Calculate the average, max, and minimum number of rides per week of the dataset.
    week_agg_data = number_bike_per_day.with_columns(pl.col('started_at_date').dt.week().alias('week')).group_by('week').agg(
        pl.mean('total_bike_rides').round(2).alias('avg_bike_rides'),
        pl.max('total_bike_rides').alias('max_bike_rides'),
        pl.min('total_bike_rides').alias('min_bike_rides')
    ).sort('week',descending=False)
    # print(week_agg_data.head(10).collect())

    # 4. For each day, calculate how many rides that day is above or below the same day last week.
    wow_comparison = (lazy_df.with_columns(pl.col('started_at').dt.date().alias('date')).group_by('date').agg(total_rides = pl.len().cast(pl.Int32))
          .sort('date')
    .with_columns(
        week_number = pl.col('date').dt.week().alias('week_number'),
        same_day_last_week = pl.col('date').shift(7),
        total_rides_week_later =  pl.col('total_rides').shift(7).cast(pl.Int32),
        wow_rides_difference = pl.col('total_rides') - pl.col('total_rides').shift(7)
    ))

    print(wow_comparison.head(20).collect())


if __name__ == "__main__":
    main()
