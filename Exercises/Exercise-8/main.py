import duckdb

def insert_data(conn,table,filename):
    conn.execute(f"INSERT INTO {table} SELECT * FROM '{filename}'")

def electric_car_per_city(conn,table):
    # 1. Count the number of electric cars per city.
    conn.sql(f"""
             SELECT city, count(*) as number_of_car
             FROM {table}
             GROUP BY city
             """).show()


def top3_popular_electric_vehicle(conn,table):
    # 2. Find the top 3 most popular electric vehicles.
    conn.sql(f"""
             SELECT make, model, count(*) as number_of_car
             FROM {table}
             GROUP BY make, model
             ORDER BY number_of_car DESC LIMIT 3
             """).show()


def most_popular_electric_vehicle_by_postal_code(conn,table):
    # 3. Find the most popular electric vehicle in each postal code.
    conn.sql(f"""

             SELECT *
             FROM (SELECT postal_code,
                          make,
                          model,
                          number_of_car,
                          RANK() OVER (PARTITION BY postal_code ORDER BY number_of_car DESC) AS rank
                   FROM (SELECT postal_code, make, model, count(*) as number_of_car
                         FROM {table}
                         GROUP BY postal_code, make, model) AS vehicle_count_per_postal_code)
             WHERE rank = 1

             """).show()

def electric_car_count_per_year_to_parquet(conn,table):
    # 4. Count the number of electric cars by model year. Write out the answer as parquet files partitioned by year.
    conn.execute(f"""COPY (
    SELECT model_year, count(*) as number_of_car
    FROM {table}
    GROUP BY model_year
    ORDER BY model_year DESC
    ) TO 'number_of_electric_car_by_year' 
    (FORMAT PARQUET, PARTITION_BY model_year,OVERWRITE_OR_IGNORE TRUE )""")

def unique_value(conn,col_name,table):
    conn.sql(f"""
    SELECT {col_name}, count(*) as number_of_car
    FROM {table}
    GROUP BY {col_name}
    ORDER BY number_of_car DESC LIMIT 5
    """).show()


def main():
    csv_path = 'data/Electric_Vehicle_Population_Data.csv'
    conn = duckdb.connect()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS electric_car (
        VIN VARCHAR,
        county VARCHAR,
        city VARCHAR,
        state_code CHAR(2),
        postal_code VARCHAR,
        model_year SMALLINT,
        make VARCHAR,
        model VARCHAR,
        electric_vehicle_type VARCHAR,
        CAFV VARCHAR,
        electric_range INT,
        base_msrp INT,
        legislative_district INT,
        dol_vehicle_id VARCHAR PRIMARY KEY,
        vehicle_location VARCHAR,
        electric_utility VARCHAR,
        census_tract_2020 CHAR(11)
    )
    """
    conn.execute(create_table_query)


    insert_data(conn,'electric_car',csv_path)
    electric_car_per_city(conn,'electric_car')
    top3_popular_electric_vehicle(conn,'electric_car')
    most_popular_electric_vehicle_by_postal_code(conn,'electric_car')
    electric_car_count_per_year_to_parquet(conn,'electric_car')

    # just for finding primary key
    # unique_value(conn,'VIN','electric_car')
    # unique_value(conn,'dol_vehicle_id','electric_car')


    conn.close()

if __name__ == "__main__":
    main()
