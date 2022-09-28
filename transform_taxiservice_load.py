#!/usr/bin/env python

# Author: Karanpreet Kaur
# date: 2022-09-05

"""Transform and Load taxi service data"""
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
import urllib.parse
from sqlalchemy import create_engine

# Load environment file
load_dotenv()

# Read connection details
DB_HOST = os.environ.get("DB_HOST")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PORT = os.environ.get("DB_PORT")

def transform_taxiservice_tables(engine):
    """ Transformation of taxi service data for reporting """

    driver_query = '''SELECT * FROM dbo.driver'''
    cabride_query = '''SELECT * FROM dbo.cab_ride'''

    try:
        with engine.begin() as conn:
            result = conn.execute(driver_query)
            driver_details = result.fetchall()

            result = conn.execute(cabride_query)
            cabride_details = result.fetchall()

        driver_details = pd.DataFrame(driver_details, columns=['id', 'first_name', 'last_name', 'birth_date', 'driver_license_number', 'expiry_date', 'working'])
        cabride_details = pd.DataFrame(cabride_details, columns=['id', 'shift_id', 'ride_start_time', 'ride_end_time', 'address_starting_point', 'GPS_starting_point', 'address_destination', 'GPS_destination', 'canceled', 'payment_type_id', 'price'])

        conn.close()
        return driver_details, cabride_details

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
            

def load_taxiservice_to_dw(transformed_taxi_service_orders, engine):
    """Load transformed weblogs into target datawarehouse in dbo schema"""
    command = (
        """
        DROP TABLE IF EXISTS dbo.driver CASCADE;
        CREATE TABLE IF NOT EXISTS dbo.driver (
            id INTEGER PRIMARY KEY,
            first_name VARCHAR(128) NOT NULL,
            last_name VARCHAR(128) NOT NULL,
            birth_date DATE NOT NULL,
            driver_license_number VARCHAR(128) NOT NULL,
            expiry_date DATE NOT NULL,
            working BOOLEAN NOT NULL
        )
        """,
        """
        DROP TABLE IF EXISTS dbo.cab_ride CASCADE;
        CREATE TABLE IF NOT EXISTS dbo.cab_ride (
            id SERIAL PRIMARY KEY,
            shift_id INTEGER NOT NULL,
            ride_start_time TIMESTAMP NOT NULL,
            ride_end_time TIMESTAMP NOT NULL,
            address_starting_point TEXT NOT NULL,
            GPS_starting_point TEXT NOT NULL,
            address_destination TEXT NOT NULL,
            GPS_destination TEXT NOT NULL,
            canceled BOOLEAN,
            payment_type_id INTEGER NOT NULL,
            price DECIMAL(10, 2) NOT NULL CHECK (price > 0) 
        )
        """,
        """
        CREATE OR REPLACE VIEW dbo.vw_working_driver_expirylicense AS 
        SELECT 
            id,
            first_name,
            last_name
        FROM dbo.driver
        WHERE working = true
        AND date_part('year', expiry_date) = date_part('year', now()) + 1 
        """,
        """
        CREATE OR REPLACE VIEW dbo.vw_percentage_canceled_rides AS 
        SELECT 
	            ROUND(CAST(SUM(CASE WHEN canceled IS true THEN 1 ELSE 0 END)::float/COUNT(*) AS NUMERIC), 2) AS percentage_canceled_rides
        FROM dbo.cab_ride
        """
    )

    with engine.begin() as conn:
        conn.execute(command[0])
        conn.execute(command[1])
    transformed_taxi_service_orders[0].to_sql('driver', con=engine, if_exists='replace', index = False, schema='dbo')
    transformed_taxi_service_orders[1].to_sql('cab_ride', con=engine, if_exists='replace', index = False, schema='dbo')

    with engine.begin() as conn:
        conn.execute(command[2])
        conn.execute(command[3])
    
    conn.close()

def main():
    database_name = 'taxi_service'

    engine = create_engine(
    f"postgresql://{DB_USER}:%s@{DB_HOST}:{DB_PORT}/{database_name}" % urllib.parse.quote(DB_PASS))

    transformed_taxi_service_orders = transform_taxiservice_tables(engine)

    database_name = 'target'

    engine = create_engine(
    f"postgresql://{DB_USER}:%s@{DB_HOST}:{DB_PORT}/{database_name}" % urllib.parse.quote(DB_PASS))

    load_taxiservice_to_dw(transformed_taxi_service_orders, engine)
    
if __name__ == "__main__":
    main()