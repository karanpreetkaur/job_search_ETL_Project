#!/usr/bin/env python

# Author: Karanpreet Kaur
# date: 2022-09-27

"""Database implementation with generated data for the online taxi service database source"""

import os
from datetime import datetime
import pandas as pd
import psycopg2
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv
import urllib.parse

# Load environment file
load_dotenv()

# Configure logging
logging.basicConfig(filename='logs_taxiservice.log', level=logging.DEBUG)

# Read connection details
DB_HOST = os.environ.get("DB_HOST")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PORT = os.environ.get("DB_PORT")


def create_database(database_name, conn):
    """ Creates taxi_service database in the PostgreSQL database """
    connection = psycopg2.connect(**conn)
    connection.autocommit = True

    #Creating a cursor object using the cursor() method
    cursor = connection.cursor()
    logging.info(' '+ str(datetime.now()) + ' ' + "Connected to postgres database successfully")


    #Preparing query to create a database
    sql = f'''CREATE DATABASE {database_name}'''

    #Creating a database
    cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'taxi_service'")
    exists = cursor.fetchone()
    if not exists:
        try:
            cursor.execute(sql)
            logging.info(' '+ str(datetime.now()) + ' ' + "taxi_service created successfully")
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(' '+ str(datetime.now()) + ' ' + error)
            print(error)
        finally:
            if connection is not None:
                connection.close()
                logging.info(' '+ str(datetime.now()) + ' ' +'postgres database connection closed after taxi_service creation')
    else:
        logging.error(' '+ str(datetime.now()) + ' ' + "taxi_service already exists")


def create_tables(conn):
    """ create tables in the PostgreSQL database """
    commands = (
        """
        CREATE SCHEMA IF NOT EXISTS dbo
        """,
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
        DROP TABLE IF EXISTS dbo.car_model CASCADE;
        CREATE TABLE IF NOT EXISTS dbo.car_model (
                id INTEGER PRIMARY KEY,
                model_name VARCHAR(64) NOT NULL,
                model_description TEXT               
                )
        """,
        """ 
        DROP TABLE IF EXISTS dbo.cab CASCADE;
        CREATE TABLE IF NOT EXISTS dbo.cab (
                id INTEGER PRIMARY KEY,
                license_plate VARCHAR(32) NOT NULL,
                car_model_id INTEGER NOT NULL,
                manufacture_year INTEGER,
                owner_id INTEGER NOT NULL,
                active BOOLEAN NOT NULL,
                CONSTRAINT fk_car_model FOREIGN KEY(car_model_id) REFERENCES dbo.car_model(id),
                CONSTRAINT fk_driver FOREIGN KEY(owner_id) REFERENCES dbo.driver(id)         
                )
        """,
        """ 
        DROP TABLE IF EXISTS dbo.shift CASCADE;
        CREATE TABLE IF NOT EXISTS dbo.shift (
                id SERIAL PRIMARY KEY,
                driver_id INTEGER NOT NULL,
                cab_id INTEGER NOT NULL,
                shift_start_time TIMESTAMP NOT NULL,
                shift_end_time TIMESTAMP NOT NULL,
                login_time TIMESTAMP NOT NULL,
                logout_time TIMESTAMP NOT NULL,
                CONSTRAINT fk_driver_shift FOREIGN KEY(driver_id) REFERENCES dbo.driver(id),
                CONSTRAINT fk_cab FOREIGN KEY(cab_id) REFERENCES dbo.cab(id)         
                );
        """,
        """
        DROP TABLE IF EXISTS dbo.payment_id CASCADE;
        CREATE TABLE IF NOT EXISTS dbo.payment_id (
            id INTEGER PRIMARY KEY,
            type_name VARCHAR(128)
        )
        """,
        """
        DROP TABLE IF EXISTS dbo.cc_agent CASCADE;
        CREATE TABLE IF NOT EXISTS dbo.cc_agent (
            id INTEGER PRIMARY KEY,
            first_name VARCHAR(128),
            last_name VARCHAR(128)
        )
        """,
        """
        DROP TABLE IF EXISTS dbo.status CASCADE;
        CREATE TABLE IF NOT EXISTS dbo.status (
            id SERIAL PRIMARY KEY,
            status_name VARCHAR(128)
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
            price DECIMAL(10, 2) NOT NULL CHECK (price > 0),
            CONSTRAINT fk_shift FOREIGN KEY(shift_id) REFERENCES dbo.shift(id),
            CONSTRAINT fk_payment FOREIGN KEY(payment_type_id) REFERENCES dbo.payment_id(id) 
        )
        """,
        """
        DROP TABLE IF EXISTS dbo.cab_ride_status CASCADE;
        CREATE TABLE IF NOT EXISTS dbo.cab_ride_status (
                id SERIAL PRIMARY KEY,
                cab_ride_id INTEGER NOT NULL,
                status_id INTEGER NOT NULL,
                status_time TIMESTAMP NOT NULL,
                cc_agent_id INTEGER NOT NULL,
                shift_id INTEGER NOT NULL,
                status_detail TEXT,
                CONSTRAINT fk_cab_ride FOREIGN KEY(cab_ride_id) REFERENCES dbo.cab_ride(id),
                CONSTRAINT fk_status FOREIGN KEY(status_id) REFERENCES dbo.status(id),
                CONSTRAINT fk_cc_agent FOREIGN KEY(cc_agent_id) REFERENCES dbo.cc_agent(id),
                CONSTRAINT fk_cab_ride_shift FOREIGN KEY(shift_id) REFERENCES dbo.shift(id)           
        )
        """
        )
    try:
        conn = psycopg2.connect(**conn)
        conn.autocommit = True
        logging.info(' '+ str(datetime.now()) + ' ' + 'taxi_service database connection established')
        
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)

        populate_taxi_service_tables(cur)

        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        logging.error(' '+ str(datetime.now()) + ' ' + error)
    finally:
        if conn is not None:
            conn.close()
            logging.info(' '+ str(datetime.now()) + ' ' + 'taxi_service_database connection closed')

def populate_taxi_service_tables(cur):
    driver_query = '''
    INSERT INTO dbo.driver(id, first_name, last_name, birth_date, driver_license_number, expiry_date, working)
    SELECT
        seq AS id,
        'first_name' || seq AS first_name,
        'last_name' || seq AS first_name,
        timestamp '2014-01-10 20:00:00' + random() * (timestamp '1988-01-20 20:00:00' - timestamp '2014-01-10 10:00:00') AS birth_date,
        floor(random() * (210000 - 80000) + seq) AS driver_license_number,
        timestamp '2025-01-10 20:00:00' + random() * (timestamp '2019-01-20 20:00:00' - timestamp '2025-01-10 10:00:00') AS expiry_date,
        random() > 0.5 AS working
    FROM 
        GENERATE_SERIES(100, 110) seq;
    '''

    car_model_query = '''
    INSERT INTO dbo.car_model(id, model_name, model_description)
    SELECT
        seq AS id,
        'Model_' || seq AS model_name,
        '' AS model_description
    FROM 
        GENERATE_SERIES(17000, 17010) seq;
    '''

    cab_query = '''
    INSERT INTO dbo.cab(id, license_plate, car_model_id, manufacture_year, owner_id, active)
    SELECT
        seq AS id,
        'License_' || seq AS license_plate,
        floor(random() * (17010 - 17000) + 17000) AS car_model_id,
        floor(random() * (2023 - 2012) + 2012) AS manufacture_year,
        floor(random() * (110 - 100) + 100) AS owner_id,
        random() > 0.5 AS active
    FROM 
        GENERATE_SERIES(900, 910) seq;
    '''

    shift_query = '''
    ALTER SEQUENCE dbo.shift_id_seq RESTART WITH 1000;

    INSERT INTO dbo.shift(driver_id, cab_id, shift_start_time, shift_end_time, login_time, logout_time)
    SELECT
        driver_id,
        cab_id,
        timestamp '2020-01-10 20:00:00' + random() * (timestamp '2022-01-20 20:00:00' - timestamp '2020-01-10 10:00:00') AS shift_start_time,
        timestamp '2020-01-10 20:00:00' + random() * (timestamp '2022-01-20 20:00:00' - timestamp '2020-01-10 10:00:00') AS shift_end_time,
        timestamp '2020-01-10 20:00:00' + random() * (timestamp '2022-01-20 20:00:00' - timestamp '2020-01-10 10:00:00') AS login_time,
        timestamp '2020-01-10 20:00:00' + random() * (timestamp '2022-01-20 20:00:00' - timestamp '2020-01-10 10:00:00') AS logout_time
    FROM 
    (
        SELECT 
            DISTINCT owner_id AS driver_id, 
            id AS cab_id
        FROM 
            dbo.cab
    ) AS tbl
    '''

    payment_query = '''
    INSERT INTO dbo.payment_id(id, type_name)
    VALUES
    (
        2000,
        'cash'
    ),
    (
        2001,
        'credit'
    )
    '''

    cc_agent_query = '''
    INSERT INTO dbo.cc_agent(id, first_name, last_name)
    SELECT
        seq AS id,
        'first_name' AS first_name,
        'last_name' AS last_name
    FROM GENERATE_SERIES(3000, 3010) seq;
    '''

    status_query = '''
    ALTER SEQUENCE dbo.status_id_seq RESTART WITH 4000;
    INSERT INTO dbo.status(status_name)
    VALUES
    (
        'new ride'
    ),
    (
        'ride assigned to driver'
    ),
    (
        'ride started'
    ),
    (
        'ride ended'
    ),
    (
        'ride canceled'
    )
    '''

    cab_ride_query = '''
    ALTER SEQUENCE dbo.cab_ride_id_seq RESTART WITH 5000;

    INSERT INTO dbo.cab_ride(shift_id, ride_start_time, ride_end_time, address_starting_point, GPS_starting_point, address_destination, GPS_destination, canceled, payment_type_id, price)
    SELECT 
        shift_id,
        timestamp '2020-01-10 20:00:00' + random() * (timestamp '2022-01-20 20:00:00' - timestamp '2020-01-10 10:00:00') AS ride_start_time,
        timestamp '2020-01-10 20:00:00' + random() * (timestamp '2022-01-20 20:00:00' - timestamp '2020-01-10 10:00:00') AS ride_end_time,
        'Random Address' AS address_starting_point,
        'Random GPS' AS GPS_starting_point,
        'Random Address' AS address_destination,
        'Random GPS' AS GPS_destination,
        random() > 0.5 AS canceled,
        floor(random() * (2002 - 2000) + 2000) AS payment_type_id,
        floor(random() * 50 + 1) AS price
    FROM 
    (
    SELECT DISTINCT a.id AS shift_id
    FROM dbo.shift a
    ) AS tbl
    '''

    cab_ride_status_query = '''
    ALTER SEQUENCE dbo.cab_ride_status_id_seq RESTART WITH 6000;

    INSERT INTO dbo.cab_ride_status(cab_ride_id, status_id, status_time, cc_agent_id, shift_id, status_detail) 
    SELECT 
        cr.id AS cab_ride_id,
		floor(random() * (4005 - 4000) + 4000) AS status_id,
        timestamp '2020-01-10 20:00:00' + random() * (timestamp '2022-01-20 20:00:00' - timestamp '2020-01-10 10:00:00') AS status_time,
		floor(random() * (3010 - 3000) + 3000) AS cc_agent_id,
		cr.shift_id AS shift_id,
		'' AS status_detail	   
    FROM dbo.cab_ride cr
    '''
    
    try:
        
        # populate table one by one
        cur.execute(driver_query)
        cur.execute(car_model_query)
        cur.execute(cab_query)
        cur.execute(shift_query)
        cur.execute(payment_query)
        cur.execute(cc_agent_query)
        cur.execute(status_query)
        cur.execute(cab_ride_query)
        cur.execute(cab_ride_status_query)

        # close communication with the PostgreSQL database server
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        logging.error(' '+ str(datetime.now()) + ' ' +error)
    

def main():
    
    conn = {
        "host": DB_HOST,
        "dbname": DB_NAME,
        "user": DB_USER,
        "password": DB_PASS,
        "port": DB_PORT,
    }

    # Taxi service database
    database_name = 'taxi_service'
    create_database(database_name, conn)

    conn = {
            "host": DB_HOST,
            "dbname": database_name,
            "user": DB_USER,
            "password": DB_PASS,
            "port": DB_PORT,
        }

    create_tables(conn)
    logging.info(' '+ str(datetime.now()) + ' ' + 'dbo.driver, dbo.car_model, dbo.cab, dbo.shift, dbo.cab_ride, dbo.cab_ride_status in taxi_service_database created successfully\n')

if __name__ == "__main__":
    main()