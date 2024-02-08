import psycopg
from pathlib import Path
from sqlalchemy import create_engine, URL
from Source.data_preparation import load_to_database
from tabulate import tabulate

import Source.db_con_config as dbc
import Source.logger as log

db_logger = log.app_logger(__name__)

"""
Database connection functions.
Used to create a connection with a database
and pass data to it.
"""


class MyDatabase:

    def __init__(self):
        """
        Retrieves parsed config parameters from credentials.ini file.
        """
        try:
            self.params = dbc.get_config()
            # self.db_url = URL.create('postgresql+psycopg',    # <---- to connect using SQLAlchemy
            #                          username=self.params['user'],
            #                          password=self.params['password'],
            #                          host=self.params['host'],
            #                          port=self.params['port'],
            #                          database=self.params['dbname'])
        except Exception as err:
            db_logger.error("A configuration error has occurred: %s", err)

    def __enter__(self):
        """
        Creates a connection to the database when main.py is run.
        Requests for username and password.
        :return: connection to a database and creation of cursor object
        """

        try:
            # self.user = input('Enter username: ')
            # self.password = input('Enter password: ')
            # self.conn = psycopg.connect(**self.params, user=self.user, password=self.password)
            self.conn = psycopg.connect(**self.params)  # remove after project completion

            # self.engine = create_engine(self.db_url)    # <---- to connect using SQLAlchemy

            self.conn.autocommit = True
            self.cursor = self.conn.cursor()
            db_logger.info("Connected to the database")
        except (Exception, AttributeError) as err:
            db_logger.error("The following connection error has occurred: %s", err)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Closes the connection to the database once the program is terminated.
        :param exc_type: exception type
        :param exc_val: exception value
        :param exc_tb: exception traceback
        """

        try:
            if self.conn is not None:
                self.conn.close()
                db_logger.info('Connection closed')
            elif exc_val:
                raise
        except (Exception, AttributeError) as err:
            db_logger.error("Connection was not closed: %s", err)

    def create_table(self):
        """
        Creates a table in a database if it does not exist.
        """
        try:
            self.table_name = \
                """CREATE TABLE IF NOT EXISTS weather_data (
                id INT GENERATED ALWAYS AS IDENTITY UNIQUE PRIMARY KEY, 
                country_id INT, country VARCHAR(5), city VARCHAR(50), longitude FLOAT, latitude FLOAT,
                main_temp FLOAT, main_feels_like FLOAT, main_temp_min FLOAT, main_temp_max FLOAT,
                date DATE, timezone INT, sunrise DATE, sunset DATE, weather_id INT, weather_main VARCHAR(20),
                weather_description VARCHAR(100), weather_icon VARCHAR(50), pressure INT, humidity INT, 
                wind_speed FLOAT, wind_deg INT, clouds INT, visibility INT, base VARCHAR(20), sys_type INT, 
                sys_id INT, cod INT);"""

            self.cursor.execute(self.table_name)
            db_logger.info('Table was created successfully.')

            self.tables_in_db = self.cursor.execute("""SELECT relname FROM pg_class 
                                                       WHERE relkind='r' 
                                                       AND relname !~ '^(pg_|sql_)';""")
            db_logger.info('Tables found in a database: {}'.format(self.cursor.fetchall()))
        except Exception as e:
            db_logger.info("An error occurred while creating a table: {}".format(e))
