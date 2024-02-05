import psycopg
from sqlalchemy import create_engine, URL
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

    def create_tables(self):
        """
        Creates a table in a database if it does not exist.
        """
        try:
            self.historical_weather_data = \
                """CREATE TABLE IF NOT EXISTS historical_weather_data (id INT PRIMARY KEY, weather VARCHAR(100),
                base VARCHAR, visibility INT, dt DATE, timezone INT, id INT);"""

            self.cursor.execute(self.historical_weather_data)
            db_logger.info('Table {} was created successfully.\n'.format(self.table_name))
        except Exception as e:
            db_logger.info("An error occurred while creating a table: {}".format(e))

    def copy_to_table(self):
        """
        Populates a table with data.
        Opens a provided CSV file in memory and uses
        copy_expert() function in combination with
        COPY SQL statement to load data to the table.
        The command will continue to ask for inputs
        until it is interrupted by a KeyboardInterrupt (depends on IDE)
        (for PyCharm hit 'Enter' to pass empty inputs to break out of the loop).
        """
        while True:
            try:
                self.table_name = input('Enter table to copy to: ')
                self.col_name = input('Enter column names: ')
                self.file_name = input('Enter CSV file to copy from: ')
                self.open_csv = open(f'Source/data/output/{self.file_name}.csv', encoding='utf8')
                self.load_csv = f"""COPY {self.table_name} ({self.col_name})
                                    FROM STDIN
                                    WITH
                                        DELIMITER ','
                                        CSV HEADER;
                                """

                self.cursor.copy_expert(sql=self.load_csv, file=self.open_csv)
                print('\n')
                db_logger.info(f'Copied CSV data from file "{self.file_name}.csv" to table "{self.table_name}"\n')
            except KeyboardInterrupt:
                raise StopIteration
