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

    def test_connection(self):
        """
        Testing connection with the database.
        :return: If successful, returns database version
                and connection parameters.
                Upon failure to connect, returns an error.
        """
        self.cursor.execute("""SELECT version()""")
        self.db_version = self.cursor.fetchone()
        db_logger.info('PostgreSQL version: %s\n', self.db_version)
        db_logger.info('PostgreSQL connection properties: %s\n', self.conn.get_dsn_parameters())

    def create_table(self):
        """
        Creates a table in a database.
        Enter table name and column name(s) with datatype.
        The command will continue to ask for inputs
        until it is interrupted by a KeyboardInterrupt (depends on IDE)
        (for PyCharm hit 'Enter' to pass empty inputs to break out of the loop).
        """
        while True:
            try:
                self.table_name = input('Enter a table name: ')
                self.col_type = input('Enter column names and data types: ')
                self.new_table = f"""CREATE TABLE IF NOT EXISTS 
                    {self.table_name} ({self.col_type});"""

                self.cursor.execute(self.new_table)
                db_logger.info('Table "%s" was created successfully.\n', self.table_name)
            except KeyboardInterrupt:
                raise StopIteration

    def check_if_created(self):
        """
        Returns a list of tables in the connected database.
        """
        self.cursor.execute("""SELECT relname FROM pg_class 
                               WHERE relkind='r' 
                               AND relname !~ '^(pg_|sql_)';""")
        db_logger.info('Tables in the database: %s\n', self.cursor.fetchall())

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

    def my_custom_sql(self):
        """
        Allows to write a custom SQL statement to manipulate database
        and data in a database.
        The command will continue to ask for input
        until it is interrupted by a KeyboardInterrupt (depends on IDE)
        (for PyCharm hit 'Enter' to pass empty inputs to break out of the loop).
        When data is returned it is returned in a tabular format.
        """
        while True:
            try:
                self.custom_sql = input('SQL statement: ')
                self.cursor.execute(f'''{self.custom_sql};''')

                if self.custom_sql.startswith('select'):
                    self.col_names = [self.desc[0] for self.desc in self.cursor.description]
                    self.rows = self.cursor.fetchall()
                    self.headers = self.col_names
                    self.table = self.rows

                    db_logger.info('SQL statement executed: "%s"\n', self.custom_sql)
                    db_logger.info('Table info: \n%s\n', tabulate(self.table, self.headers, tablefmt="github"))
                else:
                    db_logger.info('SQL statement executed: "%s"\n', self.custom_sql)
            except KeyboardInterrupt:
                raise StopIteration
