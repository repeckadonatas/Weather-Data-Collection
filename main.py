from Source.db_functions.db_functions import MyDatabase
import Source.data_preparation as dprep
import Source.get_weather_data as api_data
import Source.logger as log

import concurrent.futures
import threading
import time
from queue import Queue

main_logger = log.app_logger(__name__)


# if __name__ == '__main__':
#
#     # Downloading data from an API
#
#     try:
#         locations = {}
#         with open('Source/locations/locations.txt', 'r', encoding='utf-8') as file:
#             for line in file:
#                 city, coords_str = line.strip().rstrip(',').split(': ')
#                 coords = [float(coord) for coord in coords_str.strip('[]').split(',')]
#                 locations[city] = coords
#
#         with open('Source/credentials/api_key.txt', 'r', encoding='utf-8') as key:
#             api_key = key.readline()
#
#         exclude_weather_data = 'minutely, hourly, daily, alerts'
#
#         get_data = api_data.get_weather_data(locations, api_key, exclude_weather_data)
#
#     except Exception as e:
#         main_logger.info('Exception occurred: {}'.format(e))
#
#     # Data preparation
#
#     try:
#         cities = dprep.get_files_in_directory()
#         main_logger.info('A list of cities found: {}'.format(cities))
#
#         for city in cities:
#             city_df = dprep.create_dataframe(city)
#             main_logger.info('A dataframe was created for a file: {}'.format(city))
#
#             flatten_df = dprep.flatten_json_file(city_df, 'weather')
#             main_logger.info('Dataframe "{}" was flattened.'.format(city))
#
#             column_name_change = dprep.change_column_names(flatten_df)
#             main_logger.info('Dataframe "{}" columns were changed.'.format(city))
#
#             datetime_change = dprep.change_datetime_format(column_name_change)
#             main_logger.info('Dataframe "{}" date format was changed.'.format(city))
#
#             column_reorder = dprep.reorder_dataframe_columns(datetime_change)
#             main_logger.info('Dataframe columns were reordered.')
#
#     except Exception as e:
#         main_logger.info('Exception occurred: {}'.format(e))
#
#     # Establishing database connection.
#
#     with MyDatabase() as db:
#
#         # Creating tables if they do not exist in the database.
#
#         try:
#             table = db.create_table()
#         except Exception as e:
#             main_logger.info('Exception occurred while creating a table: {}'.format(e))
#
#         try:
#             # Loading data to the database
#             load_data = db.load_to_database(column_reorder,
#                                             'weather_data')
#             main_logger.info('Data copied from a file: {}'.format(city))
#             time.sleep(1)
#         except Exception as e:
#             main_logger.info('Exception occurred: {}'.format(e))


# CONCURRENCY TEST
# Downloading data from weather API
def download_data():
    try:
        locations = {}
        with open('Source/locations/locations.txt', 'r', encoding='utf-8') as file:
            for line in file:
                city, coords_str = line.strip().rstrip(',').split(': ')
                coords = [float(coord) for coord in coords_str.strip('[]').split(',')]
                locations[city] = coords

        with open('Source/credentials/api_key.txt', 'r', encoding='utf-8') as key:
            api_key = key.readline()

        exclude_weather_data = 'minutely, hourly, daily, alerts'

        get_data = api_data.get_weather_data(locations, api_key, exclude_weather_data)
    except Exception as e:
        main_logger.info('Exception occurred: {}'.format(e))


# Data preparation
def processing_data(queue, event):
    while not event.is_set():
        try:
            cities = dprep.get_files_in_directory()
            main_logger.info('A list of cities found: {}'.format(cities))

            for city in cities:
                city_df = dprep.create_dataframe(city)
                main_logger.info('A dataframe was created for a file: {}'.format(city))

                flatten_df = dprep.flatten_json_file(city_df, 'weather')
                main_logger.info('Dataframe "{}" was flattened.'.format(city))

                column_name_change = dprep.change_column_names(flatten_df)
                main_logger.info('Dataframe "{}" columns were changed.'.format(city))

                datetime_change = dprep.change_datetime_format(column_name_change)
                main_logger.info('Dataframe "{}" date format was changed.'.format(city))

                column_reorder = dprep.reorder_dataframe_columns(datetime_change)
                main_logger.info('Dataframe columns were reordered.')

                queue.put(column_reorder)

        except Exception as e:
            main_logger.info('Exception occurred: {}'.format(e))


def upload_to_db(queue, event):

    # Establishing database connection.

    with MyDatabase() as db:

        # Creating tables if they do not exist in the database.

        try:
            table = db.create_table()
        except Exception as e:
            main_logger.info('Exception occurred while creating a table: {}'.format(e))

        try:
            # Loading data to the database
            while not event.is_set() or not queue.empty():
                dataframe = queue.get()
                load_data = db.load_to_database(dataframe,
                                                'weather_data')
                main_logger.info('Data copied from a queue for a city: {}'.format({'city': dataframe['city'],
                                                                                   'country': dataframe['country']}))
        except Exception as e:
            main_logger.info('Exception occurred: {}'.format(e))


if __name__ == '__main__':

    # Concurrent data processing using Threads

    try:
        dataframe_queue = Queue(maxsize=0)
        event = threading.Event()
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            download_data = executor.submit(download_data)  # <- Producer
            time.sleep(1)
            processing_data = executor.submit(processing_data, dataframe_queue, event)  # <- Consumer/Producer
            upload_to_db = executor.submit(upload_to_db, dataframe_queue, event)  # <- Consumer

            time.sleep(0.01)
            event.set()
            event.wait()

            concurrent.futures.wait([download_data, processing_data, upload_to_db])
    except Exception as e:
        main_logger.info('Exception within concurrency: {}'.format(e))


# POSSIBLE SOLUTION FOR CONCURRENCY(??????):

# def download_data(weather_data, queue, event): <-- Producer

# def process_data(dataframe, queue, event): <-- Consumer/Producer

# def upload_to_db(database, queue, event): <-- Consumer

# if __name__ == '__main__':

# weather_data_queue = Queue(maxsize=20)
# dataframe_queue = Queue(maxsize=20)
# data_ready_event = threading.Event()
# with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
#     # weather_data = executor.submit(download_data, pipeline, data_ready_event)
#     # data_processing = executor.submit(process_data, pipeline, data_ready_event)
#     # upload_to_db = executor.submit(upload_to_db, pipeline, data_ready_event)

#     weather_data = executor.submit(subprocess.run, ['python', 'get_weather_data.py'])
#     data_processing = executor.submit(subprocess.run, ['python3', 'data_preparation.py'])
#     upload_to_db = executor.submit(subprocess.run, ['python3', 'db_functions.py'])
#
#     data_ready_event.wait()
#     time.sleep(2)
#
#     data_ready_event.set()
#
#     concurrent.futures.wait([weather_data, data_processing, upload_to_db])
