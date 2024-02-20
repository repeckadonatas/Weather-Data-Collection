#!/usr/bin/env python3
#-*- coding: utf-8 -*-

from Source.db_functions.db_functions import MyDatabase
import Source.data_preparation as dprep
import Source.get_weather_data as api_data
import Source.logger as log

import concurrent.futures
import threading
import time
from queue import Queue
from pathlib import Path

main_logger = log.app_logger(__name__)


def download_data():
    """
    Downloading data from weather API.
    Parsing city coordinates from a list of cities.
    Taking API key from a separate file.
    Excluding API weather data that is not needed.
    """
    try:
        locations = {}
        with open(Path(__file__).cwd() / 'Source/locations/locations.txt', 'r', encoding='utf-8') as file:
            for line in file:
                city, coords_str = line.strip().rstrip(',').split(': ')
                coords = [float(coord) for coord in coords_str.strip('[]').split(',')]
                locations[city] = coords

        with open(Path(__file__).cwd() / 'Source/credentials/api_key.txt', 'r', encoding='utf-8') as key:
            api_key = key.readline()

        exclude_weather_data = 'minutely, hourly, daily, alerts'

        get_data = api_data.get_weather_data(locations, api_key, exclude_weather_data)

    except Exception as e:
        main_logger.info('Exception occurred while downloading data: {}'.format(e))


def processing_data(queue, event):
    """
    Data preparation.
    Opening a dataframe, processing it and adding it to the queue.
    :param queue: a Queue object that contains processed data
    :param event: Event object that creates a thread to run this function.
    """
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

            event.set()
        except Exception as e:
            main_logger.info('Exception occurred while processing data: {}'.format(e))


def upload_to_db(queue, event):
    """
    Establishing database connection.
    Calls a function to create tables if they do not exist in the database.
    Loading data to the database.
    :param queue: a Queue object that contains processed data
    :param event: Event object that creates a thread to run this function.
    """
    with MyDatabase() as db:
        try:
            table = db.create_table()
        except Exception as e:
            main_logger.info('Exception occurred while creating a table: {}'.format(e))

        try:
            while not event.is_set() or not queue.empty():
                dataframe = queue.get()
                load_data = db.load_to_database(dataframe,
                                                'weather_data')
                main_logger.info('Data copied from a queue for a city: {}'.format({'city': dataframe['city'],
                                                                                   'country': dataframe['country']}))
        except Exception as e:
            main_logger.info('Exception occurred while loading data: {}'.format(e))


if __name__ == '__main__':

    # Concurrent data processing using Threads

    try:
        dataframe_queue = Queue(maxsize=20)
        this_event = threading.Event()
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            download_data = executor.submit(download_data)  # <- Producer
            time.sleep(5)

            for _ in concurrent.futures.as_completed([download_data]):
                processing_data = executor.submit(processing_data, dataframe_queue, this_event)  # <- Consumer/Producer
                time.sleep(0.5)

                concurrent.futures.wait([processing_data])

                upload_to_db = executor.submit(upload_to_db, dataframe_queue, this_event)  # <- Consumer

                this_event.set()  # Signal the event to start processing and uploading

                time.sleep(0.02)  # Allow some processing time for the data to be ready
                this_event.wait()  # Wait for the download script to finish

            main_logger.info('Data ready to be uploaded.\n')
    except Exception as e:
        main_logger.info('Threading exception occurred: {}'.format(e))

    main_logger.info('Data uploaded successfully.\n')
