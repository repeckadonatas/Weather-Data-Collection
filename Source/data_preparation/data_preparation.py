import os
import json
import pandas as pd
import numpy as np
from pathlib import Path


def get_files_in_directory():
    """
    Sets a path to JSON file.
    :return a file name in a set path
    """

    path_to_files = Path(__file__).cwd() / 'Source/data/input/'
    files_in_path = os.scandir(path_to_files)

    list_of_files = []
    for file in files_in_path:
        if file.is_dir() or file.is_file():
            list_of_files.append(file.name)
            return list_of_files  # <----- don't forget to align this statement back with for!!!!!


def create_dataframe(file_json):
    """
    Creates a pandas dataframe from JSON file.
    Sets the maximum available columns to be shown.
    Requires name of the file.
    """
    path_to_files = Path(__file__).cwd() / 'Source/data/input/'
    with open(path_to_files / file_json) as jfile:
        json_data = json.load(jfile)
        df = pd.DataFrame(pd.json_normalize(json_data))
        pd.set_option('display.max_columns', None)
    return df


def flatten_json_file(dataframe: pd.DataFrame, row: str) -> pd.DataFrame:
    """
    Flattens the supplied dataframe and returns a new dataframe with flattened json data.
    :param dataframe: dataframe to flatten
    :return: new dataframe with flattened json data
    """
    dataframe_flat = dataframe[row].apply(pd.Series)
    dataframe_flat_0 = dataframe_flat[0].apply(pd.Series)
    dataframe_flat_0.columns = ['weather_id', 'weather_main', 'weather_description', 'weather_icon']
    dataframe_new = pd.concat([dataframe, dataframe_flat_0], axis=1)
    dataframe_new = dataframe_new.drop(columns=['weather'], axis=1)
    return dataframe_new


def change_column_names(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Changes the column names of the dataframe to their new column names.
    :param dataframe: a pandas dataframe to change column names
    :return: dataframe with new column names
    """
    new_names = {"dt": "date",
                 "id": "country_id",
                 "coord.lon": "longitude",
                 "coord.lat": "latitude",
                 "main.temp": "main_temp",
                 "main.feels_like": "main_feels_like",
                 "main.temp_min": "main_temp_min",
                 "main.temp_max": "main_temp_max",
                 "main.pressure": "pressure",
                 "main.humidity": "humidity",
                 "wind.speed": "wind_speed",
                 "wind.deg": "wind_deg",
                 "clouds.all": "clouds",
                 "sys.type": "sys_type",
                 "sys.id": "sys_id",
                 "sys.country": "country",
                 "sys.sunrise": "sunrise",
                 "sys.sunset": "sunset"}
    dataframe.rename(columns=new_names, inplace=True)
    return dataframe


def change_datetime_format(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Change datetime format of a given dataframe to ISO 8601 format
    :param dataframe: dataframe to change datetime format for.
    :return: dataframe with changed datetime format
    """
    dataframe['date'] = pd.to_datetime(dataframe['date'], unit='s', errors='coerce')
    dataframe['sunrise'] = pd.to_datetime(dataframe['sunrise'], unit='s', errors='coerce')
    dataframe['sunset'] = pd.to_datetime(dataframe['sunset'], unit='s', errors='coerce')
    return dataframe


def load_to_database(dataframe: pd.DataFrame, table_name: str, engine) -> None:
    """
    Function to load the data of a dataframe to a specified table in the database.
    :param dataframe: dataframe to load data from.
    :param table_name: table to load the data to.
    :return: None
    """
    dataframe.to_sql(table_name, engine, if_exists='append')
