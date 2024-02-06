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

def load_to_database(dataframe: pd.DataFrame, table_name: str, engine) -> None:
    """
    Function to load the data of a dataframe to a specified table in the database.
    :param dataframe: dataframe to load data from.
    :param table_name: table to load the data to.
    :return: None
    """
    dataframe.to_sql(table_name, engine, if_exists='append')

