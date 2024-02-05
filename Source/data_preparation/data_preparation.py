import os
import json
import pandas as pd
import numpy as np
from ast import literal_eval


def get_files_in_directory():
    """
    Sets a path to JSON file.
    :returns a file name in a set path
    """
    path_to_files = '../data/input'     # <---- NEED TO FIX THIS PATH ISSUE!!!!!!
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
    path_to_files = '../data/input/'
    with open(path_to_files + file_json) as jfile:
        json_data = json.load(jfile)
        df = pd.DataFrame(pd.json_normalize(json_data))
        pd.set_option('display.max_columns', None)
    return df

