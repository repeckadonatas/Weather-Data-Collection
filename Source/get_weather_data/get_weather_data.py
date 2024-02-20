#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import json
import Source.logger as log
from pathlib import Path

api_logger = log.app_logger(__name__)


def get_weather_data(locations: dict,
                     api_key: str,
                     exclude_weather_data: str):
    """
    Get weather data for a given coordinates of a city.
    A JSON file is created containing weather data for every city as
    it's coordinates are being provided.
    :param locations: a dictionary with 'lat' and 'lon' values of a city
    :param api_key: API key for weather API service used
    :param exclude_weather_data: weather data to exclude from API response
    """
    path_to_data_storage = Path(__file__).cwd() / 'Source/data/input/'
    for city in locations:
        coordinates = locations[city]
        lat = coordinates[0]
        lon = coordinates[1]
        api_url = (f'https://api.openweathermap.org/data/2.5/weather?'
                   f'lat={lat}&'
                   f'lon={lon}&'
                   f'exclude={exclude_weather_data}&'
                   f'appid={api_key}&'
                   'units=metric')

        headers = {'accept': 'application/json; charset=utf-8'}
        response = requests.get(api_url, headers=headers)
        json_response = response.json()

        if json_response['cod'] != 200:
            api_logger.info(f'An error occurred: {json_response["cod"]}')
        else:
            with open(path_to_data_storage / (city + '_response.json'), 'w', encoding='utf-8') as f:
                json.dump(json_response, f, ensure_ascii=False, indent=4)
                api_logger.info('Downloading weather API data for a city {}...'.format(city))
