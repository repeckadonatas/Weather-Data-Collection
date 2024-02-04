import requests
import json
import pandas as pd
from tabulate import tabulate
import Source.logger as log

# api_logger = log.app_logger(__name__)

# Should be in a separate file!!!
locations = {"Istanbul, Turkey": [41.01384, 28.94966],
             "London, United Kingdom": [51.50853, -0.12574],
             "Saint Petersburg, Russia": [59.93863, 30.31413]}

# Should be in a separate file!!!
my_api_key = "0e4f42ad2d9820c67abc7df49b2a450c"

exclude_weather_data = 'minutely, hourly, daily, alerts'

for city in locations:
    coordinates = locations[city]
    lat = coordinates[0]
    lon = coordinates[1]
    # print(city, [lat, lon])
    api_url = (f'https://api.openweathermap.org/data/2.5/weather?'
               f'lat={lat}&'
               f'lon={lon}&'
               f'exclude={exclude_weather_data}&'
               f'appid={my_api_key}&'
               'units=metric')

    headers = {'accept': 'application/json; charset=utf-8'}
    response = requests.get(api_url, headers=headers)
    json_response = response.json()
    # print(response)
    # print(json_response, '\n')

    # if json_response['cod'] != 200:
    #     api_logger.info(f'An error occurred: {json_response[""]}')
    # else:
    #     with open('../data/input/' + city + '_response.json', 'w', encoding='utf-8') as f:
    #         json.dump(json_response, f, ensure_ascii=False, indent=4)
    #         api_logger.info(f'Request successfully made for {city}.')

    with open('../data/input/' + city + '_response.json', 'w', encoding='utf-8') as f:
        json.dump(json_response, f, ensure_ascii=False, indent=4)
