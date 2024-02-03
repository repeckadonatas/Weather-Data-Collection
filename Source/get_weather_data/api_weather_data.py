import requests
import pandas as pd
import json
from tabulate import tabulate


locations = {"Istanbul, Turkey": [41.01384, 28.94966],
             "London, United Kingdom": [51.50853, -0.12574],
             "Saint Petersburg, Russia": [59.93863, 30.31413]}

my_api_key = "0e4f42ad2d9820c67abc7df49b2a450c"

exclude_weather_data = 'minutely, hourly, daily, alerts'

for city in locations:
    coordinates = locations[city]
    lat = coordinates[0]
    lon = coordinates[1]
    print(city, [lat, lon])
    api_url = (f'https://api.openweathermap.org/data/2.5/weather?'
               f'lat={lat}&'
               f'lon={lon}&'
               f'exclude={exclude_weather_data}&'
               f'appid={my_api_key}&'
               'units=metric')

    headers = {'accept': 'application/json; charset=utf-8'}
    response = requests.get(api_url, headers=headers)
    json_response = response.json()
    print(response)
    print(json_response, '\n')

    with open('Source/misc/' + city + '_response.json', 'w', encoding='utf-8') as f:
        json.dump(json_response, f, ensure_ascii=False, indent=4)

    # df = pd.json_normalize(json_response)
    # df['dt'] = pd.to_datetime(df['dt'], unit='s')
    # print(tabulate(df, headers=df.columns, tablefmt='github'), '\n')

    df = pd.DataFrame(pd.json_normalize(json_response))
    print(tabulate(df, headers=df.columns, tablefmt='github'), '\n')
