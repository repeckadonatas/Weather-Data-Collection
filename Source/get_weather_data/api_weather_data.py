import requests


locations = {"Istanbul, Turkey": [41.01384, 28.94966],
             "London, United Kingdom": [51.50853, -0.12574],
             "Saint Petersburg, Russia": [59.93863, 30.31413]}

my_api_key = "0e4f42ad2d9820c67abc7df49b2a450c"
# my_api_key = "ChoLnguB8uVAaJtv9wnEOUQBNtXhEATR"

exclude_weather_data = 'minutely, hourly, daily, alerts'


for city in locations:
    coordinates = locations[city]
    lat = coordinates[0]
    lon = coordinates[1]
    print(city, [lat, lon])
    api_url = (f'https://api.openweathermap.org/data/3.0/onecall?'
               f'lat={lat}&'
               f'lon={lon}&'
               f'exclude={exclude_weather_data}&'
               f'appid={my_api_key}')

    headers = {'accept': 'application/json; charset=utf-8'}
    response = requests.get(api_url, headers=headers)
    json_response = response.json()
    print(response)
    print(json_response, '\n')



# for location in range(0, len(locations)):
#     for city in locations:
#         for [*coords] in city:
#             api_url = (f'https://api.tomorrow.io/v4/weather/realtime?location={coords}&'
#                        'fields=temperature&'
#                        'units=metric&'
#                        f'apikey={my_api_key}')
#
#             headers = {'Content-Type': 'application/json; charset=utf-8'}
#             response = requests.get(api_url, headers=headers)
#             json_response = response.json()
#             print(response)
#             print(json_response)




