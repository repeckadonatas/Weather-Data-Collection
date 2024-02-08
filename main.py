from Source.db_functions.db_functions import MyDatabase
import Source.data_preparation as data_preparation
import Source.get_weather_data as get_weather_data
import Source.logger as log

main_logger = log.app_logger(__name__)

if __name__ == '__main__':

    # Downloading data from an API

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

        weather_data = get_weather_data.get_weather_data(locations, api_key, exclude_weather_data)
    except Exception as e:
        main_logger.info('Exception occurred: {}'.format(e))

    # Data preparation

    try:
        cities = data_preparation.get_files_in_directory()
        main_logger.info('A list of cities found: {}'.format(cities))
    except Exception as e:
        main_logger.info('Exception occurred: {}'.format(e))

    try:
        for city in cities:
            city_df = data_preparation.create_dataframe(city)
            main_logger.info('A dataframe was created for a file: {}'.format(city))

            flatten_df = data_preparation.flatten_json_file(city_df, 'weather')
            main_logger.info('Dataframe "{}" was flattened.'.format(city))

            column_name_change = data_preparation.change_column_names(flatten_df)
            main_logger.info('Dataframe "{}" columns were changed.'.format(city))

            datetime_change = data_preparation.change_datetime_format(column_name_change)
            main_logger.info('Dataframe "{}" date format was changed.'.format(city))

            column_reorder = data_preparation.reorder_dataframe_columns(datetime_change)
            main_logger.info('Dataframe columns were reordered.')
    except Exception as e:
        main_logger.info('Exception occurred: {}'.format(e))


    # Establishing database connection.
    with MyDatabase() as db:

        # Creating tables if they do not exist in the database.

        try:
            table = db.create_table()
        except Exception as e:
            main_logger.info('Exception occurred: {}'.format(e))

        # Loading data to the database

        try:
            load_data = data_preparation.load_to_database(column_reorder,
                                                          'historical_weather_data',
                                                          engine=None)
            main_logger.info('Data copied: {}'.format(load_data))
        except Exception as e:
            main_logger.info('Exception occurred: {}'.format(e))
