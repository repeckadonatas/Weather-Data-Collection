from Source.db_functions.db_functions import MyDatabase
from Source.data_preparation import get_files_in_directory, create_dataframe
import Source.logger as log

main_logger = log.app_logger(__name__)


if __name__ == '__main__':
    with MyDatabase() as db:

        main_logger.info('Hello World')

        try:
            cities = get_files_in_directory()
            main_logger.info('A list of cities found: {}'.format(cities))
        except Exception as e:
            main_logger.info('Exception occurred: {}'.format(e))

        try:
            for city in cities:
                city_df = create_dataframe(city)
                city_df.head()
        except Exception as e:
            main_logger.info('Exception occurred: {}'.format(e))
