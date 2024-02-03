import Source.logger as log
from Source.db_functions.db_functions import MyDatabase

main_logger = log.app_logger(__name__)


if __name__ == '__main__':
    with MyDatabase() as db:

        main_logger.info('Hello World')

    with open('Source/locations/locations.txt', 'r', encoding='utf-8') as file:
        lines = file.read()
        main_logger.info('Locations: \n%s\n', lines)
