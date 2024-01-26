import Source.logger as log
from Source.db_functions.db_functions import MyDatabase

main_logger = log.app_logger(__name__)


if __name__ == '__main__':
    with MyDatabase() as db:

        main_logger.info('Hello World')
