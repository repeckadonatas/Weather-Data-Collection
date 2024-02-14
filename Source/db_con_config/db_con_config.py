#!/usr/bin/env python3
#-*- coding: utf-8 -*-

import configparser
from configparser import SafeConfigParser
import Source.logger as log


# sqlalchemy engine connection should be set up here in order to use sqlalchemy with postgresql
parser_logger = log.app_logger(__name__)

FILENAME = 'Source/credentials/credentials.ini'
SECTION = 'turing-projects-2'   # change after completion of project


def get_config(filename=FILENAME, section=SECTION):
    """
    Parses the connection parameters from credentials.ini file.
    Parsed parameters are used to create a connection to the database.
    """

    parser = SafeConfigParser()
    parser.read(filename)

    db = {}
    try:
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    except (Exception, configparser.Error) as err:
        parser_logger.error('Parsing error: {0}'.format(err))
    return db
