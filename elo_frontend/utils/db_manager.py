"""@package db_manager
Database Manager

This script manages all the interactions with the database.

@file db_manager.py

@author Tyler Shake

@par Notifications:

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The below copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

@copyright Copyright 2019 Tyler Shake

"""

import logging
import logging.config
import os
import ConfigParser
import psycopg2
import pkg_resources
import appdirs

import elo_frontend.utils.exceptions as exceptions

class DBManager(object):
    """A database manager class.

    Args:
        db_user (string):   username for database access
        db_pass (string):   password for database access

    Attributes:
        attr1 (int):  First attribute
        attr2 (int):  Second attribute
        attr3 (int):  Third attribute

    Raises:
        IOError: Error accessing the config file or log file
        OSError: Error creating utility directory
        ConfigError: Error with logger level in config file

    """

    def __init__(self, db_user, db_pass):
        """Initializes database manager class."""

        # setup logger, config, and utility directory
        self._configure()

        self._logger.info("Connecting to database")
        self._db_user = db_user
        self._db_pass = db_pass
        self._db_name = self._config.get('options', 'db_name')
        self._db_host = self._config.get('options', 'db_host')
        self._db_port = self._config.get('options', 'db_port')
        self._db_conn = psycopg2.connect(database=self._db_name, user=self._db_user,
                                         password=self._db_pass, host=self._db_host,
                                         port=self._db_port)

    def _configure(self):

        # configure directories and files
        self._config_directory = appdirs.user_config_dir('elo_frontend')
        self._log_directory = appdirs.user_log_dir('elo_frontend')
        self._config_file = os.path.join(self._config_directory, 'elo_frontend.conf')
        if not os.path.isfile(self._config_file):
            self._create_user_config()
        self._log_file = os.path.join(self._log_directory, 'elo_frontend.log')
        if not os.path.isdir(self._log_directory):
            os.makedirs(self._log_directory)

        # configure logger
        self._logger = logging.getLogger("elo_frontend")
        if not self._logger.handlers:
            console = logging.StreamHandler()
            console.setLevel(logging.DEBUG)
            formatter = logging.Formatter(
                '%(asctime)s <%(levelname)s> [%(filename)s: %(lineno)d]: %(message)s')
            console.setFormatter(formatter)
            self._logger.addHandler(console)
            file_handle = logging.FileHandler(self._log_file)
            file_handle.setLevel(logging.DEBUG)
            file_handle.setFormatter(formatter)
            self._logger.addHandler(file_handle)

        # get log level from config file
        self._config = ConfigParser.RawConfigParser()
        self._config.read(self._config_file)

        if self._config.get('logger', 'level') == 'DEBUG':
            self._logger.setLevel(logging.DEBUG)
        elif self._config.get('logger', 'level') == 'INFO':
            self._logger.setLevel(logging.INFO)
        elif self._config.get('logger', 'level') == 'WARNING':
            self._logger.setLevel(logging.WARNING)
        elif self._config.get('logger', 'level') == 'ERROR':
            self._logger.setLevel(logging.ERROR)
        elif self._config.get('logger', 'level') == 'CRITICAL':
            self._logger.setLevel(logging.CRITICAL)
        else:
            raise exceptions.ConfigError("Invalid logger level in config file")

    def _create_user_config(self):

        source = pkg_resources.resource_stream('config', 'elo_frontend.conf')
        if not os.path.isdir(self._config_directory):
            os.makedirs(self._config_directory)
        with open(self._config_file, 'w') as destination:
            destination.writelines(source)
