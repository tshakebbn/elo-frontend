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
import time
import MySQLdb
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

        time.sleep(10)
        self._logger.info("Connecting to database")
        self._db_user = db_user
        self._db_pass = db_pass
        self._db_name = self._config.get('options', 'db_name')
        self._db_host = self._config.get('options', 'db_host')
        self._db_port = self._config.get('options', 'db_port')
        self._db_conn = MySQLdb.connect(user=self._db_user, passwd=self._db_pass,
                                        host=self._db_host, db=self._db_name)
        cursor = self._db_conn.cursor()
        self._logger.info("Creating tables")

        cursor.execute("CREATE TABLE IF NOT EXISTS rating (\
rating_id INT NOT NULL AUTO_INCREMENT,\
mu DECIMAL(6,4) NOT NULL,\
sigma DECIMAL(6,4) NOT NULL,\
time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\
PRIMARY KEY (rating_id),\
UNIQUE INDEX rating_id_UNIQUE (rating_id ASC))")

        cursor.execute("CREATE TABLE IF NOT EXISTS player (\
player_id INT NOT NULL AUTO_INCREMENT,\
first_name VARCHAR(45) NOT NULL,\
last_name VARCHAR(45) NOT NULL,\
nickname VARCHAR(45) NULL,\
time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\
fb_offense_rating INT NOT NULL,\
fb_defense_rating INT NOT NULL,\
mk_ind_rating INT NOT NULL,\
mp_ind_rating INT NOT NULL,\
ss_ind_rating INT NOT NULL,\
pp_ind_rating INT NOT NULL,\
PRIMARY KEY (player_id),\
UNIQUE INDEX player_id_UNIQUE (player_id ASC),\
INDEX fb_offense_rating_idx (fb_offense_rating ASC),\
INDEX fb_defense_rating_idx (fb_defense_rating ASC),\
INDEX mk_rating_ind_idx (mk_ind_rating ASC),\
INDEX mp_rating_ind_idx (mp_ind_rating ASC),\
INDEX ss_rating_ind_idx (ss_ind_rating ASC),\
INDEX pp_rating_ind_idx (pp_ind_rating ASC),\
CONSTRAINT fb_offense_rating \
FOREIGN KEY (fb_offense_rating) \
REFERENCES rating (rating_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT fb_defense_rating \
FOREIGN KEY (fb_defense_rating) \
REFERENCES rating (rating_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT mk_ind_rating \
FOREIGN KEY (mk_ind_rating) \
REFERENCES rating (rating_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT mp_ind_rating \
FOREIGN KEY (mp_ind_rating) \
REFERENCES rating (rating_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT ss_ind_rating \
FOREIGN KEY (ss_ind_rating) \
REFERENCES rating (rating_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT pp_ind_rating \
FOREIGN KEY (pp_ind_rating) \
REFERENCES rating (rating_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION)")

        cursor.execute("CREATE TABLE IF NOT EXISTS team (\
team_id INT NOT NULL AUTO_INCREMENT,\
team_name VARCHAR(75) NOT NULL,\
time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\
fb_team_rating INT NOT NULL,\
mk_team_rating INT NOT NULL,\
mp_team_rating INT NOT NULL,\
ss_team_rating INT NOT NULL,\
PRIMARY KEY (team_id),\
UNIQUE INDEX team_id_UNIQUE (team_id ASC),\
UNIQUE INDEX team_name_UNIQUE (team_name ASC),\
INDEX fb_team_rating_idx (fb_team_rating ASC),\
INDEX mk_team_rating_idx (mk_team_rating ASC),\
INDEX mp_team_rating_idx (mp_team_rating ASC),\
INDEX ss_team_rating_idx (ss_team_rating ASC),\
CONSTRAINT fb_team_rating \
FOREIGN KEY (fb_team_rating) \
REFERENCES rating (rating_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION, \
CONSTRAINT mk_team_rating \
FOREIGN KEY (mk_team_rating) \
REFERENCES rating (rating_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION, \
CONSTRAINT mp_team_rating \
FOREIGN KEY (mp_team_rating) \
REFERENCES rating (rating_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION, \
CONSTRAINT ss_team_rating \
FOREIGN KEY (ss_team_rating) \
REFERENCES rating (rating_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION)")

        cursor.execute("CREATE TABLE IF NOT EXISTS player_team_xref (\
player INT NOT NULL,\
team INT NOT NULL,\
INDEX player_idx (player ASC),\
INDEX team_idx (team ASC),\
CONSTRAINT player \
FOREIGN KEY (player) \
REFERENCES player (player_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT team \
FOREIGN KEY (team) \
REFERENCES team (team_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION)")

        cursor.execute("CREATE TABLE IF NOT EXISTS fb_result (\
result_id INT NOT NULL AUTO_INCREMENT,\
offense_winner INT NOT NULL,\
defense_winner INT NOT NULL,\
offense_loser INT NOT NULL,\
defense_loser INT NOT NULL,\
time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\
PRIMARY KEY (result_id),\
UNIQUE INDEX result_id_UNIQUE (result_id ASC),\
INDEX offense_winner_idx (offense_winner ASC),\
INDEX defense_winner_idx (defense_winner ASC),\
INDEX offense_loser_idx (offense_loser ASC),\
INDEX defense_loser_idx (defense_loser ASC),\
CONSTRAINT offense_winner \
FOREIGN KEY (offense_winner) \
REFERENCES player (player_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT defense_winner \
FOREIGN KEY (defense_winner) \
REFERENCES player (player_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT offense_loser \
FOREIGN KEY (offense_loser) \
REFERENCES player (player_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT defense_loser \
FOREIGN KEY (defense_loser) \
REFERENCES player (player_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION)")

        cursor.execute("CREATE TABLE IF NOT EXISTS mk_ind_result (\
result_id INT NOT NULL AUTO_INCREMENT,\
mk_ind_first INT NOT NULL,\
mk_ind_second INT NOT NULL,\
mk_ind_third INT NULL,\
mk_ind_fourth INT NULL,\
time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\
PRIMARY KEY (result_id),\
UNIQUE INDEX result_id_UNIQUE (result_id ASC),\
INDEX mk_ind_first_idx (mk_ind_first ASC),\
INDEX mk_ind_second_idx (mk_ind_second ASC),\
INDEX mk_ind_third_idx (mk_ind_third ASC),\
INDEX mk_ind_fourth_idx (mk_ind_fourth ASC),\
CONSTRAINT mk_ind_first \
FOREIGN KEY (mk_ind_first) \
REFERENCES player (player_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT mk_ind_second \
FOREIGN KEY (mk_ind_second) \
REFERENCES player (player_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT mk_ind_third \
FOREIGN KEY (mk_ind_third) \
REFERENCES player (player_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT mk_ind_fourth \
FOREIGN KEY (mk_ind_fourth) \
REFERENCES player (player_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION)")

        cursor.execute("CREATE TABLE IF NOT EXISTS mk_team_result (\
result_id INT NOT NULL AUTO_INCREMENT,\
mk_team_first INT NOT NULL,\
mk_team_second INT NOT NULL,\
time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\
PRIMARY KEY (result_id),\
UNIQUE INDEX result_id_UNIQUE (result_id ASC),\
INDEX mk_team_first_idx (mk_team_first ASC),\
INDEX mk_team_second_idx (mk_team_second ASC),\
CONSTRAINT mk_team_first \
FOREIGN KEY (mk_team_first) \
REFERENCES team (team_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT mk_team_second \
FOREIGN KEY (mk_team_second) \
REFERENCES team (team_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION)")

        cursor.execute("CREATE TABLE IF NOT EXISTS mp_ind_result (\
result_id INT NOT NULL AUTO_INCREMENT,\
mp_ind_first INT NOT NULL,\
mp_ind_second INT NOT NULL,\
mp_ind_third INT NULL,\
mp_ind_fourth INT NULL,\
time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\
PRIMARY KEY (result_id),\
UNIQUE INDEX result_id_UNIQUE (result_id ASC),\
INDEX mp_ind_first_idx (mp_ind_first ASC),\
INDEX mp_ind_second_idx (mp_ind_second ASC),\
INDEX mp_ind_third_idx (mp_ind_third ASC),\
INDEX mp_ind_fourth_idx (mp_ind_fourth ASC),\
CONSTRAINT mp_ind_first \
FOREIGN KEY (mp_ind_first) \
REFERENCES player (player_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT mp_ind_second \
FOREIGN KEY (mp_ind_second) \
REFERENCES player (player_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT mp_ind_third \
FOREIGN KEY (mp_ind_third) \
REFERENCES player (player_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT mp_ind_fourth \
FOREIGN KEY (mp_ind_fourth) \
REFERENCES player (player_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION)")

        cursor.execute("CREATE TABLE IF NOT EXISTS mp_team_result (\
result_id INT NOT NULL AUTO_INCREMENT,\
mp_team_first INT NOT NULL,\
mp_team_second INT NOT NULL,\
time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\
PRIMARY KEY (result_id),\
UNIQUE INDEX result_id_UNIQUE (result_id ASC),\
INDEX mp_team_first_idx (mp_team_first ASC),\
INDEX mp_team_second_idx (mp_team_second ASC),\
CONSTRAINT mp_team_first \
FOREIGN KEY (mp_team_first) \
REFERENCES team (team_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT mp_team_second \
FOREIGN KEY (mp_team_second) \
REFERENCES team (team_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION)")

        cursor.execute("CREATE TABLE IF NOT EXISTS ss_ind_result (\
result_id INT NOT NULL AUTO_INCREMENT,\
ss_ind_first INT NOT NULL,\
ss_ind_second INT NOT NULL,\
ss_ind_third INT NULL,\
ss_ind_fourth INT NULL,\
ss_ind_fifth INT NULL,\
ss_ind_sixth INT NULL,\
ss_ind_seventh INT NULL,\
ss_ind_eighth INT NULL,\
time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\
PRIMARY KEY (result_id),\
UNIQUE INDEX result_id_UNIQUE (result_id ASC),\
INDEX ss_ind_first_idx (ss_ind_first ASC),\
INDEX ss_ind_second_idx (ss_ind_second ASC),\
INDEX ss_ind_third_idx (ss_ind_third ASC),\
INDEX ss_ind_fourth_idx (ss_ind_fourth ASC),\
INDEX ss_ind_fifth_idx (ss_ind_fifth ASC),\
INDEX ss_ind_sixth_idx (ss_ind_sixth ASC),\
INDEX ss_ind_seventh_idx (ss_ind_seventh ASC),\
INDEX ss_ind_eighth_idx (ss_ind_eighth ASC),\
CONSTRAINT ss_ind_first \
FOREIGN KEY (ss_ind_first) \
REFERENCES player (player_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT ss_ind_second \
FOREIGN KEY (ss_ind_second) \
REFERENCES player (player_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT ss_ind_third \
FOREIGN KEY (ss_ind_third) \
REFERENCES player (player_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT ss_ind_fourth \
FOREIGN KEY (ss_ind_fourth) \
REFERENCES player (player_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION, \
CONSTRAINT ss_ind_fifth \
FOREIGN KEY (ss_ind_fifth) \
REFERENCES player (player_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT ss_ind_sixth \
FOREIGN KEY (ss_ind_sixth) \
REFERENCES player (player_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT ss_ind_seventh \
FOREIGN KEY (ss_ind_seventh) \
REFERENCES player (player_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT ss_ind_eighth \
FOREIGN KEY (ss_ind_eighth) \
REFERENCES player (player_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION)")

        cursor.execute("CREATE TABLE IF NOT EXISTS ss_team_result (\
result_id INT NOT NULL AUTO_INCREMENT,\
ss_team_first INT NOT NULL,\
ss_team_second INT NOT NULL,\
ss_team_third INT NULL,\
ss_team_fourth INT NULL,\
time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\
PRIMARY KEY (result_id),\
UNIQUE INDEX result_id_UNIQUE (result_id ASC),\
INDEX ss_team_first_idx (ss_team_first ASC),\
INDEX ss_team_second_idx (ss_team_second ASC),\
INDEX ss_team_third_idx (ss_team_third ASC),\
INDEX ss_team_fourth_idx (ss_team_fourth ASC),\
CONSTRAINT ss_team_first \
FOREIGN KEY (ss_team_first) \
REFERENCES team (team_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT ss_team_second \
FOREIGN KEY (ss_team_second) \
REFERENCES team (team_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT ss_team_third \
FOREIGN KEY (ss_team_third) \
REFERENCES team (team_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT ss_team_fourth \
FOREIGN KEY (ss_team_fourth) \
REFERENCES team (team_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION)")

        cursor.execute("CREATE TABLE IF NOT EXISTS pp_result (\
result_id INT NOT NULL AUTO_INCREMENT,\
pp_winner INT NOT NULL,\
pp_loser INT NOT NULL,\
time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\
PRIMARY KEY (result_id),\
UNIQUE INDEX result_id_UNIQUE (result_id ASC),\
INDEX pp_winner_idx (pp_winner ASC),\
INDEX pp_loser_idx (pp_loser ASC),\
CONSTRAINT pp_winner \
FOREIGN KEY (pp_winner) \
REFERENCES player (player_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT pp_loser \
FOREIGN KEY (pp_loser) \
REFERENCES player (player_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION)")

        cursor.execute("CREATE TABLE IF NOT EXISTS fb_offense_rating_hist (\
rating INT NOT NULL,\
player INT NOT NULL,\
time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\
INDEX rating_idx (rating ASC),\
INDEX player_idx (player ASC),\
CONSTRAINT fb_offense_rating_hist \
FOREIGN KEY (rating) \
REFERENCES rating (rating_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT fb_offense_player_hist \
FOREIGN KEY (player) \
REFERENCES player (player_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION)")

        cursor.execute("CREATE TABLE IF NOT EXISTS fb_defense_rating_hist (\
rating INT NOT NULL,\
player INT NOT NULL,\
time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\
INDEX rating_idx (rating ASC),\
INDEX player_idx (player ASC),\
CONSTRAINT fb_defense_rating_hist \
FOREIGN KEY (rating) \
REFERENCES rating (rating_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT fb_defense_player_hist \
FOREIGN KEY (player) \
REFERENCES player (player_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION)")

        cursor.execute("CREATE TABLE IF NOT EXISTS fb_team_rating_hist (\
rating INT NOT NULL,\
team INT NOT NULL,\
time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\
INDEX rating_idx (rating ASC),\
INDEX team_idx (team ASC),\
CONSTRAINT fb_team_rating_hist \
FOREIGN KEY (rating) \
REFERENCES rating (rating_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT fb_team_player_hist \
FOREIGN KEY (team) \
REFERENCES team (team_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION)")

        cursor.execute("CREATE TABLE IF NOT EXISTS mk_team_rating_hist (\
rating INT NOT NULL,\
team INT NOT NULL,\
time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\
INDEX rating_idx (rating ASC),\
INDEX team_idx (team ASC),\
CONSTRAINT mk_team_rating_hist \
FOREIGN KEY (rating) \
REFERENCES rating (rating_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT mk_team_player_hist \
FOREIGN KEY (team) \
REFERENCES team (team_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION)")

        cursor.execute("CREATE TABLE IF NOT EXISTS mk_ind_rating_hist (\
rating INT NOT NULL,\
player INT NOT NULL,\
time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\
INDEX rating_idx (rating ASC),\
INDEX player_idx (player ASC),\
CONSTRAINT mk_ind_rating_hist \
FOREIGN KEY (rating) \
REFERENCES rating (rating_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT mk_ind_player_hist \
FOREIGN KEY (player) \
REFERENCES player (player_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION)")

        cursor.execute("CREATE TABLE IF NOT EXISTS mp_team_rating_hist (\
rating INT NOT NULL,\
team INT NOT NULL,\
time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\
INDEX rating_idx (rating ASC),\
INDEX team_idx (team ASC),\
CONSTRAINT mp_team_rating_hist \
FOREIGN KEY (rating) \
REFERENCES rating (rating_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT mp_team_player_hist \
FOREIGN KEY (team) \
REFERENCES team (team_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION)")

        cursor.execute("CREATE TABLE IF NOT EXISTS mp_ind_rating_hist (\
rating INT NOT NULL,\
player INT NOT NULL,\
time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\
INDEX rating_idx (rating ASC),\
INDEX player_idx (player ASC),\
CONSTRAINT mp_ind_rating_hist \
FOREIGN KEY (rating) \
REFERENCES rating (rating_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT mp_ind_player_hist \
FOREIGN KEY (player) \
REFERENCES player (player_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION)")

        cursor.execute("CREATE TABLE IF NOT EXISTS ss_team_rating_hist (\
rating INT NOT NULL,\
team INT NOT NULL,\
time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\
INDEX rating_idx (rating ASC),\
INDEX team_idx (team ASC),\
CONSTRAINT ss_team_rating_hist \
FOREIGN KEY (rating) \
REFERENCES rating (rating_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT ss_team_player_hist \
FOREIGN KEY (team) \
REFERENCES team (team_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION)")

        cursor.execute("CREATE TABLE IF NOT EXISTS ss_ind_rating_hist (\
rating INT NOT NULL,\
player INT NOT NULL,\
time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\
INDEX rating_idx (rating ASC),\
INDEX player_idx (player ASC),\
CONSTRAINT ss_ind_rating_hist \
FOREIGN KEY (rating) \
REFERENCES rating (rating_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT ss_ind_player_hist \
FOREIGN KEY (player) \
REFERENCES player (player_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION)")

        cursor.execute("CREATE TABLE IF NOT EXISTS pp_ind_rating_hist (\
rating INT NOT NULL,\
player INT NOT NULL,\
time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\
INDEX rating_idx (rating ASC),\
INDEX player_idx (player ASC),\
CONSTRAINT pp_ind_rating_hist \
FOREIGN KEY (rating) \
REFERENCES rating (rating_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION,\
CONSTRAINT pp_ind_player_hist \
FOREIGN KEY (player) \
REFERENCES player (player_id) \
ON DELETE NO ACTION \
ON UPDATE NO ACTION)")

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
