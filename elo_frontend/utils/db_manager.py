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
import traceback
import ConfigParser
import time
import MySQLdb
import pkg_resources
import appdirs
import trueskill

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
course VARCHAR(75) NOT NULL,\
time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\
PRIMARY KEY (result_id),\
UNIQUE INDEX result_id_UNIQUE (result_id ASC),\
UNIQUE INDEX course_UNIQUE (course ASC),\
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

    def add_player(self, first_name, last_name, nickname):
        """Example method description.

        Args:
            first_name (str):   players first name
            last_name (str):    players last name
            nickname (str):     player's nickname

        Raises:
            DBValueError:       invalid db entry
            DBConnectionError:  database connection issues
            DBSyntaxError:      invalid database programming statement

        Returns:
            self.attr1

        """

        if len(first_name) is 0:
            self._logger.error("First name must be at least one character")
            raise exceptions.DBValueError("First name must be at least one character")

        if len(last_name) is 0:
            self._logger.error("Last name must be at least one character")
            raise exceptions.DBValueError("Last name must be at least one character")

        try:
            if not self.check_if_player_exists(first_name=first_name, last_name=last_name,
                                               nickname=nickname):
                raise exceptions.DBValueError("Name already exists in database")

            fb_offense_rating_id = self.create_new_default_rating()
            fb_defense_rating_id = self.create_new_default_rating()
            mk_ind_rating_id = self.create_new_default_rating()
            mp_ind_rating_id = self.create_new_default_rating()
            ss_ind_rating_id = self.create_new_default_rating()
            pp_ind_rating_id = self.create_new_default_rating()

            self.check_if_db_connected()
            cursor = self._db_conn.cursor()
            self._logger.info("Adding new player to database")
            cursor.execute("INSERT INTO player (first_name, last_name, \
nickname, fb_offense_rating, fb_defense_rating, mk_ind_rating, mp_ind_rating, \
ss_ind_rating, pp_ind_rating) VALUES ('{0}', '{1}', '{2}', \
{3}, {4}, {5}, {6}, {7}, {8})".format(first_name, last_name, nickname, fb_offense_rating_id,
                                      fb_defense_rating_id, mk_ind_rating_id, mp_ind_rating_id,
                                      ss_ind_rating_id, pp_ind_rating_id))
            player_id = cursor.lastrowid
            cursor.execute("INSERT INTO pp_ind_rating_hist (rating, player) VALUES ({0}, {1}\
)".format(pp_ind_rating_id, player_id))
            cursor.execute("INSERT INTO ss_ind_rating_hist (rating, player) VALUES ({0}, {1}\
)".format(ss_ind_rating_id, player_id))
            cursor.execute("INSERT INTO mp_ind_rating_hist (rating, player) VALUES ({0}, {1}\
)".format(mp_ind_rating_id, player_id))
            cursor.execute("INSERT INTO mk_ind_rating_hist (rating, player) VALUES ({0}, {1}\
)".format(mk_ind_rating_id, player_id))
            cursor.execute("INSERT INTO fb_defense_rating_hist (rating, player) VALUES ({0}, {1}\
)".format(fb_defense_rating_id, player_id))
            cursor.execute("INSERT INTO fb_offense_rating_hist (rating, player) VALUES ({0}, {1}\
)".format(fb_offense_rating_id, player_id))
            self._db_conn.commit()

        except MySQLdb.OperationalError:
            self._logger.error("MySQL operational error occured")
            traceback.print_exc()
            raise exceptions.DBConnectionError("Cannot connect to MySQL server")

        except MySQLdb.ProgrammingError:
            self._logger.error("MySQL programming error")
            traceback.print_exc()
            raise exceptions.DBSyntaxError("MySQL syntax error")

        else:
            pass

    def check_if_team_exists(self, team_name):
        """Method to check if team currently exists in database

        Args:
            team_name (str):    team name

        Returns:
            True/False if team exists

        Raises:
            DBConnectionError:  database connection issues
            DBSyntaxError:      invalid database programming statement

        """

        try:
            self._logger.debug("Checking if team already exists")
            self.check_if_db_connected()
            cursor = self._db_conn.cursor()
            cursor.execute("SELECT team_name FROM team")
            teams = cursor.fetchall()

            for existing_team in teams:
                if team_name == existing_team[0]:
                    return True

        except MySQLdb.OperationalError:
            self._logger.error("MySQL operational error occured")
            traceback.print_exc()
            raise exceptions.DBConnectionError("Cannot connect to MySQL server")

        except MySQLdb.ProgrammingError:
            self._logger.error("MySQL programming error")
            traceback.print_exc()
            raise exceptions.DBSyntaxError("MySQL syntax error")

        else:
            return False

    def check_if_two_players_on_team(self, member_one, member_two):
        """Method to check if two players are already on a team

        Args:
            player_one (tup):    player one
            player_two (tup):    player two

        Returns:
            Team if exists

        Raises:
            DBConnectionError:  database connection issues
            DBSyntaxError:      invalid database programming statement

        """

        try:
            self._logger.debug("Checking if players are already on team together")
            self.check_if_db_connected()
            cursor = self._db_conn.cursor()

            cursor.execute("SELECT player_id FROM player WHERE \
first_name = '{0}' AND last_name = '{1}' AND nickname = '{2}'".format(
                member_one[0], member_one[1], member_one[2]))
            player_one_id = cursor.fetchone()[0]

            cursor.execute("SELECT player_id FROM player WHERE \
first_name = '{0}' AND last_name = '{1}' AND nickname = '{2}'".format(
                member_two[0], member_two[1], member_two[2]))
            player_two_id = cursor.fetchone()[0]

            cursor.execute("SELECT team FROM player_team_xref WHERE \
player = {0}".format(player_one_id))
            teams = cursor.fetchall()

            #TODO will give false positive if members are on a team of three
            for team in teams:
                cursor.execute("SELECT player FROM player_team_xref WHERE \
team = {0}".format(team[0]))
                players = cursor.fetchall()
                for player in players:
                    if player[0] == player_two_id:
                        return team[0]

        except MySQLdb.OperationalError:
            self._logger.error("MySQL operational error occured")
            traceback.print_exc()
            raise exceptions.DBConnectionError("Cannot connect to MySQL server")

        except MySQLdb.ProgrammingError:
            self._logger.error("MySQL programming error")
            traceback.print_exc()
            raise exceptions.DBSyntaxError("MySQL syntax error")

        else:
            return False

    def check_if_player_exists(self, first_name, last_name, nickname):
        """Method to check if player currently exists in database

        Args:
            first_name (str):   player first name
            last_name(str):     player last name
            nickname (str):     player nickname

        Returns:
            True/False if player exists

        Raises:
            DBConnectionError:  database connection issues
            DBSyntaxError:      invalid database programming statement

        """

        try:
            self._logger.debug("Checking if player already exists")
            self.check_if_db_connected()
            cursor = self._db_conn.cursor()
            cursor.execute("SELECT first_name, last_name, nickname FROM player")
            players = cursor.fetchall()

            for existing_first_name, existing_last_name, existing_nickname in players:
                if (first_name == existing_first_name) and (last_name ==
                    existing_last_name) and (nickname == existing_nickname):
                    return False

        except MySQLdb.OperationalError:
            self._logger.error("MySQL operational error occured")
            traceback.print_exc()
            raise exceptions.DBConnectionError("Cannot connect to MySQL server")

        except MySQLdb.ProgrammingError:
            self._logger.error("MySQL programming error")
            traceback.print_exc()
            raise exceptions.DBSyntaxError("MySQL syntax error")

        else:
            return True

    def check_if_db_connected(self):
        """Method to check if still connected to database"""

        self._logger.debug("Checking if database is still connected")
        try:
            cursor = self._db_conn.cursor()
            cursor.execute("SELECT * FROM player")

        except MySQLdb.OperationalError:
            self._logger.info("Database connection dropped, reconnecting...")
            traceback.print_exc()
            self._db_conn = MySQLdb.connect(user=self._db_user, passwd=self._db_pass,
                                            host=self._db_host, db=self._db_name)

        else:
            pass

    def create_new_default_rating(self):
        """Creates a new rating at the default level

        Returns:
            rating_id for rating just created

        Raises:
            DBConnectionError:  database connection issues
            DBSyntaxError:      invalid database programming statement

        """

        try:
            self._logger.debug("Creating new default rating")
            new_rating = trueskill.Rating()
            self.check_if_db_connected()
            cursor = self._db_conn.cursor()
            cursor.execute("INSERT INTO rating (mu, sigma) VALUES ({0}, {1})".format(
                new_rating.mu, new_rating.sigma))
            rating_id = cursor.lastrowid

        except MySQLdb.OperationalError:
            self._logger.error("MySQL operational error occured")
            traceback.print_exc()
            raise exceptions.DBConnectionError("Cannot connect to MySQL server")

        except MySQLdb.ProgrammingError:
            self._logger.error("MySQL programming error")
            traceback.print_exc()
            raise exceptions.DBSyntaxError("MySQL syntax error")

        else:
            return rating_id

    def get_all_players(self):
        """Method to get all players from database

        Returns:
            tuple of player tuples

        Raises:
            DBConnectionError:  database connection issues
            DBSyntaxError:      invalid database programming statement

        """

        self._logger.debug("Getting player list")

        try:
            self.check_if_db_connected()
            cursor = self._db_conn.cursor()
            cursor.execute("SELECT first_name, last_name, nickname, time FROM player \
    ORDER BY time DESC")
            players = cursor.fetchall()

        except MySQLdb.OperationalError:
            self._logger.error("MySQL operational error occured")
            traceback.print_exc()
            raise exceptions.DBConnectionError("Cannot connect to MySQL server")

        except MySQLdb.ProgrammingError:
            self._logger.error("MySQL programming error")
            traceback.print_exc()
            raise exceptions.DBSyntaxError("MySQL syntax error")

        else:
            return players

    def get_total_players(self):
        """Method to get player count from database

        Returns:
            count of players

        Raises:
            DBConnectionError:  database connection issues
            DBSyntaxError:      invalid database programming statement

        """

        self._logger.debug("Getting total player count")
        try:
            self.check_if_db_connected()
            cursor = self._db_conn.cursor()
            cursor.execute("SELECT COUNT(player_id) FROM player")
            count = cursor.fetchone()[0]

        except MySQLdb.OperationalError:
            self._logger.error("MySQL operational error occured")
            traceback.print_exc()
            raise exceptions.DBConnectionError("Cannot connect to MySQL server")

        except MySQLdb.ProgrammingError:
            self._logger.error("MySQL programming error")
            traceback.print_exc()
            raise exceptions.DBSyntaxError("MySQL syntax error")

        else:
            return count

    def add_ppresult(self, winner, loser):
        """Method to add a ping pong result to database

        Args:
            winner(tup):    match winner
            loser (tup):    match loser

        Raises:
            DBValueError:       invalid db entry
            DBConnectionError:  database connection issues
            DBSyntaxError:      invalid database programming statement

        """

        if len(winner) != 3:
            raise exceptions.DBValueError("Winner must be complete")

        if len(winner) != 3:
            raise exceptions.DBValueError("Winner must be complete")

        if winner == loser:
            raise exceptions.DBValueError("Winner and loser cannot be same person")

        self._logger.debug("Adding pp result to database")
        try:
            self.check_if_db_connected()
            cursor = self._db_conn.cursor()
            cursor.execute("INSERT INTO pp_result (pp_winner, pp_loser) VALUES ((SELECT \
player_id FROM player WHERE first_name = '{0}' AND last_name \
= '{1}' AND nickname = '{2}'), (SELECT player_id FROM player WHERE first_name \
= '{3}' AND last_name = '{4}' AND nickname = '{5}'))".format(
    winner[0], winner[1], winner[2], loser[0],
    loser[1], loser[2]))

            self._logger.debug("Updating pp ratings")
            cursor.execute("SELECT player_id, pp_ind_rating FROM player \
WHERE first_name = '{0}' AND last_name = '{1}' AND nickname = '{2}'".format(
                winner[0], winner[1], winner[2]))
            winner_player_id, rating = cursor.fetchall()[0]

            cursor.execute("SELECT mu, sigma FROM rating WHERE rating_id \
= {0}".format(rating))
            mu, sigma = cursor.fetchall()[0]
            winner_rating = trueskill.Rating(mu=float(mu),
                                             sigma=float(sigma))

            cursor.execute("SELECT player_id, pp_ind_rating FROM player \
WHERE first_name = '{0}' AND last_name = '{1}' AND nickname = '{2}'".format(
                loser[0], loser[1], loser[2]))
            loser_player_id, rating = cursor.fetchall()[0]

            cursor.execute("SELECT mu, sigma FROM rating WHERE rating_id \
= {0}".format(rating))
            mu, sigma = cursor.fetchall()[0]
            loser_rating = trueskill.Rating(mu=float(mu),
                                            sigma=float(sigma))

            new_winner_rating, new_loser_rating = \
            trueskill.rate_1vs1(winner_rating, loser_rating)

            cursor.execute("INSERT INTO rating (mu, sigma) VALUES ({0}, {1}\
)".format(new_winner_rating.mu, new_winner_rating.sigma))
            new_rating_id = cursor.lastrowid
            cursor.execute("UPDATE player set pp_ind_rating = {0} where \
player_id = {1}".format(new_rating_id, winner_player_id))
            cursor.execute("INSERT INTO pp_ind_rating_hist (rating, player) VALUES ({0}, {1}\
)".format(new_rating_id, winner_player_id))

            cursor.execute("INSERT INTO rating (mu, sigma) VALUES ({0}, {1}\
)".format(new_loser_rating.mu, new_loser_rating.sigma))
            new_rating_id = cursor.lastrowid
            cursor.execute("UPDATE player set pp_ind_rating = {0} where \
player_id = {1}".format(new_rating_id, loser_player_id))
            cursor.execute("INSERT INTO pp_ind_rating_hist (rating, player) VALUES ({0}, {1}\
)".format(new_rating_id, loser_player_id))

            self._db_conn.commit()

        except MySQLdb.OperationalError:
            self._logger.error("MySQL operational error occured")
            traceback.print_exc()
            raise exceptions.DBConnectionError("Cannot connect to MySQL server")

        except MySQLdb.ProgrammingError:
            self._logger.error("MySQL programming error")
            traceback.print_exc()
            raise exceptions.DBSyntaxError("MySQL syntax error")

        else:
            pass

    def add_fbresult(self, offense_winner, defense_winner, offense_loser,
        defense_loser):
        """Method to add a foosball result to database

        Args:
            offense_winner(tup):    offense_winner
            defense_winner (tup):   defense_winner
            offense_loser (tup):    offense_loser
            defense_loser (tup):    defense_loser

        Raises:
            DBValueError:       invalid db entry
            DBConnectionError:  database connection issues
            DBSyntaxError:      invalid database programming statement

        """

        if len(offense_winner) != 3:
            raise exceptions.DBValueError("Offense winner must\
 be complete")

        if len(defense_winner) != 3:
            raise exceptions.DBValueError("Defense winner must\
 be complete")

        if len(offense_loser) != 3:
            raise exceptions.DBValueError("Offense loser must\
 be complete")

        if len(defense_loser) != 3:
            raise exceptions.DBValueError("Defense loser must\
 be complete")

        if offense_winner == defense_winner or offense_winner == offense_loser or offense_winner == defense_loser:
            raise exceptions.DBValueError("Duplicate players in result")

        if defense_winner == offense_loser or defense_winner == defense_loser:
            raise exceptions.DBValueError("Duplicate players in result")

        if offense_loser == defense_loser:
            raise exceptions.DBValueError("Duplicate players in result")

        self._logger.debug("Adding fb result to database")
        try:
            self.check_if_db_connected()
            cursor = self._db_conn.cursor()
            cursor.execute("INSERT INTO fb_result (offense_winner, \
defense_winner, offense_loser, defense_loser) VALUES ((SELECT \
player_id FROM player WHERE first_name = '{0}' AND last_name \
= '{1}' AND nickname = '{2}'), (SELECT player_id FROM player WHERE first_name \
= '{3}' AND last_name = '{4}' AND nickname = '{5}'), (SELECT player_id FROM \
player WHERE first_name = '{6}' AND last_name = '{7}' AND nickname = '{8}'), \
(SELECT player_id FROM player WHERE first_name = '{9}' AND last_name = '{10}' \
AND nickname = '{11}'))".format(offense_winner[0],
                offense_winner[1], offense_winner[2], defense_winner[0],
                defense_winner[1], defense_winner[2], offense_loser[0],
                offense_loser[1], offense_loser[2], defense_loser[0],
                defense_loser[1], defense_loser[2]))

            self._logger.debug("Updating fb ratings")
            cursor.execute("SELECT player_id, fb_offense_rating FROM player \
WHERE first_name = '{0}' AND last_name = '{1}' AND nickname = '{2}'".format(
                offense_winner[0], offense_winner[1], offense_winner[2]))
            offense_winner_player_id, rating = cursor.fetchall()[0]

            cursor.execute("SELECT mu, sigma FROM rating WHERE rating_id \
= {0}".format(rating))
            mu, sigma = cursor.fetchall()[0]
            offense_winner_rating = trueskill.Rating(mu=float(mu),
                sigma=float(sigma))

            cursor.execute("SELECT player_id, fb_defense_rating FROM player \
WHERE first_name = '{0}' AND last_name = '{1}' AND nickname = '{2}'".format(
                defense_winner[0], defense_winner[1], defense_winner[2]))
            defense_winner_player_id, rating = cursor.fetchall()[0]

            cursor.execute("SELECT mu, sigma FROM rating WHERE rating_id \
= {0}".format(rating))
            mu, sigma = cursor.fetchall()[0]
            defense_winner_rating = trueskill.Rating(mu=float(mu),
                sigma=float(sigma))

            cursor.execute("SELECT player_id, fb_offense_rating FROM player \
WHERE first_name = '{0}' AND last_name = '{1}' AND nickname = '{2}'".format(
                offense_loser[0], offense_loser[1], offense_loser[2]))
            offense_loser_player_id, rating = cursor.fetchall()[0]

            cursor.execute("SELECT mu, sigma FROM rating WHERE rating_id \
= {0}".format(rating))
            mu, sigma = cursor.fetchall()[0]
            offense_loser_rating = trueskill.Rating(mu=float(mu),
                sigma=float(sigma))

            cursor.execute("SELECT player_id, fb_defense_rating FROM player \
WHERE first_name = '{0}' AND last_name = '{1}' AND nickname = '{2}'".format(
                defense_loser[0], defense_loser[1], defense_loser[2]))
            defense_loser_player_id, rating = cursor.fetchall()[0]

            cursor.execute("SELECT mu, sigma FROM rating WHERE rating_id \
= {0}".format(rating))
            mu, sigma = cursor.fetchall()[0]
            defense_loser_rating = trueskill.Rating(mu=float(mu),
                sigma=float(sigma))

            (new_offense_winner_rating, new_defense_winner_rating), \
            (new_offense_loser_rating, new_defense_loser_rating) = \
            trueskill.rate([(offense_winner_rating, defense_winner_rating),
                (offense_loser_rating, defense_loser_rating)], ranks=[0, 1])

            cursor.execute("INSERT INTO rating (mu, sigma) VALUES ({0}, {1}\
)".format(new_offense_winner_rating.mu, new_offense_winner_rating.sigma))
            new_rating_id = cursor.lastrowid
            cursor.execute("UPDATE player set fb_offense_rating = {0} where \
player_id = {1}".format(new_rating_id, offense_winner_player_id))
            cursor.execute("INSERT INTO fb_offense_rating_hist (rating, player) VALUES ({0}, {1}\
)".format(new_rating_id, offense_winner_player_id))

            cursor.execute("INSERT INTO rating (mu, sigma) VALUES ({0}, {1}\
)".format(new_defense_winner_rating.mu, new_defense_winner_rating.sigma))
            new_rating_id = cursor.lastrowid
            cursor.execute("UPDATE player set fb_defense_rating = {0} where \
player_id = {1}".format(new_rating_id, defense_winner_player_id))
            cursor.execute("INSERT INTO fb_defense_rating_hist (rating, player) VALUES ({0}, {1}\
)".format(new_rating_id, defense_winner_player_id))

            cursor.execute("INSERT INTO rating (mu, sigma) VALUES ({0}, {1}\
)".format(new_offense_loser_rating.mu, new_offense_loser_rating.sigma))
            new_rating_id = cursor.lastrowid
            cursor.execute("UPDATE player set fb_offense_rating = {0} where \
player_id = {1}".format(new_rating_id, offense_loser_player_id))
            cursor.execute("INSERT INTO fb_offense_rating_hist (rating, player) VALUES ({0}, {1}\
)".format(new_rating_id, offense_loser_player_id))

            cursor.execute("INSERT INTO rating (mu, sigma) VALUES ({0}, {1}\
)".format(new_defense_loser_rating.mu, new_defense_loser_rating.sigma))
            new_rating_id = cursor.lastrowid
            cursor.execute("UPDATE player set fb_defense_rating = {0} where \
player_id = {1}".format(new_rating_id, defense_loser_player_id))
            cursor.execute("INSERT INTO fb_defense_rating_hist (rating, player) VALUES ({0}, {1}\
)".format(new_rating_id, defense_loser_player_id))

            self._logger.debug("Update foosball team ratings")
            # check if winners are on a team together
            winning_team = self.check_if_two_players_on_team(
                offense_winner, defense_winner)

            if not winning_team:
                # create a new team
                winning_team = self.add_fb_team(team_name="{0} & {1}\
".format(offense_winner[0], defense_winner[0]), member_one=offense_winner,
                    member_two=defense_winner)

            # check if losers are on a team together
            losing_team = self.check_if_two_players_on_team(
                offense_loser, defense_loser)

            if not losing_team:
                # create a new team
                losing_team = self.add_fb_team(team_name="{0} & {1}\
".format(offense_loser[0], defense_loser[0]), member_one=offense_loser,
                    member_two=defense_loser)
                # avoid timestamp issues in database
                time.sleep(1)

            # get ratings
            cursor.execute("SELECT fb_team_rating from team WHERE team_id \
= {0}".format(winning_team))
            winning_team_rating_id = cursor.fetchone()[0]
            
            cursor.execute("SELECT fb_team_rating from team WHERE team_id \
= {0}".format(losing_team))
            losing_team_rating_id = cursor.fetchone()[0]

            cursor.execute("SELECT mu, sigma FROM rating WHERE rating_id \
= {0}".format(winning_team_rating_id))
            mu, sigma = cursor.fetchall()[0]
            winning_team_rating = trueskill.Rating(mu=float(mu),
                sigma=float(sigma))

            cursor.execute("SELECT mu, sigma FROM rating WHERE rating_id \
= {0}".format(losing_team_rating_id))
            mu, sigma = cursor.fetchall()[0]
            losing_team_rating = trueskill.Rating(mu=float(mu),
                sigma=float(sigma))

            new_winning_team_rating, new_losing_team_rating = \
            trueskill.rate_1vs1(winning_team_rating, losing_team_rating)

            cursor.execute("INSERT INTO rating (mu, sigma) VALUES ({0}, {1}\
)".format(new_winning_team_rating.mu, new_winning_team_rating.sigma))
            new_rating_id = cursor.lastrowid
            cursor.execute("UPDATE team SET fb_team_rating = {0} where \
team_id = {1}".format(new_rating_id, winning_team))
            cursor.execute("INSERT INTO fb_team_rating_hist (rating, team) VALUES ({0}, {1}\
)".format(new_rating_id, winning_team))

            cursor.execute("INSERT INTO rating (mu, sigma) VALUES ({0}, {1}\
)".format(new_losing_team_rating.mu, new_losing_team_rating.sigma))
            new_rating_id = cursor.lastrowid
            cursor.execute("UPDATE team SET fb_team_rating = {0} where \
team_id = {1}".format(new_rating_id, losing_team))
            cursor.execute("INSERT INTO fb_team_rating_hist (rating, team) VALUES ({0}, {1}\
)".format(new_rating_id, losing_team))
            self._db_conn.commit()

        except MySQLdb.OperationalError:
            self._logger.error("MySQL operational error occured")
            traceback.print_exc()
            raise exceptions.DBConnectionError("Cannot connect to MySQL server")

        except MySQLdb.ProgrammingError:
            self._logger.error("MySQL programming error")
            traceback.print_exc()
            raise exceptions.DBSyntaxError("MySQL syntax error")

        else:
            pass

    def add_mkresult(self, first, second, third,
        fourth, course):
        """Method to add a mk result to database

        Args:
            first (tup):    first place
            second (tup):   second place
            third (tup):    third place
            fourth (tup):   fourth place
            course (str):   course

        Raises:
            DBValueError:       invalid db entry
            DBConnectionError:  database connection issues
            DBSyntaxError:      invalid database programming statement

        """

        if len(first) != 3:
            raise exceptions.DBValueError("1st must\
 be complete")

        if len(second) != 3:
            raise exceptions.DBValueError("2nd must\
 be complete")

        if not third:
            if fourth:
                raise exceptions.DBValueError("Invalid result")

        if third:
            if fourth:
                if third == fourth:
                    raise exceptions.DBValueError("Duplicate players in result")

        if first == second or first == third or first == fourth:
            raise exceptions.DBValueError("Duplicate players in result")

        if second == third or second == fourth:
            raise exceptions.DBValueError("Duplicate players in result")

        self._logger.debug("Adding mk result to database")
        try:
            self.check_if_db_connected()
            cursor = self._db_conn.cursor()

            if not fourth:
                if not third:
                    cursor.execute("INSERT INTO mk_ind_result (mk_ind_first, \
            mk_ind_second, mk_ind_third, mk_ind_fourth, course) VALUES ((SELECT \
            player_id FROM player WHERE first_name = '{0}' AND last_name \
            = '{1}' AND nickname = '{2}'), (SELECT player_id FROM player WHERE first_name \
            = '{3}' AND last_name = '{4}' AND nickname = '{5}'), NULL, \
            NULL, '{6}')".format(first[0],
                            first[1], first[2], second[0],
                            second[1], second[2], course))
                else:
                    cursor.execute("INSERT INTO mk_ind_result (mk_ind_first, \
        mk_ind_second, mk_ind_third, mk_ind_fourth, course) VALUES ((SELECT \
        player_id FROM player WHERE first_name = '{0}' AND last_name \
        = '{1}' AND nickname = '{2}'), (SELECT player_id FROM player WHERE first_name \
        = '{3}' AND last_name = '{4}' AND nickname = '{5}'), (SELECT player_id FROM \
        player WHERE first_name = '{6}' AND last_name = '{7}' AND nickname = '{8}'), \
        NULL, '{9}')".format(first[0],
                        first[1], first[2], second[0],
                        second[1], second[2], third[0],
                        third[1], third[2], course))
            else:
                cursor.execute("INSERT INTO mk_ind_result (mk_ind_first, \
    mk_ind_second, mk_ind_third, mk_ind_fourth, course) VALUES ((SELECT \
    player_id FROM player WHERE first_name = '{0}' AND last_name \
    = '{1}' AND nickname = '{2}'), (SELECT player_id FROM player WHERE first_name \
    = '{3}' AND last_name = '{4}' AND nickname = '{5}'), (SELECT player_id FROM \
    player WHERE first_name = '{6}' AND last_name = '{7}' AND nickname = '{8}'), \
    (SELECT player_id FROM player WHERE first_name = '{9}' AND last_name = '{10}' \
    AND nickname = '{11}'), '{12}')".format(first[0],
                    first[1], first[2], second[0],
                    second[1], second[2], third[0],
                    third[1], third[2], fourth[0],
                    fourth[1], fourth[2], course))

            self._logger.debug("Updating mk ratings")
            cursor.execute("SELECT player_id, mk_ind_rating FROM player \
WHERE first_name = '{0}' AND last_name = '{1}' AND nickname = '{2}'".format(
                first[0], first[1], first[2]))
            first_player_id, rating = cursor.fetchall()[0]

            cursor.execute("SELECT mu, sigma FROM rating WHERE rating_id \
= {0}".format(rating))
            mu, sigma = cursor.fetchall()[0]
            first_rating = trueskill.Rating(mu=float(mu),
                sigma=float(sigma))

            cursor.execute("SELECT player_id, mk_ind_rating FROM player \
WHERE first_name = '{0}' AND last_name = '{1}' AND nickname = '{2}'".format(
                second[0], second[1], second[2]))
            second_player_id, rating = cursor.fetchall()[0]

            cursor.execute("SELECT mu, sigma FROM rating WHERE rating_id \
= {0}".format(rating))
            mu, sigma = cursor.fetchall()[0]
            second_rating = trueskill.Rating(mu=float(mu),
                sigma=float(sigma))

            if third:
                cursor.execute("SELECT player_id, mk_ind_rating FROM player \
WHERE first_name = '{0}' AND last_name = '{1}' AND nickname = '{2}'".format(
                    third[0], third[1], third[2]))
                third_player_id, rating = cursor.fetchall()[0]

                cursor.execute("SELECT mu, sigma FROM rating WHERE rating_id \
= {0}".format(rating))
                mu, sigma = cursor.fetchall()[0]
                third_rating = trueskill.Rating(mu=float(mu),
                    sigma=float(sigma))

            if fourth:
                cursor.execute("SELECT player_id, mk_ind_rating FROM player \
WHERE first_name = '{0}' AND last_name = '{1}' AND nickname = '{2}'".format(
                    fourth[0], fourth[1], fourth[2]))
                fourth_player_id, rating = cursor.fetchall()[0]

                cursor.execute("SELECT mu, sigma FROM rating WHERE rating_id \
= {0}".format(rating))
                mu, sigma = cursor.fetchall()[0]
                fourth_rating = trueskill.Rating(mu=float(mu),
                    sigma=float(sigma))

            if not fourth:
                if not third:
                    (new_first_rating,), (new_second_rating,) = \
                    trueskill.rate([(first_rating,), (second_rating,)], ranks=[0, 1])
                else:
                    (new_first_rating,), (new_second_rating,), \
                    (new_third_rating,) = \
                    trueskill.rate([(first_rating,), (second_rating,),
                        (third_rating,)], ranks=[0, 1, 2])
            else:
                (new_first_rating,), (new_second_rating,), \
                (new_third_rating,), (new_fourth_rating,) = \
                trueskill.rate([(first_rating,), (second_rating,),
                    (third_rating,), (fourth_rating,)], ranks=[0, 1, 2, 3])

            cursor.execute("INSERT INTO rating (mu, sigma) VALUES ({0}, {1}\
)".format(new_first_rating.mu, new_first_rating.sigma))
            new_rating_id = cursor.lastrowid
            cursor.execute("UPDATE player set mk_ind_rating = {0} where \
player_id = {1}".format(new_rating_id, first_player_id))
            cursor.execute("INSERT INTO mk_ind_rating_hist (rating, player) VALUES ({0}, {1}\
)".format(new_rating_id, first_player_id))

            cursor.execute("INSERT INTO rating (mu, sigma) VALUES ({0}, {1}\
)".format(new_second_rating.mu, new_second_rating.sigma))
            new_rating_id = cursor.lastrowid
            cursor.execute("UPDATE player set mk_ind_rating = {0} where \
player_id = {1}".format(new_rating_id, second_player_id))
            cursor.execute("INSERT INTO mk_ind_rating_hist (rating, player) VALUES ({0}, {1}\
)".format(new_rating_id, second_player_id))

            if third:
                cursor.execute("INSERT INTO rating (mu, sigma) VALUES ({0}, {1}\
)".format(new_third_rating.mu, new_third_rating.sigma))
                new_rating_id = cursor.lastrowid
                cursor.execute("UPDATE player set mk_ind_rating = {0} where \
player_id = {1}".format(new_rating_id, third_player_id))
                cursor.execute("INSERT INTO mk_ind_rating_hist (rating, player) VALUES ({0}, {1}\
)".format(new_rating_id, third_player_id))

            if fourth:
                cursor.execute("INSERT INTO rating (mu, sigma) VALUES ({0}, {1}\
)".format(new_fourth_rating.mu, new_fourth_rating.sigma))
                new_rating_id = cursor.lastrowid
                cursor.execute("UPDATE player set mk_ind_rating = {0} where \
player_id = {1}".format(new_rating_id, fourth_player_id))
                cursor.execute("INSERT INTO mk_ind_rating_hist (rating, player) VALUES ({0}, {1}\
)".format(new_rating_id, fourth_player_id))

            self._db_conn.commit()

        except MySQLdb.OperationalError:
            self._logger.error("MySQL operational error occured")
            traceback.print_exc()
            raise exceptions.DBConnectionError("Cannot connect to MySQL server")

        except MySQLdb.ProgrammingError:
            self._logger.error("MySQL programming error")
            traceback.print_exc()
            raise exceptions.DBSyntaxError("MySQL syntax error")

        else:
            pass

    def delete_last_ppresult(self,):
        """Method to delete last ping pong result from database

        Raises:
            DBValueError:       invalid db entry
            DBConnectionError:  database connection issues
            DBSyntaxError:      invalid database programming statement

        """

        self._logger.debug("Deleting last pp result from database")
        try:
            self.check_if_db_connected()
            cursor = self._db_conn.cursor()
            cursor.execute("SELECT result_id, pp_winner, pp_loser FROM pp_result ORDER BY time \
DESC LIMIT 1")
            results = cursor.fetchall()
            result_id = results[0][0]
            winner_player_id = results[0][1]
            loser_player_id = results[0][2]

            # revert from pp_ind_rating_hist
            cursor.execute("SELECT rating FROM pp_ind_rating_hist WHERE player = {0} ORDER BY time DESC LIMIT 2".format(winner_player_id))
            results = cursor.fetchall()
            winner_new_rating_id = results[0][0]
            winner_previous_rating_id = results[1][0]
            cursor.execute("SELECT rating FROM pp_ind_rating_hist WHERE player = {0} ORDER BY time DESC LIMIT 2".format(loser_player_id))
            results = cursor.fetchall()
            loser_new_rating_id = results[0][0]
            loser_previous_rating_id = results[1][0]
            # update player ratings in player
            cursor.execute("UPDATE player SET pp_ind_rating = {0} WHERE \
player_id = {1}".format(loser_previous_rating_id, loser_player_id))
            cursor.execute("UPDATE player SET pp_ind_rating = {0} WHERE \
player_id = {1}".format(winner_previous_rating_id, winner_player_id))
            cursor.execute("DELETE FROM pp_ind_rating_hist WHERE rating = {0}".format(winner_new_rating_id))
            cursor.execute("DELETE FROM pp_ind_rating_hist WHERE rating = {0}".format(loser_new_rating_id))
            # delete result from pp_result
            cursor.execute("DELETE FROM pp_result WHERE result_id = {0}".format(result_id))
            self._db_conn.commit()

        except MySQLdb.OperationalError:
            self._logger.error("MySQL operational error occured")
            traceback.print_exc()
            raise exceptions.DBConnectionError("Cannot connect to MySQL server")

        except MySQLdb.ProgrammingError:
            self._logger.error("MySQL programming error")
            traceback.print_exc()
            raise exceptions.DBSyntaxError("MySQL syntax error")

        else:
            pass

    def delete_last_fbresult(self,):
        """Method to delete last foosball result from database

        Raises:
            DBValueError:       invalid db entry
            DBConnectionError:  database connection issues
            DBSyntaxError:      invalid database programming statement

        """

        self._logger.debug("Deleting last fb result from database")
        try:
            self.check_if_db_connected()
            cursor = self._db_conn.cursor()
            cursor.execute("SELECT result_id, offense_winner, defense_winner, offense_loser, \
defense_loser FROM fb_result ORDER BY time \
DESC LIMIT 1")
            results = cursor.fetchall()
            result_id = results[0][0]
            offense_winner_player_id = results[0][1]
            defense_winner_player_id = results[0][2]
            offense_loser_player_id = results[0][3]
            defense_loser_player_id = results[0][4]

            # revert from fb_ind_rating_hist
            cursor.execute("SELECT rating FROM fb_offense_rating_hist WHERE player = {0} ORDER \
BY time DESC LIMIT 2".format(offense_winner_player_id))
            results = cursor.fetchall()
            offense_winner_new_rating_id = results[0][0]
            offense_winner_previous_rating_id = results[1][0]
            cursor.execute("SELECT rating FROM fb_defense_rating_hist WHERE player = {0} ORDER \
BY time DESC LIMIT 2".format(defense_winner_player_id))
            results = cursor.fetchall()
            defense_winner_new_rating_id = results[0][0]
            defense_winner_previous_rating_id = results[1][0]
            cursor.execute("SELECT rating FROM fb_offense_rating_hist WHERE player = {0} ORDER \
BY time DESC LIMIT 2".format(offense_loser_player_id))
            results = cursor.fetchall()
            offense_loser_new_rating_id = results[0][0]
            offense_loser_previous_rating_id = results[1][0]
            cursor.execute("SELECT rating FROM fb_defense_rating_hist WHERE player = {0} ORDER \
BY time DESC LIMIT 2".format(defense_loser_player_id))
            results = cursor.fetchall()
            defense_loser_new_rating_id = results[0][0]
            defense_loser_previous_rating_id = results[1][0]
            # update player ratings in player
            cursor.execute("UPDATE player SET fb_offense_rating = {0} WHERE \
player_id = {1}".format(offense_winner_previous_rating_id, offense_winner_player_id))
            cursor.execute("UPDATE player SET fb_defense_rating = {0} WHERE \
player_id = {1}".format(defense_winner_previous_rating_id, defense_winner_player_id))
            cursor.execute("UPDATE player SET fb_offense_rating = {0} WHERE \
player_id = {1}".format(offense_loser_previous_rating_id, offense_loser_player_id))
            cursor.execute("UPDATE player SET fb_defense_rating = {0} WHERE \
player_id = {1}".format(defense_loser_previous_rating_id, defense_loser_player_id))
            cursor.execute("DELETE FROM fb_offense_rating_hist WHERE rating = {0}".format(offense_winner_new_rating_id))
            cursor.execute("DELETE FROM fb_defense_rating_hist WHERE rating = {0}".format(defense_winner_new_rating_id))
            cursor.execute("DELETE FROM fb_offense_rating_hist WHERE rating = {0}".format(offense_loser_new_rating_id))
            cursor.execute("DELETE FROM fb_defense_rating_hist WHERE rating = {0}".format(defense_loser_new_rating_id))

            # get team ids
            cursor.execute("SELECT first_name, last_name, nickname FROM player WHERE player_id \
= {0}".format(offense_winner_player_id))
            results = cursor.fetchall()[0]
            offense_winner_first_name = results[0]
            offense_winner_last_name = results[1]
            offense_winner_nickname = results[2]
            cursor.execute("SELECT first_name, last_name, nickname FROM player WHERE player_id \
= {0}".format(defense_winner_player_id))
            results = cursor.fetchall()[0]
            defense_winner_first_name = results[0]
            defense_winner_last_name = results[1]
            defense_winner_nickname = results[2]
            winner_team_id = self.check_if_two_players_on_team(
                (offense_winner_first_name, offense_winner_last_name, offense_winner_nickname),
                (defense_winner_first_name, defense_winner_last_name, defense_winner_nickname))
            cursor.execute("SELECT first_name, last_name, nickname FROM player WHERE player_id \
= {0}".format(offense_loser_player_id))
            results = cursor.fetchall()[0]
            offense_loser_first_name = results[0]
            offense_loser_last_name = results[1]
            offense_loser_nickname = results[2]
            cursor.execute("SELECT first_name, last_name, nickname FROM player WHERE player_id \
= {0}".format(defense_loser_player_id))
            results = cursor.fetchall()[0]
            defense_loser_first_name = results[0]
            defense_loser_last_name = results[1]
            defense_loser_nickname = results[2]
            loser_team_id = self.check_if_two_players_on_team(
                (offense_loser_first_name, offense_loser_last_name, offense_loser_nickname),
                (defense_loser_first_name, defense_loser_last_name, defense_loser_nickname))
            #revert from fb_team_rating_hist
            cursor.execute("SELECT rating FROM fb_team_rating_hist WHERE team = {0} ORDER \
BY time DESC LIMIT 2".format(winner_team_id))
            results = cursor.fetchall()
            winner_new_rating_id = results[0][0]
            winner_previous_rating_id = results[1][0]
            cursor.execute("SELECT rating FROM fb_team_rating_hist WHERE team = {0} ORDER \
BY time DESC LIMIT 2".format(loser_team_id))
            results = cursor.fetchall()
            loser_new_rating_id = results[0][0]
            loser_previous_rating_id = results[1][0]

            # update team ratings in team
            cursor.execute("UPDATE team SET fb_team_rating = {0} WHERE \
team_id = {1}".format(winner_previous_rating_id, winner_team_id))
            cursor.execute("UPDATE team SET fb_team_rating = {0} WHERE \
team_id = {1}".format(loser_previous_rating_id, loser_team_id))
            cursor.execute("DELETE FROM fb_team_rating_hist WHERE rating = {0}".format(winner_new_rating_id))
            cursor.execute("DELETE FROM fb_team_rating_hist WHERE rating = {0}".format(loser_new_rating_id))

            # delete result from fb_result
            cursor.execute("DELETE FROM fb_result WHERE result_id = {0}".format(result_id))
            self._db_conn.commit()

        except MySQLdb.OperationalError:
            self._logger.error("MySQL operational error occured")
            traceback.print_exc()
            raise exceptions.DBConnectionError("Cannot connect to MySQL server")

        except MySQLdb.ProgrammingError:
            self._logger.error("MySQL programming error")
            traceback.print_exc()
            raise exceptions.DBSyntaxError("MySQL syntax error")

        else:
            pass

    def delete_last_mkresult(self,):
        """Method to delete last mk result from database

        Raises:
            DBValueError:       invalid db entry
            DBConnectionError:  database connection issues
            DBSyntaxError:      invalid database programming statement

        """

        self._logger.debug("Deleting last mk result from database")
        try:
            self.check_if_db_connected()
            cursor = self._db_conn.cursor()
            cursor.execute("SELECT result_id, mk_ind_first, mk_ind_second, mk_ind_third, \
mk_ind_fourth FROM mk_ind_result ORDER BY time \
DESC LIMIT 1")
            results = cursor.fetchall()
            result_id = results[0][0]
            first_player_id = results[0][1]
            second_player_id = results[0][2]
            third_player_id = results[0][3]
            fourth_player_id = results[0][4]

            # revert from mk_ind_rating_hist
            cursor.execute("SELECT rating FROM mk_ind_rating_hist WHERE player = {0} ORDER \
BY time DESC LIMIT 2".format(first_player_id))
            results = cursor.fetchall()
            first_new_rating_id = results[0][0]
            first_previous_rating_id = results[1][0]
            cursor.execute("SELECT rating FROM mk_ind_rating_hist WHERE player = {0} ORDER \
BY time DESC LIMIT 2".format(second_player_id))
            results = cursor.fetchall()
            second_new_rating_id = results[0][0]
            second_previous_rating_id = results[1][0]
            if third_player_id:
                cursor.execute("SELECT rating FROM mk_ind_rating_hist WHERE player = {0} ORDER \
BY time DESC LIMIT 2".format(third_player_id))
                results = cursor.fetchall()
                third_new_rating_id = results[0][0]
                third_previous_rating_id = results[1][0]
            if fourth_player_id:
                cursor.execute("SELECT rating FROM mk_ind_rating_hist WHERE player = {0} ORDER \
BY time DESC LIMIT 2".format(fourth_player_id))
                results = cursor.fetchall()
                fourth_new_rating_id = results[0][0]
                fourth_previous_rating_id = results[1][0]

            # update player ratings in player
            cursor.execute("UPDATE player SET mk_ind_rating = {0} WHERE \
player_id = {1}".format(first_previous_rating_id, first_player_id))
            cursor.execute("DELETE FROM mk_ind_rating_hist WHERE rating = {0}".format(first_new_rating_id))
            cursor.execute("UPDATE player SET mk_ind_rating = {0} WHERE \
player_id = {1}".format(second_previous_rating_id, second_player_id))
            cursor.execute("DELETE FROM mk_ind_rating_hist WHERE rating = {0}".format(second_new_rating_id))
            if third_player_id:
                cursor.execute("UPDATE player SET mk_ind_rating = {0} WHERE \
player_id = {1}".format(third_previous_rating_id, third_player_id))
                cursor.execute("DELETE FROM mk_ind_rating_hist WHERE rating = {0}".format(third_new_rating_id))
            if fourth_player_id:
                cursor.execute("UPDATE player SET mk_ind_rating = {0} WHERE \
player_id = {1}".format(fourth_previous_rating_id, fourth_player_id))
                cursor.execute("DELETE FROM mk_ind_rating_hist WHERE rating = {0}".format(fourth_new_rating_id))

            # delete result from mk_result
            cursor.execute("DELETE FROM mk_ind_result WHERE result_id = {0}".format(result_id))
            self._db_conn.commit()

        except MySQLdb.OperationalError:
            self._logger.error("MySQL operational error occured")
            traceback.print_exc()
            raise exceptions.DBConnectionError("Cannot connect to MySQL server")

        except MySQLdb.ProgrammingError:
            self._logger.error("MySQL programming error")
            traceback.print_exc()
            raise exceptions.DBSyntaxError("MySQL syntax error")

        else:
            pass

    def get_all_ppresults(self):
        """Method to get all pp results from database

        Returns:
            all pp results

        Raises:
            DBConnectionError:  database connection issues
            DBSyntaxError:      invalid database programming statement

        """

        all_results = ()
        self._logger.debug("Getting all ping pong results")

        try:
            self.check_if_db_connected()
            cursor = self._db_conn.cursor()
            cursor.execute("SELECT result_id, pp_winner, pp_loser, time FROM pp_result ORDER BY time DESC")
            results = cursor.fetchall()

            for result_id, winner_id, loser_id, timestamp in results:
                intermediate_results = ()

                cursor.execute("SELECT first_name, last_name, nickname FROM \
player WHERE player_id = {0}".format(winner_id))
                winner = cursor.fetchall()
                first_name_winner, last_name_winner, \
                    nickname_winner = winner[0]

                cursor.execute("SELECT first_name, last_name, nickname FROM \
player WHERE player_id = {0}".format(loser_id))
                loser = cursor.fetchall()
                first_name_loser, last_name_loser, \
                    nickname_loser = loser[0]

                intermediate_results = intermediate_results + \
                    (result_id, first_name_winner, last_name_winner,
                     nickname_winner, first_name_loser,
                     last_name_loser, nickname_loser,
                     timestamp.strftime('%Y-%m-%d'))

                all_results = all_results + (intermediate_results,)
                del intermediate_results

        except MySQLdb.OperationalError:
            self._logger.error("MySQL operational error occured")
            traceback.print_exc()
            raise exceptions.DBConnectionError("Cannot connect to MySQL server")

        except MySQLdb.ProgrammingError:
            self._logger.error("MySQL programming error")
            traceback.print_exc()
            raise exceptions.DBSyntaxError("MySQL syntax error")

        else:
            return all_results

    def get_all_fbresults(self):
        """Method to get all fb results from database

        Returns:
            all fb results

        Raises:
            DBConnectionError:  database connection issues
            DBSyntaxError:      invalid database programming statement

        """

        all_results = ()
        self._logger.debug("Getting all foosball results")

        try:
            self.check_if_db_connected()
            cursor = self._db_conn.cursor()
            cursor.execute("SELECT result_id, offense_winner, defense_winner, offense_loser, \
defense_loser, time FROM fb_result ORDER BY time DESC")
            results = cursor.fetchall()

            for result_id, offense_winner_id, defense_winner_id, offense_loser_id, defense_loser_id, timestamp in results:
                intermediate_results = ()

                cursor.execute("SELECT first_name, last_name, nickname FROM \
player WHERE player_id = {0}".format(offense_winner_id))
                offense_winner = cursor.fetchall()
                first_name_offense_winner, last_name_offense_winner, \
                    nickname_offense_winner = offense_winner[0]
                cursor.execute("SELECT first_name, last_name, nickname FROM \
player WHERE player_id = {0}".format(defense_winner_id))
                defense_winner = cursor.fetchall()
                first_name_defense_winner, last_name_defense_winner, \
                    nickname_defense_winner = defense_winner[0]
                cursor.execute("SELECT first_name, last_name, nickname FROM \
player WHERE player_id = {0}".format(offense_loser_id))
                offense_loser = cursor.fetchall()
                first_name_offense_loser, last_name_offense_loser, \
                    nickname_offense_loser = offense_loser[0]
                cursor.execute("SELECT first_name, last_name, nickname FROM \
player WHERE player_id = {0}".format(defense_loser_id))
                defense_loser = cursor.fetchall()
                first_name_defense_loser, last_name_defense_loser, \
                    nickname_defense_loser = defense_loser[0]

                intermediate_results = intermediate_results + \
                    (result_id, first_name_offense_winner, last_name_offense_winner,
                     nickname_offense_winner, first_name_defense_winner, last_name_defense_winner,
                     nickname_defense_winner, first_name_offense_loser,
                     last_name_offense_loser, nickname_offense_loser, first_name_defense_loser,
                     last_name_defense_loser, nickname_defense_loser,
                     timestamp.strftime('%Y-%m-%d'))

                all_results = all_results + (intermediate_results,)
                del intermediate_results

        except MySQLdb.OperationalError:
            self._logger.error("MySQL operational error occured")
            traceback.print_exc()
            raise exceptions.DBConnectionError("Cannot connect to MySQL server")

        except MySQLdb.ProgrammingError:
            self._logger.error("MySQL programming error")
            traceback.print_exc()
            raise exceptions.DBSyntaxError("MySQL syntax error")

        else:
            return all_results

    def get_all_mkresults(self):
        """Method to get all mk results from database

        Returns:
            all mk results

        Raises:
            DBConnectionError:  database connection issues
            DBSyntaxError:      invalid database programming statement

        """

        all_results = ()
        self._logger.debug("Getting all mk results")

        try:
            self.check_if_db_connected()
            cursor = self._db_conn.cursor()
            cursor.execute("SELECT result_id, mk_ind_first, mk_ind_second, mk_ind_third, \
mk_ind_fourth, course, time FROM mk_ind_result ORDER BY time DESC")
            results = cursor.fetchall()

            for result_id, first_id, second_id, third_id, fourth_id, course, timestamp in results:
                intermediate_results = ()

                cursor.execute("SELECT first_name, last_name, nickname FROM \
player WHERE player_id = {0}".format(first_id))
                first = cursor.fetchall()
                first_name_first, last_name_first, \
                    nickname_first = first[0]
                cursor.execute("SELECT first_name, last_name, nickname FROM \
player WHERE player_id = {0}".format(second_id))
                second = cursor.fetchall()
                first_name_second, last_name_second, \
                    nickname_second = second[0]
                try:
                    cursor.execute("SELECT first_name, last_name, nickname FROM \
    player WHERE player_id = {0}".format(third_id))
                    third = cursor.fetchall()
                    first_name_third, last_name_third, \
                        nickname_third = third[0]
                except MySQLdb.OperationalError:
                    first_name_third = ''
                    last_name_third = ''
                    nickname_third = ''
                try:
                    cursor.execute("SELECT first_name, last_name, nickname FROM \
    player WHERE player_id = {0}".format(fourth_id))
                    fourth = cursor.fetchall()
                    first_name_fourth, last_name_fourth, \
                        nickname_fourth = fourth[0]
                except MySQLdb.OperationalError:
                    first_name_fourth = ''
                    last_name_fourth = ''
                    nickname_fourth = ''

                intermediate_results = intermediate_results + \
                    (result_id, first_name_first, last_name_first,
                     nickname_first, first_name_second, last_name_second,
                     nickname_second, first_name_third,
                     last_name_third, nickname_third, first_name_fourth,
                     last_name_fourth, nickname_fourth, course,
                     timestamp.strftime('%Y-%m-%d'))

                all_results = all_results + (intermediate_results,)
                del intermediate_results

        except MySQLdb.OperationalError:
            self._logger.error("MySQL operational error occured")
            traceback.print_exc()
            raise exceptions.DBConnectionError("Cannot connect to MySQL server")

        except MySQLdb.ProgrammingError:
            self._logger.error("MySQL programming error")
            traceback.print_exc()
            raise exceptions.DBSyntaxError("MySQL syntax error")

        else:
            return all_results

    def get_total_ppresults(self):
        """Method to get pp result count from database

        Returns:
            total number of results

        Raises:
            DBConnectionError:  database connection issues
            DBSyntaxError:      invalid database programming statement

        """

        self._logger.debug("Getting ping pong results count")

        try:
            self.check_if_db_connected()
            cursor = self._db_conn.cursor()
            cursor.execute("SELECT COUNT(result_id) FROM pp_result")
            count = cursor.fetchone()[0]

        except MySQLdb.OperationalError:
            self._logger.error("MySQL operational error occured")
            traceback.print_exc()
            raise exceptions.DBConnectionError("Cannot connect to MySQL server")

        except MySQLdb.ProgrammingError:
            self._logger.error("MySQL programming error")
            traceback.print_exc()
            raise exceptions.DBSyntaxError("MySQL syntax error")

        else:
            return count

    def get_total_fbresults(self):
        """Method to get fb result count from database

        Returns:
            total number of results

        Raises:
            DBConnectionError:  database connection issues
            DBSyntaxError:      invalid database programming statement

        """

        self._logger.debug("Getting foosball results count")

        try:
            self.check_if_db_connected()
            cursor = self._db_conn.cursor()
            cursor.execute("SELECT COUNT(result_id) FROM fb_result")
            count = cursor.fetchone()[0]

        except MySQLdb.OperationalError:
            self._logger.error("MySQL operational error occured")
            traceback.print_exc()
            raise exceptions.DBConnectionError("Cannot connect to MySQL server")

        except MySQLdb.ProgrammingError:
            self._logger.error("MySQL programming error")
            traceback.print_exc()
            raise exceptions.DBSyntaxError("MySQL syntax error")

        else:
            return count

    def get_total_mkresults(self):
        """Method to get mk result count from database

        Returns:
            total number of results

        Raises:
            DBConnectionError:  database connection issues
            DBSyntaxError:      invalid database programming statement

        """

        self._logger.debug("Getting mk results count")

        try:
            self.check_if_db_connected()
            cursor = self._db_conn.cursor()
            cursor.execute("SELECT COUNT(result_id) FROM mk_ind_result")
            count = cursor.fetchone()[0]

        except MySQLdb.OperationalError:
            self._logger.error("MySQL operational error occured")
            traceback.print_exc()
            raise exceptions.DBConnectionError("Cannot connect to MySQL server")

        except MySQLdb.ProgrammingError:
            self._logger.error("MySQL programming error")
            traceback.print_exc()
            raise exceptions.DBSyntaxError("MySQL syntax error")

        else:
            return count

    def get_pp_ind_rankings(self):
        """Method to get ping pong individual rankings from database

        Returns:
            individual rank list

        Raises:
            DBConnectionError:  database connection issues
            DBSyntaxError:      invalid database programming statement

        """

        ranks = []
        self._logger.debug("Getting ping pong individual rankings")

        try:
            self.check_if_db_connected()
            cursor = self._db_conn.cursor()
            cursor.execute("SELECT player_id, first_name, last_name, \
nickname FROM player")
            players = cursor.fetchall()

            for player_id, first_name, last_name, nickname in players:
                cursor.execute("SELECT pp_ind_rating FROM \
player WHERE player_id = {0}".format(player_id))
                ind_rating = cursor.fetchall()[0][0]
                cursor.execute("SELECT mu, sigma FROM rating WHERE rating_id \
= {0}".format(ind_rating))
                mu, sigma = cursor.fetchall()[0]

                ind_rank = float(mu) - (3 * float(sigma))

                cursor.execute("SELECT COUNT(result_id) FROM pp_result WHERE \
pp_winner = {0}".format(player_id))
                win_count = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(result_id) FROM pp_result WHERE \
pp_loser = {0}".format(player_id))
                loss_count = cursor.fetchone()[0]

                intermediate_rank = (first_name, last_name, nickname, round(ind_rank, 4),
                                     win_count, loss_count)
                ranks.append(intermediate_rank)
                del intermediate_rank

        except MySQLdb.OperationalError:
            self._logger.error("MySQL operational error occured")
            traceback.print_exc()
            raise exceptions.DBConnectionError("Cannot connect to MySQL server")

        except MySQLdb.ProgrammingError:
            self._logger.error("MySQL programming error")
            traceback.print_exc()
            raise exceptions.DBSyntaxError("MySQL syntax error")

        else:
            return ranks

    def get_fb_ind_rankings(self):
        """Method to get foosball individual rankings from database

        Returns:
            individual rank list

        Raises:
            DBConnectionError:  database connection issues
            DBSyntaxError:      invalid database programming statement

        """

        ranks = []
        self._logger.debug("Getting foosball individual rankings")

        try:
            self.check_if_db_connected()
            cursor = self._db_conn.cursor()
            cursor.execute("SELECT player_id, first_name, last_name, \
nickname FROM player")
            players = cursor.fetchall()

            for player_id, first_name, last_name, nickname in players:
                cursor.execute("SELECT fb_offense_rating, fb_defense_rating FROM \
player WHERE player_id = {0}".format(player_id))
                offense_rating, defense_rating = cursor.fetchall()[0]

                cursor.execute("SELECT mu, sigma FROM rating WHERE rating_id \
= {0}".format(offense_rating))
                mu, sigma = cursor.fetchall()[0]

                offense_rank = float(mu) - (3 * float(sigma))
                cursor.execute("SELECT mu, sigma FROM rating WHERE rating_id \
= {0}".format(defense_rating))
                mu, sigma = cursor.fetchall()[0]

                defense_rank = float(mu) - (3 * float(sigma))

                cursor.execute("SELECT COUNT(result_id) FROM fb_result WHERE \
offense_winner = {0}".format(player_id))
                offense_win_count = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(result_id) FROM fb_result WHERE \
defense_winner = {0}".format(player_id))
                defense_win_count = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(result_id) FROM fb_result WHERE \
offense_loser = {0}".format(player_id))
                offense_lose_count = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(result_id) FROM fb_result WHERE \
defense_loser = {0}".format(player_id))
                defense_lose_count = cursor.fetchone()[0]

                intermediate_rank = (first_name, last_name, nickname,
                    'Offense', round(offense_rank, 4), offense_win_count,
                    offense_lose_count)
                ranks.append(intermediate_rank)
                del intermediate_rank
                intermediate_rank = (first_name, last_name, nickname,
                    'Defense', round(defense_rank, 4), defense_win_count,
                    defense_lose_count)
                ranks.append(intermediate_rank)
                del intermediate_rank

        except MySQLdb.OperationalError:
            self._logger.error("MySQL operational error occured")
            traceback.print_exc()
            raise exceptions.DBConnectionError("Cannot connect to MySQL server")

        except MySQLdb.ProgrammingError:
            self._logger.error("MySQL programming error")
            traceback.print_exc()
            raise exceptions.DBSyntaxError("MySQL syntax error")

        else:
            return ranks

    def get_mk_ind_rankings(self):
        """Method to get mk individual rankings from database

        Returns:
            individual rank list

        Raises:
            DBConnectionError:  database connection issues
            DBSyntaxError:      invalid database programming statement

        """

        ranks = []
        self._logger.debug("Getting mk individual rankings")

        try:
            self.check_if_db_connected()
            cursor = self._db_conn.cursor()
            cursor.execute("SELECT player_id, first_name, last_name, \
nickname FROM player")
            players = cursor.fetchall()

            for player_id, first_name, last_name, nickname in players:
                cursor.execute("SELECT mk_ind_rating FROM \
player WHERE player_id = {0}".format(player_id))
                ind_rating = cursor.fetchall()[0][0]
                cursor.execute("SELECT mu, sigma FROM rating WHERE rating_id \
= {0}".format(ind_rating))
                mu, sigma = cursor.fetchall()[0]

                ind_rank = float(mu) - (3 * float(sigma))

                cursor.execute("SELECT COUNT(result_id) FROM mk_ind_result WHERE \
mk_ind_first = {0}".format(player_id))
                first_count = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(result_id) FROM mk_ind_result WHERE \
mk_ind_second = {0}".format(player_id))
                second_count = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(result_id) FROM mk_ind_result WHERE \
mk_ind_third = {0}".format(player_id))
                third_count = cursor.fetchone()[0]

                intermediate_rank = (first_name, last_name, nickname, round(ind_rank, 4),
                                     first_count, second_count, third_count)
                ranks.append(intermediate_rank)
                del intermediate_rank

        except MySQLdb.OperationalError:
            self._logger.error("MySQL operational error occured")
            traceback.print_exc()
            raise exceptions.DBConnectionError("Cannot connect to MySQL server")

        except MySQLdb.ProgrammingError:
            self._logger.error("MySQL programming error")
            traceback.print_exc()
            raise exceptions.DBSyntaxError("MySQL syntax error")

        else:
            return ranks

    def get_fb_team_rankings(self):
        """Method to get fb team rankings from database

        Returns:
            team rank list

        Raises:
            DBConnectionError:  database connection issues
            DBSyntaxError:      invalid database programming statement

        """

        ranks = []
        self._logger.debug("Getting foosball team rankings")

        try:
            self.check_if_db_connected()
            cursor = self._db_conn.cursor()
            cursor.execute("SELECT team_id, team_name FROM team")
            teams = cursor.fetchall()

            for team_id, team_name in teams:
                cursor.execute("SELECT fb_team_rating FROM \
team WHERE team_id = {0}".format(team_id))
                team_rating = cursor.fetchall()[0]

                cursor.execute("SELECT mu, sigma FROM rating WHERE rating_id \
= {0}".format(team_rating[0]))
                mu, sigma = cursor.fetchall()[0]

                team_rank = float(mu) - (3 * float(sigma))

                # get player_ids
                cursor.execute("SELECT player from player_team_xref \
WHERE team = {0}".format(team_id))
                players = cursor.fetchall()
                player_one = players[0]
                player_two = players[1]

                cursor.execute("SELECT first_name FROM player WHERE \
player_id = {0}".format(player_one[0]))
                player_one_name = cursor.fetchone()[0]

                cursor.execute("SELECT first_name FROM player WHERE \
player_id = {0}".format(player_two[0]))
                player_two_name = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(result_id) FROM fb_result WHERE \
(offense_winner = {0} AND defense_winner = {1}) OR (offense_winner = {1} \
AND defense_winner = {0})".format(player_one[0], player_two[0]))
                team_win_count = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(result_id) FROM fb_result WHERE \
(offense_loser = {0} AND defense_loser = {1}) OR (offense_loser = {1} \
AND defense_loser = {0})".format(player_one[0], player_two[0]))
                team_loss_count = cursor.fetchone()[0]

                intermediate_rank = (team_name, round(team_rank, 4),
                    team_win_count, team_loss_count, player_one_name,
                    player_two_name)
                ranks.append(intermediate_rank)
                del intermediate_rank

        except MySQLdb.OperationalError:
            self._logger.error("MySQL operational error occured")
            traceback.print_exc()
            raise exceptions.DBConnectionError("Cannot connect to MySQL server")

        except MySQLdb.ProgrammingError:
            self._logger.error("MySQL programming error")
            traceback.print_exc()
            raise exceptions.DBSyntaxError("MySQL syntax error")

        else:
            return ranks

    def get_pp_ind_rankings_hist(self, player):
        """Method to get ping pong individual rankings history from database

        Args:
            player (str):   player

        Returns:
            history of ranks

        Raises:
            DBValueError:       invalid db entry
            DBConnectionError:  database connection issues
            DBSyntaxError:      invalid database programming statement

        """

        if len(player) != 3:
            raise exceptions.DBValueError("Player must be complete")

        rank_hist = []
        self._logger.debug("Getting ping pong individual ranking history for player")

        try:
            self.check_if_db_connected()
            cursor = self._db_conn.cursor()
            cursor.execute("SELECT player_id FROM player WHERE \
first_name = '{0}' AND last_name = '{1}' AND nickname = \
'{2}'".format(player[0], player[1], player[2]))
            player_id = cursor.fetchone()[0]
            cursor.execute("SELECT rating, time FROM pp_ind_rating_hist \
WHERE player = {0} ORDER BY time DESC".format(player_id))
            results = cursor.fetchall()

            for rating, timestamp in results:
                cursor.execute("SELECT mu, sigma FROM rating WHERE rating_id \
= {0}".format(rating))
                mu, sigma = cursor.fetchall()[0]
                rank = float(mu) - (3 * float(sigma))
                intermediate_rank = (round(rank, 4), timestamp)
                rank_hist.append(intermediate_rank)
                del intermediate_rank

        except MySQLdb.OperationalError:
            self._logger.error("MySQL operational error occured")
            traceback.print_exc()
            raise exceptions.DBConnectionError("Cannot connect to MySQL server")

        except MySQLdb.ProgrammingError:
            self._logger.error("MySQL programming error")
            traceback.print_exc()
            raise exceptions.DBSyntaxError("MySQL syntax error")

        else:
            return rank_hist

    def get_all_pp_ind_rankings_hist(self):
        """Method to get ping pong individual rankings history from database

        Returns:
            total history of ranks

        Raises:
            DBValueError:       invalid db entry
            DBConnectionError:  database connection issues
            DBSyntaxError:      invalid database programming statement

        """

        total_rank_hist = []
        self._logger.debug("Getting ping pong total individual ranking history")

        try:
            self.check_if_db_connected()
            cursor = self._db_conn.cursor()
            cursor.execute("SELECT first_name, last_name, \
nickname FROM player")
            players = cursor.fetchall()

            for first_name, last_name, nickname in players:
                intermediate_hist = self.get_pp_ind_rankings_hist((first_name, last_name, nickname))
                total_rank_hist.append((first_name, last_name, nickname), intermediate_hist)
                del intermediate_hist

        except MySQLdb.OperationalError:
            self._logger.error("MySQL operational error occured")
            traceback.print_exc()
            raise exceptions.DBConnectionError("Cannot connect to MySQL server")

        except MySQLdb.ProgrammingError:
            self._logger.error("MySQL programming error")
            traceback.print_exc()
            raise exceptions.DBSyntaxError("MySQL syntax error")

        else:
            return total_rank_hist

    def edit_player(self, previous_player, new_player):
        """Method to edit player in database

        Args:
            previous_player (dict): existing player in database
            new_player (dict):      replacement player for database

        Raises:
            DBValueError:       invalid db entry
            DBConnectionError:  database connection issues
            DBSyntaxError:      invalid database programming statement

        """

        if len(new_player['first_name']) is 0:
            raise exceptions.DBValueError("First name must be at \
least one character")

        if len(new_player['last_name']) is 0:
            raise exceptions.DBValueError("Last name must be at \
least one character")

        if not self.check_if_player_exists(**new_player):
            raise exceptions.DBValueError("Name you are trying to change to already \
exists in database")

        self._logger.debug("Editing player in database")
        try:
            self.check_if_db_connected()
            cursor = self._db_conn.cursor()
            sql_params = dict(previous_player.items() + new_player.items())
            sql = """UPDATE player
                     SET first_name='{first_name}',
                         last_name='{last_name}',
                         nickname='{nickname}'
                     WHERE first_name='{previous_first_name}' AND
                           last_name='{previous_last_name}' AND
                           nickname='{previous_nickname}';""".format(**sql_params)
            cursor.execute(sql)
            self._db_conn.commit()

        except MySQLdb.OperationalError:
            self._logger.error("MySQL operational error occured")
            traceback.print_exc()
            raise exceptions.DBConnectionError("Cannot connect to MySQL server")

        except MySQLdb.ProgrammingError:
            self._logger.error("MySQL programming error")
            traceback.print_exc()
            raise exceptions.DBSyntaxError("MySQL syntax error")

        else:
            pass

    def edit_team(self, previous_team, new_team):
        """Method to edit team name in database

        Args:
            previous_team (str):    existing team in database
            new_team (str):         replacement team for database

        Raises:
            DBValueError:       invalid db entry
            DBConnectionError:  database connection issues
            DBSyntaxError:      invalid database programming statement

        """

        if len(new_team) is 0:
            raise exceptions.DBValueError("Team name must be at \
least one character")

        if self.check_if_team_exists(new_team):
            raise exceptions.DBValueError("Name you are trying to change to already \
exists in database")

        self._logger.debug("Editing team in database")
        try:
            self.check_if_db_connected()
            cursor = self._db_conn.cursor()
            cursor.execute("SELECT team_id FROM team WHERE team_name = '{0}'".format(previous_team))
            team_id = cursor.fetchall()[0][0]
            cursor.execute("UPDATE team SET team_name = '{0}' WHERE team_id = {1}".format(new_team, team_id))
            self._db_conn.commit()

        except MySQLdb.OperationalError:
            self._logger.error("MySQL operational error occured")
            traceback.print_exc()
            raise exceptions.DBConnectionError("Cannot connect to MySQL server")

        except MySQLdb.ProgrammingError:
            self._logger.error("MySQL programming error")
            traceback.print_exc()
            raise exceptions.DBSyntaxError("MySQL syntax error")

        else:
            pass

    def get_all_fb_teams(self):
        """Method to get all fb teams from database

        Returns:
            tuple of team tuples

        Raises:
            DBConnectionError:  database connection issues
            DBSyntaxError:      invalid database programming statement

        """

        all_teams = ()
        self._logger.debug("Getting all fb teams from database")
        try:
            self.check_if_db_connected()
            cursor = self._db_conn.cursor()
            cursor.execute("SELECT team_id, team_name, time FROM team ORDER BY \
time DESC")
            teams = cursor.fetchall()

            for team_id, name, timestamp in teams:
                intermediate_teams = ()
                intermediate_teams = intermediate_teams + (name,)
                cursor.execute("SELECT player FROM player_team_xref WHERE \
team = {0}".format(team_id))
                players = cursor.fetchall()
                for player in players:
                    cursor.execute("SELECT first_name, last_name, nickname \
FROM player WHERE player_id = {0}".format(player[0]))
                    first_name, last_name, nickname = cursor.fetchall()[0]

                    intermediate_teams = intermediate_teams + (first_name,
                        last_name, nickname)

                intermediate_teams = intermediate_teams + (timestamp.strftime('%Y-%m-%d'),)
                all_teams = all_teams + (intermediate_teams,)
                del intermediate_teams

        except MySQLdb.OperationalError:
            self._logger.error("MySQL operational error occured")
            traceback.print_exc()
            raise exceptions.DBConnectionError("Cannot connect to MySQL server")

        except MySQLdb.ProgrammingError:
            self._logger.error("MySQL programming error")
            traceback.print_exc()
            raise exceptions.DBSyntaxError("MySQL syntax error")

        else:
            return all_teams

    def get_total_fb_teams(self):
        """Method to get total fb teams from database

        Returns:
            total number of teams

        Raises:
            DBConnectionError:  database connection issues
            DBSyntaxError:      invalid database programming statement

        """

        self._logger.debug("Getting total fb teams from database")
        try:
            self.check_if_db_connected()
            cursor = self._db_conn.cursor()
            cursor.execute("SELECT COUNT(team_id) FROM team")
            count = cursor.fetchone()[0]

        except MySQLdb.OperationalError:
            self._logger.error("MySQL operational error occured")
            traceback.print_exc()
            raise exceptions.DBConnectionError("Cannot connect to MySQL server")

        except MySQLdb.ProgrammingError:
            self._logger.error("MySQL programming error")
            traceback.print_exc()
            raise exceptions.DBSyntaxError("MySQL syntax error")

        else:
            return count

    def add_fb_team(self, team_name, member_one, member_two):
        """Method to add a fb team to database

        Args:
            team_name (str):    team name
            member_one (tup):   first member names
            member_two (tup):   second member names

        Raises:
            DBValueError:       invalid db entry
            DBConnectionError:  database connection issues
            DBSyntaxError:      invalid database programming statement

        """

        if len(team_name) is 0:
            raise exceptions.DBValueError("Team name must be at \
least one character")

        if len(member_one) != 3:
            raise exceptions.DBValueError("First team member must\
 be complete")

        if len(member_two) != 3:
            raise exceptions.DBValueError("Second team member must\
 be complete")

        if member_one == member_two:
            raise exceptions.DBValueError("Team members must be different players")

        if self.check_if_team_exists(team_name=team_name):
            raise exceptions.DBValueError("Team already exists")

        if self.check_if_two_players_on_team(member_one, member_two):
            raise exceptions.DBValueError("Players already on team together")

        self._logger.debug("Adding fb team to database")
        try:
            self.check_if_db_connected()
            cursor = self._db_conn.cursor()
            fb_rating_id = self.create_new_default_rating()
            mk_rating_id = self.create_new_default_rating()
            mp_rating_id = self.create_new_default_rating()
            ss_rating_id = self.create_new_default_rating()
            cursor.execute("INSERT INTO team (team_name, fb_team_rating, mk_team_rating, \
mp_team_rating, ss_team_rating) VALUES ('{0}', {1}, {2}, {3}, {4})".format(
    team_name, fb_rating_id, mk_rating_id, mp_rating_id, ss_rating_id))

            team_id = cursor.lastrowid

            cursor.execute("INSERT INTO fb_team_rating_hist (rating, team) VALUES ({0}, {1}\
)".format(fb_rating_id, team_id))
            cursor.execute("INSERT INTO mk_team_rating_hist (rating, team) VALUES ({0}, {1}\
)".format(mk_rating_id, team_id))
            cursor.execute("INSERT INTO mp_team_rating_hist (rating, team) VALUES ({0}, {1}\
)".format(mp_rating_id, team_id))
            cursor.execute("INSERT INTO ss_team_rating_hist (rating, team) VALUES ({0}, {1}\
)".format(ss_rating_id, team_id))

            cursor.execute("INSERT INTO player_team_xref (player, team) \
VALUES ((SELECT player_id FROM player WHERE first_name = '{0}' AND last_name \
= '{1}' AND nickname = '{2}'), {3})".format(member_one[0], member_one[1],
                member_one[2], team_id))

            cursor.execute("INSERT INTO player_team_xref (player, team) \
VALUES ((SELECT player_id FROM player WHERE first_name = '{0}' AND last_name \
= '{1}' AND nickname = '{2}'), {3})".format(member_two[0], member_two[1],
                member_two[2], team_id))

            self._db_conn.commit()

        except MySQLdb.OperationalError:
            self._logger.error("MySQL operational error occured")
            traceback.print_exc()
            raise exceptions.DBConnectionError("Cannot connect to MySQL server")

        except MySQLdb.ProgrammingError:
            self._logger.error("MySQL programming error")
            traceback.print_exc()
            raise exceptions.DBSyntaxError("MySQL syntax error")

        else:
            return team_id

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
