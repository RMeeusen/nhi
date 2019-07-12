#  -*- coding: utf-8 -*-
#   Copyright notice
#     --------------------------------------------------------------------
#     Copyright (C) 2019 Deltares
#         Rob Rikken
#         Rob.Rikken@deltares.nl
#   #
#     This library is free software: you can redistribute it and/or modify
#     it under the terms of the GNU General Public License as published by
#     the Free Software Foundation, either version 3 of the License, or
#     (at your option) any later version.
#   #
#     This library is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU General Public License for more details.
#   #
#     You should have received a copy of the GNU General Public License
#     along with this library.  If not, see <http://www.gnu.org/licenses/>.
#     --------------------------------------------------------------------
#   #
#   This tool is part of <a href="http://www.OpenEarth.eu">OpenEarthTools</a>.
#   OpenEarthTools is an online collaboration to share and manage data and
#   programming tools in an open source, version controlled environment.
#   Sign up to receive regular updates of this function, and to contribute
#   your own tools.
#   #
#
# This is the class that holds the configuration to make a connection to the NHI database.


class DatabaseConfig(object):
    __host = 'localhost:5432'
    __database = 'nhidatabase'
    __username = 'postgres'
    __password = 'postgres'

    @staticmethod
    def get_host():
        return DatabaseConfig.__host

    @staticmethod
    def get_database():
        return DatabaseConfig.__database

    @staticmethod
    def get_username():
        return DatabaseConfig.__username

    @staticmethod
    def get_password():
        return DatabaseConfig.__password
