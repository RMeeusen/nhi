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
from sqlalchemy import create_engine

from datasources.DatabaseConfig import DatabaseConfig


# Creates an engine from SqlAlchemy for the PostGIS database of the NHI
class NhiDatasource:
    def __init__(self):
        self.__database_engine = create_engine(''.join([
            'postgresql://',
            DatabaseConfig.get_username(),
            ':',
            DatabaseConfig.get_password(),
            '@',
            DatabaseConfig.get_host(),
            '/',
            DatabaseConfig.get_database(),
        ]),
            pool_size=80,
            max_overflow=10,
            pool_pre_ping=True
        )

    def get_connection(self, **kwargs):
        return self.__database_engine.connect(**kwargs)
