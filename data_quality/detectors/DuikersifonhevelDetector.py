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
from threading import Thread

from detectors.Detector import Detector


class DuikersifonhevelDetector(Detector):
    @staticmethod
    def get_table_name():
        return 'duikersifonhevel'

    @staticmethod
    def get_inlet_not_zero_suggestions():
        return {
            'code': 1401,
            'suggestion': 'Width and height of the inlet needs to be larger than zero.'
        }

    def check_inlet_not_zero(self):
        with self.get_datasource().get_connection() as connection:
            inlets_that_are_zero = connection.execute('''
                SELECT DISTINCT duikersifonhevel.*
                FROM duikersifonhevel
                WHERE hoogteopening <= 0 
                    OR breedteopening <= 0
            ''')

        self.insert_suggestions_records(inlets_that_are_zero, self.get_inlet_not_zero_suggestions())

    def build_threads(self):
        inlet_thread = Thread(target=self.check_inlet_not_zero)

        return [inlet_thread]
