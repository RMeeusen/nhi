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
from threading import Thread
from typing import Dict, List

from detectors.Detector import Detector


# This class holds the functions to detect errors in the weirs.
class StuwDetector(Detector):
    __hydroobject_distance = None

    __has_hydroobject_suggestion = {
        'code': None,
        'suggestion': None
    }

    __flow_height_difference_suggestion = {
        'code': None,
        'suggestion': None
    }

    def __init__(self):
        super().__init__()
        self.set_table_name('stuw')
        self.set_hydroobject_distance(1)
        self.set_has_hydroobject_suggestion(
            2101,
            'The weir needs to lie within a distance of ' + str(self.get_hydroobject_distance()) + ' meters of a '
            'hydroobject.'
        )
        self.set_flow_height_difference_suggestion(
            2102,
            'The lowest flow height needs to be lower than the highest flow height.'
        )

    def get_has_hydroobject_suggestion(self) -> Dict:
        return self.__has_hydroobject_suggestion

    def set_has_hydroobject_suggestion(self, code: int, suggestion: str) -> None:
        self.__has_hydroobject_suggestion = {
            'code': code,
            'suggestion': suggestion
        }

    def get_flow_height_difference_suggestion(self) -> Dict:
        return self.__flow_height_difference_suggestion

    def set_flow_height_difference_suggestion(self, code: int, suggestion: str) -> None:
        self.__flow_height_difference_suggestion = {
            'code': code,
            'suggestion': suggestion
        }

    def get_hydroobject_distance(self) -> float:
        return self.__hydroobject_distance

    def set_hydroobject_distance(self, object_range) -> None:
        self.__hydroobject_distance = object_range

    # This functions checks if a weir is close to hydroobject.
    def check_hydroobject_distance(self) -> None:
        # Get the connection and select the weirs that do not lie close to the hydroobjects
        with self.get_datasource().get_connection() as connection:
            results = connection.execute('''
                SELECT DISTINCT *
                FROM stuw
                LEFT JOIN hydroobject ON st_dwithin(stuw.geometriepunt, hydroobject.geometrielijn, {distance}) 
                    WHERE hydroobject.hydroobjectid IS NULL
            '''.format(distance=self.get_hydroobject_distance()))

        # Insert the weirs into the suggestion table for the weirs.
        self.insert_suggestions_records(results, self.get_has_hydroobject_suggestion())

    def check_flow_height_difference(self) -> None:
        with self.get_datasource().get_connection() as connection:
            erroneous_heights = connection.execute('''
                SELECT DISTINCT stuw.*
                FROM stuw
                WHERE laagstedoorstroomhoogte > hoogstedoorstroomhoogte
            ''')

        self.insert_suggestions_records(erroneous_heights, self.get_flow_height_difference_suggestion())

    def build_threads(self) -> List[Thread]:
        hydroobject_thread = Thread(target=self.check_hydroobject_distance)
        flow_height_thread = Thread(target=self.check_flow_height_difference)

        return [hydroobject_thread, flow_height_thread]
