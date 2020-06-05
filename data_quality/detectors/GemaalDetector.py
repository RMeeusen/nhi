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
from threading import Thread
from typing import List, Dict

from detectors.Detector import Detector


# The gemaal detector checks if pumping stations of good quality
class GemaalDetector(Detector):
    # Range were a pumping station should lie apposed to a hydroobject in meters.
    __hydroobject_range = None

    __hydroobject_distance_suggestion = {
        'code': None,
        'suggestion': None
    }

    def __init__(self):
        super().__init__()
        self.set_table_name('gemaal')
        self.set_hydroobject_distance(10)
        self.set_hydroobject_distance_suggestion(
            1601,
            'The pumping station needs to lie within a range of ' + str(self.get_hydroobject_distance()) + ' meters of '
            'a hydroobject.'
        )

    def get_hydroobject_distance(self) -> float:
        return self.__hydroobject_range

    def set_hydroobject_distance(self, detection_range: float):
        self.__hydroobject_range = detection_range

    # The error message for when the range a hydroobject lies in is exceeded.
    def get_hydroobject_distance_suggestion(self) -> Dict:
        return self.__hydroobject_distance_suggestion

    def set_hydroobject_distance_suggestion(self, code: int, suggestion: str) -> None:
        self.__hydroobject_distance_suggestion = {
            'code': code,
            'suggestion': suggestion
        }

    # Function to check the hydroobject range. The range is set to a default value, or can be set via the set function.
    # After first getting all the erroneous data from the data table, the errors are inserted.
    def check_hydroobject_distance(self) -> None:
        # Detection boundary in meters. How far can a pumping station lie from a hydroobject?
        with self.get_datasource().get_connection() as connection:
            results = connection.execute('''
                SELECT DISTINCT gemaal.*
                    FROM gemaal
                    LEFT JOIN hydroobject ON st_dwithin(gemaal.geometriepunt, hydroobject.geometrielijn, {range}) 
                    WHERE hydroobject.hydroobjectid IS NULL
            '''.format(range=self.get_hydroobject_distance()))

        # Insert all the errors that are found with a code and description.
        self.insert_suggestions_records(results, self.get_hydroobject_distance_suggestion())

    def build_threads(self) -> List[Thread]:
        thread_hydroobject_distance = Thread(target=self.check_hydroobject_distance)

        return [thread_hydroobject_distance]
