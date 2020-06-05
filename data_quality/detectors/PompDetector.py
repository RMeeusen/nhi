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


class PompDetector(Detector):
    __hydroobject_range = 10

    __hydroobject_intersection_suggestion = {
        'code': None,
        'suggestion': None
    }

    def __init__(self):
        super().__init__()
        self.set_table_name('pomp')
        self .set_hydroobjest_intersection_suggestion(
            2001,
            'The pump needs to lie within a range of ' + str(self.get_hydroobject_distance()) +
            ' meters of a hydroobject.'
        )

    def get_hydroobject_distance(self) -> float:
        return self.__hydroobject_range

    def set_hydroobject_distance(self, distance: float) -> None:
        self.__hydroobject_range = distance

    def get_hydroobject_intersection_suggestion(self) -> Dict:
        return self.__hydroobject_intersection_suggestion

    def set_hydroobjest_intersection_suggestion(self, code: int, suggestion: str) -> None:
        self.__hydroobject_intersection_suggestion = {
            'code': code,
            'suggestion': suggestion
        }

    # This functions checks if the pump lies in a preset range of a hydroobject.
    def check_hydroobject_distance(self) -> None:
        with self.get_datasource().get_connection() as connection:
            results = connection.execute('''
                SELECT DISTINCT pomp.*
                FROM pomp
                    LEFT JOIN hydroobject ON st_dwithin(pomp.geometriepunt, hydroobject.geometrielijn, {range})
                    WHERE hydroobjectid IS NULL;
            '''.format(range=self.get_hydroobject_distance()))

        self.insert_suggestions_records(results, self.get_hydroobject_intersection_suggestion())

    def build_threads(self) -> List[Thread]:
        hydroobject_thread = Thread(target=self.check_hydroobject_distance)

        return [hydroobject_thread]
