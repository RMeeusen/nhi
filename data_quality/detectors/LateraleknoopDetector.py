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
from typing import Dict, List

from detectors.Detector import Detector


class LateraleknoopDetector(Detector):
    __containment_in_catchment_suggestion = {
        'code': None,
        'suggestion': None
    }

    def __init__(self):
        super().__init__()
        self.set_table_name('lateraleknoop')
        self.set_contained_in_catchment_suggestion(
            1801,
            'A laterale knoop needs to lie within the associated catchment area.'
        )

    def get_contained_in_catchment_suggestion(self) -> Dict:
        return self.__containment_in_catchment_suggestion

    def set_contained_in_catchment_suggestion(self, code: int, suggestion: str) -> None:
        self.__containment_in_catchment_suggestion = {
            'code': code,
            'suggestion': suggestion
        }

    def check_containment_in_catchment(self) -> None:
        with self.get_datasource().get_connection() as connection:
            laterale_knopen_outside_catchment = connection.execute('''
                SELECT DISTINCT *
                FROM lateraleknoop
                JOIN afvoergebied on lateraleknoop.lateraleknoopid = afvoergebied.lateraleknoopid
                WHERE NOT st_intersects(afvoergebied.geometrievlak, lateraleknoop.geometriepunt)   
            ''')

        self.insert_suggestions_records(laterale_knopen_outside_catchment, self.get_contained_in_catchment_suggestion())

    def build_threads(self) -> List[Thread]:
        catchment_thread = Thread(target=self.check_containment_in_catchment)

        return [catchment_thread]
