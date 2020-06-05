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


class BodemvalDetector(Detector):
    __hydroobject_intersection_suggestion = {
        'code': None,
        'suggestion': None
    }

    def __init__(self) -> None:
        super().__init__()
        self.set_table_name('bodemval')
        self.set_hydroobject_intersection_suggestion(
            1101,
            'The bodemval must be on top of a hydroobject.'
        )

    def get_hydroobject_intersection_suggestion(self) -> Dict:
        return self.__hydroobject_intersection_suggestion

    def set_hydroobject_intersection_suggestion(self, code: int, suggestion: str) -> None:
        self.__hydroobject_intersection_suggestion = {
            'code': code,
            'suggestion': suggestion
        }

    def check_hydroobject_intersection(self) -> None:
        with self.get_datasource().get_connection() as connection:
            bodemvallen_without_hydroobject = connection.execute('''
                SELECT DISTINCT bodemval.*
                FROM bodemval
                    LEFT JOIN hydroobject ON st_intersects(bodemval.geometriepunt, hydroobject.geometrielijn)
                WHERE hydroobjectid IS NULL  
            ''')

            self.insert_suggestions_records(
                bodemvallen_without_hydroobject,
                self.get_hydroobject_intersection_suggestion()
            )

    def build_threads(self) -> List[Thread]:
        check_intersection_thread = Thread(target=self.check_hydroobject_intersection)

        return [check_intersection_thread]
