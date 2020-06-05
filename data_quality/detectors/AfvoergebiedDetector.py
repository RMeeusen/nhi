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


class AfvoergebiedDetector(Detector):
    __overlap_suggestion = {
        'code': None,
        'suggestion': None
    }

    __size_suggestion = {
        'code': None,
        'suggestion': None
    }

    def __init__(self) -> None:
        super().__init__()
        super().set_table_name('afvoergebied')
        self.set_overlap_suggestion(
            1001,
            'A catchment area should not overlap with another catchment area.'
        )
        self.set_size_suggestion(
            1002,
            'The area of the catchment should be larger than zero.'
        )

    def get_overlap_suggestion(self) -> Dict:
        return self.__overlap_suggestion

    def set_overlap_suggestion(self, code: int, suggestion: str) -> None:
        self.__overlap_suggestion = {
            'code': code,
            'suggestion': suggestion
        }

    def get_size_suggestion(self) -> Dict:
        return self.__size_suggestion

    def set_size_suggestion(self, code: int, suggestion: str) -> None:
        self.__size_suggestion = {
            'code': code,
            'suggestion': suggestion
        }

    def check_self_overlap(self) -> None:
        with self.get_datasource().get_connection() as connection:
            results = connection.execute('''
                SELECT DISTINCT table_a.*
                FROM afvoergebied as table_a
                LEFT JOIN afvoergebied as table_b ON
                    st_overlaps(table_b.geometrievlak, table_a.geometrievlak) OR
                    st_contains(table_b.geometrievlak, table_a.geometrievlak) 
                WHERE table_a.afvoergebiedid != table_b.afvoergebiedid
                    AND table_b.afvoergebiedid IS NOT NULL
            ''')

        self.insert_suggestions_records(results, self.get_overlap_suggestion())

    # This functions checks that the catchment area is larger than zero
    def check_minimum_size(self) -> None:
        with self.get_datasource().get_connection() as connection:
            catchments = connection.execute('''
                SELECT DISTINCT *
                FROM afvoergebied
                WHERE NOT oppervlakte > 0
            ''')

        self.insert_suggestions_records(catchments, self.get_size_suggestion())

    def build_threads(self) -> List[Thread]:
        catchment_overlap = Thread(target=self.check_self_overlap)

        return [catchment_overlap]
