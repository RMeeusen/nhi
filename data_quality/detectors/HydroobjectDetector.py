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


# This detector detect problems with the hydro objects.
class HydroobjectDetector(Detector):
    __end_node_suggestion = {
        'code': None,
        'suggestion': None
    }

    __cross_section_suggestion = {
        'code': None,
        'suggestion': None
    }

    __roughness_suggestion = {
        'code': None,
        'suggestion': None
    }

    def __init__(self):
        super().__init__()
        self.set_table_name('hydroobject')
        self.set_end_node_suggestion(
            1701,
            'This hydroobject is not noded properly.'
        )
        self.set_cross_section_suggestion(
            1702,
            'Every hydroobject needs to have a cross section.'
        )
        self.set_roughness_suggestion(
            1703,
            'The low roughness value should always be lower than the high roughness value.'
        )

    def get_end_node_suggestion(self) -> Dict:
        return self.__end_node_suggestion

    def set_end_node_suggestion(self, code: int, suggestion: str) -> None:
        self.__end_node_suggestion = {
            'code': code,
            'suggestion': suggestion
        }

    def get_cross_section_suggestion(self) -> Dict:
        return self.__roughness_suggestion

    def set_cross_section_suggestion(self, code: int, suggestion: str) -> None:
        self.__cross_section_suggestion = {
            'code': code,
            'suggestion': suggestion
        }

    def get_roughness_suggestion(self) -> Dict:
        return self.__roughness_suggestion

    def set_roughness_suggestion(self, code, suggestion) -> None:
        self.__roughness_suggestion = {
           'code': code,
           'suggestion': suggestion
        }

    def check_intersections_end_nodes(self) -> None:
        # This function checks if the intersections between the hydroobjects themselves, are noded. So no line ends
        # in the middel of another line.
        with self.get_datasource().get_connection() as connection:
            results = connection.execute('''
                SELECT DISTINCT a.* FROM hydroobject a, hydroobject b
                    WHERE ST_Intersects(a.geometrielijn, b.geometrielijn)
                        AND NOT ST_Relate(a.geometrielijn, b.geometrielijn, '****0****')
                        AND a.hydroobjectid != b.hydroobjectid;
            ''')

        self.insert_suggestions_records(results, self.get_end_node_suggestion())

    def check_if_object_has_cross_section(self) -> None:
        # Every hydroobject needs to have a cross section. So check this with the intersection function if there are
        # intersections between the datasets.
        # The administration area with number 3 is not checked, because the cross section lines are invalid, and slow
        # down the query to much (by hours)
        with self.get_datasource().get_connection() as connection:
            results = connection.execute('''
                SELECT DISTINCT hydroobject.*, {quality_schema}.cross_section_lines.*
                FROM hydroobject
                    LEFT JOIN {quality_schema}.cross_section_lines 
                        ON st_intersects(hydroobject.geometrielijn, {quality_schema}.cross_section_lines.cross_section)
                    WHERE {quality_schema}.cross_section_lines.profielcode IS NULL
                    AND hydroobject.administratiefgebiedid != 3
            '''.format(quality_schema=self.get_quality_schema_name()))

        self.insert_suggestions_records(results, self.get_cross_section_suggestion())

    def check_roughness_value(self) -> None:
        # The roughness values in with the dwarsprofielen can have a few different qualifications:
        # 1=Chezy, 2=Manning, 3=StricklerKn, 4=StricklerKs, 5=White Colebrook, 6=Bos en Bijkerk
        # For Chezy, Bos & Bijker, Manning and Strickler, a higher value means a lower roughness, for White Colebrook a
        # lower value means a lower roughness.
        with self.get_datasource().get_connection() as connection:
            erroneous_roughness_values = connection.execute('''
                SELECT DISTINCT hydroobject.*
                FROM hydroobject
                WHERE 
                    CASE ruwheidstypeid 
                        WHEN 5 THEN ruwheidswaardelaag > ruwheidswaardehoog
                        ELSE ruwheidswaardelaag < ruwheidswaardehoog
                    END
            ''')

        self.insert_suggestions_records(erroneous_roughness_values, self.get_roughness_suggestion())

    def build_threads(self) -> List[Thread]:
        roughness_thread = Thread(target=self.check_roughness_value)
        intersection_thread = Thread(target=self.check_intersections_end_nodes)
        cross_section_thread = Thread(target=self.check_if_object_has_cross_section)

        return [roughness_thread, intersection_thread, cross_section_thread]
