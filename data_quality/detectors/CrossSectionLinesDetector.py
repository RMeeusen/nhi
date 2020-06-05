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


class CrossSectionLinesDetector(Detector):
    __has_hydroobject_suggestion = {
        'code': None,
        'suggestion': None
    }

    def __init__(self):
        super().__init__()
        # The hydamo schema name is set to data quality to create and then use the table from the data quality schema.
        # This way the hydamo schema is not changed
        self.set_hydamo_schema_name('data_quality')
        self.set_table_name('cross_section_lines')
        self.set_has_hydroobject_suggestion(
            1301,
            'Every cross section should lie on a hydroobject.',
        )

    def get_has_hydroobject_suggestion(self) -> Dict:
        return self.__has_hydroobject_suggestion

    def set_has_hydroobject_suggestion(self, code: int, suggestion: str) -> None:
        self.__has_hydroobject_suggestion = {
            'code': code,
            'suggestion': suggestion
        }

    # This function checks if a cross section has an intersection with a hydroobject. It uses a table called
    # cross_section_lines that are the cross sections in line form. This table has to be generated before this function
    # can be used.
    def check_if_cross_section_line_has_hydroobject(self):
        # Every hydroobject needs to have a cross section. So check this with the intersection function if there are
        # intersections between the data sets. Only typeprofielid 4 is checked, because that is the type of profile that
        # is a cross section of a watercourse.
        with self.get_datasource().get_connection() as connection:
            results = connection.execute('''
                SELECT DISTINCT {quality_schema}.cross_section_lines.*, hydroobject.* 
                FROM {quality_schema}.cross_section_lines
                    LEFT JOIN hydroobject ON st_intersects(
                            public.hydroobject.geometrielijn, {quality_schema}.cross_section_lines.cross_section
                        )
                    WHERE hydroobject.hydroobjectid IS NULL
                    AND cross_section_lines.typeprofielid = 4
            '''.format(quality_schema=self.get_quality_schema_name()))

        # Insert the cross sections that don't have a hydroobject into the errors table
        self.insert_suggestions_records(results, self.get_has_hydroobject_suggestion())

    def build_threads(self) -> List[Thread]:
        hydroobject_intersection_thread = Thread(target=self.check_if_cross_section_line_has_hydroobject)

        return [hydroobject_intersection_thread]
