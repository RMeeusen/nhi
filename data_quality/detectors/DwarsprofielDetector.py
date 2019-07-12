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
import ogr

from datasources.DatasourceRegistry import DatasourceRegistry
from detectors.Detector import Detector


class DwarsprofielDetector(Detector):
    # The margin for the AHN data is 20cm, this is the 99,7% value for the systematic and stochastic error made in the
    # gathering of the ground level data of the AHN.
    __ahn_top_margin = None
    __ahn_bottom_margin = None

    __ahn_height_suggestion = {
        'code': None,
        'suggestion': None
    }

    __roughness_suggestion = {
        'code': None,
        'suggestion': None
    }

    def __init__(self):
        super(DwarsprofielDetector, self).__init__()
        self.set_table_name('dwarsprofiel')

        self.set_ahn_top_margin(0.1)
        self.set_ahn_bottom_margin(0.2)

        self.set_ahn_height_suggestion(
            1501,
            'The cross section point needs to be within +' + str(self.get_ahn_top_margin()) + ' meters and -' +
            str(self.get_ahn_bottom_margin()) + ' meters of the AHN value.',
        )
        self.set_roughness_suggestion(
            1502,
            'The low roughness value should be lower than the high roughness value.'
        )

    def get_ahn_height_suggestion(self) -> Dict:
        return self.__ahn_height_suggestion

    def set_ahn_height_suggestion(self, code: int, suggestion: str) -> None:
        self.__ahn_height_suggestion = {
            'code': code,
            'suggestion': suggestion
        }

    def get_roughness_suggestion(self) -> Dict:
        return self.__roughness_suggestion

    def set_roughness_suggestion(self, code: int, suggestion: str) -> None:
        self.__roughness_suggestion = {
            'code': code,
            'suggestion': suggestion
        }

    def get_ahn_top_margin(self) -> float:
        return self.__ahn_top_margin

    def set_ahn_top_margin(self, margin: float):
        self.__ahn_top_margin = margin

    def get_ahn_bottom_margin(self) -> float:
        return self.__ahn_bottom_margin

    def set_ahn_bottom_margin(self, margin: float):
        self.__ahn_bottom_margin = margin

    def check_with_ahn_heights(self, cross_sections_on_tile, ahn_tile_dataset, ahn_tile_array) -> None:
        for cross_section in cross_sections_on_tile:

            # Create a geometry from the well known text from the database
            cross_section_ogr = ogr.CreateGeometryFromWkt(getattr(cross_section, 'geometriepunt_wkt'))
            # Get the value for the ahn height for a 0.5 by 0.5 pixel from the ahn service
            row, column = ahn_tile_dataset.index(cross_section_ogr.GetX(), cross_section_ogr.GetY())
            row = int(row)
            column = int(column)
            try:
                ahn_height = ahn_tile_array[int(row), int(column)]
            except IndexError:
                print('IndexError, row: ' + str(row) + ' column: ' + str(column))
                continue

            # If no Ahn data is found, skip the rest, and get the next cross section.
            if ahn_height is None or ahn_height == 0.0:
                continue

            # When there is a pixel found in the Ahn database, check this value with the HyDAMO value for
            # the cross section.
            cross_section_z = cross_section_ogr.GetZ()

            ahn_max = ahn_height + self.get_ahn_top_margin()
            ahn_min = ahn_height - self.get_ahn_bottom_margin()
            if cross_section_z < ahn_min or cross_section_z > ahn_max:
                # When the data points are not within the error margin, insert them into the database.
                self.insert_ahn_suggestion_into_database(cross_section, cross_section_z, ahn_height)

    def insert_ahn_suggestion_into_database(self, cross_section, cross_section_z: float, ahn_height: float) -> None:
        nhi_datasource = DatasourceRegistry.retrieve('NhiDatasource')
        with nhi_datasource.get_connection() as connection:
            connection.execute('''
            INSERT INTO {quality_schema}.{table_name}_suggestions ({table_name}_id, code, description)
                VALUES ({foreign_key}, {error_code}, '{error_suggestion}')
            '''.format(foreign_key=getattr(cross_section, self.get_table_name() + 'id'),
                       error_code=self.get_ahn_height_suggestion()['code'],
                       error_suggestion=self.get_ahn_height_suggestion()['suggestion'] +
                                        ' AHN height: ' + str(ahn_height) +
                                        ', cross section height: ' + str(cross_section_z) + '.',
                       table_name=self.get_table_name(),
                       quality_schema=self.get_quality_schema_name()
                       ))

    @staticmethod
    def retrieve_cross_section_with_bounding_box(srid, x_min, y_min, x_max, y_max):
        nhi_datasource = DatasourceRegistry.retrieve('NhiDatasource')
        with nhi_datasource.get_connection() as connection:
            cross_sections_result = connection.execute('''
                SELECT dwarsprofiel.*,
                    st_astext(dwarsprofiel.geometriepunt) AS geometriepunt_wkt,
                    st_srid(dwarsprofiel.geometriepunt) AS geometriepunt_srid
                FROM dwarsprofiel
                WHERE st_intersects(st_makeenvelope({x_min}, {y_min}, {x_max}, {y_max}, {srid}), dwarsprofiel.geometriepunt)
            '''.format(srid=srid, x_min=x_min, y_min=y_min, x_max=x_max, y_max=y_max))

        return cross_sections_result.fetchall()

    # The roughness values in with the dwarsprofielen can have a few different qualifications:
    # 1=Chezy, 2=Manning, 3=StricklerKn, 4=StricklerKs, 5=White Colebrook, 6=Bos en Bijkerk
    # For Chezy, Bos & Bijker, Manning and Strickler, a higher value means a lower roughness, for White Colebrook a
    # lower value means a lower roughness.
    def check_roughness(self) -> None:
        with self.get_datasource().get_connection() as connection:
            erroneous_roughnesses = connection.execute('''
                SELECT DISTINCT dwarsprofiel.*
                FROM dwarsprofiel
                WHERE
                    CASE ruwheidstypeid
                        WHEN 5 THEN ruwheidswaardelaag > ruwheidswaardehoog
                        ELSE ruwheidswaardelaag < ruwheidswaardehoog
                    END
            ''')

        self.insert_suggestions_records(erroneous_roughnesses, self.get_roughness_suggestion())

    def build_threads(self) -> List[Thread]:
        roughness_thread = Thread(target=self.check_roughness)

        return [roughness_thread]
