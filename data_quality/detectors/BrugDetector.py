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
from typing import List, Dict

import ogr

from detectors.Detector import Detector


class BrugDetector(Detector):
    __hydroobject_distance = None

    __hydroobject_distance_suggestion = {
        'code': None,
        'suggestion': None
    }

    __ground_level_suggestion = {
        'code': None,
        'suggestion': None
    }

    __bottom_lower_than_top_suggestion = {
        'code': None,
        'suggestion': None
    }

    def __init__(self):
        super().__init__()
        self.set_table_name('brug')
        self.set_hydroobject_distance(1)
        self.set_hydroobject_distance_suggestion(
            1201,
            'The bridge must lie near (within ' + str(self.get_hydroobject_range()) + ' meters) a hydroobject.'
        )
        self.set_ground_level_suggestion(
            1202,
            'The bridge top height should be higher than the surrounding ground level.'
        )
        self.set_bottom_lower_than_top_suggestion(
            2103,
            'The bottom of the bridge must be lower than the top of the bridge.'
        )

    # Range of the bridge to the closest hydroobject
    def get_hydroobject_range(self) -> float:
        return self.__hydroobject_distance

    def set_hydroobject_distance(self, distance: float) -> None:
        self.__hydroobject_distance = distance

    def get_hydroobject_distance_suggestion(self) -> Dict:
        return self.__hydroobject_distance_suggestion

    def set_hydroobject_distance_suggestion(self, code: int, suggestion: str) -> None:
        self.__hydroobject_distance_suggestion = {
            'code': code,
            'suggestion': suggestion
        }

    def get_ground_level_suggestion(self):
        return self.__ground_level_suggestion

    def set_ground_level_suggestion(self, code: int, suggestion: str) -> None:
        self.__ground_level_suggestion = {
            'code': code,
            'suggestion': suggestion
        }

    def get_bottom_lower_than_top_suggestion(self) -> Dict:
        return self.__bottom_lower_than_top_suggestion

    def set_bottom_lower_than_top_suggestion(self, code, suggestion) -> None:
        self.__bottom_lower_than_top_suggestion = {
            'code': code,
            'suggestion': suggestion
        }

    def has_entries_in_bouding_box(self, srid: int, bounding_box: List[float]) -> bool:
        return False

    def check_ahn_height_top_bridge(self, bridges_on_tile, ahn_tile_dataset, ahn_tile_array):
        for bridge in bridges_on_tile:
            bridge_geometry = ogr.CreateGeometryFromWkt(getattr(bridge, 'geometriepunt_wkt'))
            # Get the value for the ahn height for a 0.5 by 0.5 pixel from the ahn service
            row, column = ahn_tile_dataset.index(bridge_geometry.GetX(), bridge_geometry.GetY())
            row = int(row)
            column = int(column)

            size_steps = 0
            min_row = 0
            max_row = 0
            min_col = 0
            max_col = 0

            while True:
                try:
                    ahn_height = ahn_tile_array[int(row), int(column)]
                except IndexError:
                    print('IndexError, row: ' + str(row) + ' column: ' + str(column))

                # If no Ahn data is found, make the area to check larger
                if ahn_height is None or ahn_height == 0.0:
                    min_row = min_row - 1
                    max_row = max_row + 1
                    min_col = min_col - 1
                    max_col = max_col + 1

    def check_distance_to_hydroobject(self):
        with self.get_datasource().get_connection() as connection:
            bridges_without_hydroobject = connection.execute('''
                SELECT DISTINCT brug.*
                FROM brug
                LEFT JOIN hydroobject ON st_dwithin(brug.geometriepunt, hydroobject.geometrielijn, {range})
                WHERE hydroobject.hydroobjectid IS NULL
            '''.format(range=self.get_hydroobject_range()))

        self.insert_suggestions_records(bridges_without_hydroobject, self.get_hydroobject_distance_suggestion())

    def check_bottom_lower_than_top(self):
        with self.get_datasource().get_connection() as connection:
            bridges_with_erroneous_heights = connection.execute('''
                SELECT DISTINCT *
                FROM brug
                WHERE hoogteonderzijde > hoogtebovenzijde
            ''')

        self.insert_suggestions_records(bridges_with_erroneous_heights, self.get_bottom_lower_than_top_suggestion())

    #def check_bridge_top_heigher_than_ahn(self, bounding_box, ahn_tile):
    #    bridges_to_be_checked

    def build_threads(self) -> List[Thread]:
        hydroobject_distance_thread = Thread(target=self.check_distance_to_hydroobject)
        bottom_top_thread = Thread(target=self.check_bottom_lower_than_top)

        return [hydroobject_distance_thread, bottom_top_thread]
