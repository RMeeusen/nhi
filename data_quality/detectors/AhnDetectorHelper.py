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
import datetime
import threading
from queue import Empty, Queue

from rasterio import open as rasterio_open, Env, MemoryFile
from rasterio.errors import RasterioIOError
from requests.exceptions import ConnectionError, ChunkedEncodingError

from datasources.AhnDatasource import AhnWebCoverageDatasource
from detectors.DwarsprofielDetector import DwarsprofielDetector


class AhnDetectorHelper:
    def __init__(self):
        super().__init__()

    def check_tiles_using_threads(self, threads) -> None:
        start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # The queue will hold all the coordinates for the parts of the map that are checked
        tile_queue = Queue()

        print("Starting AHN threads")
        print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        for index in range(threads):
            thread = threading.Thread(target=self.tile_thread, args=(tile_queue,))
            thread.start()

        # coverage_BBOX = [13000, 279000, 306250, 616250]
        # The values are rounded here to 2000, so it can be divided equally when selecting parts of the map of the
        # Netherlands
        total_x_min = 14000
        total_y_min = 280000
        total_x_max = 306000
        total_y_max = 616000

        # The map parts are 2000 by 2000 because this is the maximum size the AHN server allows.
        columns = (total_x_max - total_x_min) / 2000
        rows = (total_y_max - total_y_min) / 2000

        # Build the Queue for all the tiles in the country
        for row in range(int(rows)):
            for column in range(int(columns)):
                x_min = row * 2000 + total_x_min
                y_min = column * 2000 + total_y_min
                x_max = (row + 1) * 2000 + total_x_min
                y_max = (column + 1) * 2000 + total_y_min

                tile_queue.put({
                    'x_min': x_min,
                    'y_min': y_min,
                    'x_max': x_max,
                    'y_max': y_max,
                    'row': row,
                    'column': column
                })

        tile_queue.join()

        print('AHN start time: ' + start_time)
        print("Finished AHN threads")
        print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    @staticmethod
    def tile_thread(tile_queue) -> None:
        while True:
            try:
                tile_details = tile_queue.get(timeout=5)
            except Empty:
                break

            cross_section_detector = DwarsprofielDetector()
            cross_sections_on_tile = cross_section_detector.retrieve_cross_section_with_bounding_box(
                28992,
                tile_details['x_min'],
                tile_details['y_min'],
                tile_details['x_max'],
                tile_details['y_max']
            )

            if len(cross_sections_on_tile) == 0:
                tile_queue.task_done()
                continue

            # First try to load the file from disk.
            try:
                dataset = open('data/ahn2_geotiff_row_' + str(tile_details['row']) + '_column_' +
                               str(tile_details['column']) + '.tif')
                cross_section_detector.check_with_ahn_heights(cross_sections_on_tile, dataset, dataset.read(1))

            # If the data is not found on the disk, make connection with the AHN server and get the tile.
            except RasterioIOError:
                print('Tile not found on disk' + str(tile_details['row']) + '-' + str(tile_details['column']))
                ahn_datasource = AhnWebCoverageDatasource()
                try:
                    ahn_response = ahn_datasource.retrieve_tile_ahn2(
                        'EPSG:28992',
                        tile_details['x_min'],
                        tile_details['y_min'],
                        tile_details['x_max'],
                        tile_details['y_max'],
                    )
                except (ConnectionError, ChunkedEncodingError, AttributeError):
                    print('Ahn connection error, ')
                    print('error on ' + str(tile_details['row']) + ', ' + str(tile_details['column']))
                    tile_queue.task_done()
                    tile_queue.put(tile_details)
                    continue

                with MemoryFile(ahn_response) as memfile:
                    with memfile.open() as dataset:
                        tile_array = dataset.read(1)
    
                        with Env():
                            profile = dataset.profile
    
                            with rasterio_open('data/ahn2_geotiff_row_' + str(tile_details['row']) +
                                      '_column_' + str(tile_details['column']) + '.tif', 'w', **profile) as dst:
                                dst.write(tile_array, 1)

                        cross_section_detector.check_with_ahn_heights(cross_sections_on_tile, dataset, tile_array)

            tile_queue.task_done()

            if tile_queue.qsize() % 1000 == 0:
                print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                print(str(tile_queue.qsize()))
