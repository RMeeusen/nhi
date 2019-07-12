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
from owslib.wcs import WebCoverageService


class AhnWebCoverageDatasource(object):
    def __init__(self):
        try:
            self.wcs_ahn3 = WebCoverageService('https://geodata.nationaalgeoregister.nl/ahn3/wcs', version='1.0.0')
            self.wcs_ahn2 = WebCoverageService('https://geodata.nationaalgeoregister.nl/ahn2/wcs', version='1.0.0')
        except:
            print("AHN WCS host unavailable")

    def retrieve_tile_ahn3(self, srid, x_min, y_min, x_max, y_max):
        output_ahn3 = self.wcs_ahn3.getCoverage(
            identifier='ahn3_05m_dtm',
            bbox=[x_min, y_min, x_max, y_max],
            format='GEOTIFF_FLOAT32',
            crs=srid,
            resx=0.5,
            resy=0.5
        )

        return output_ahn3

    def retrieve_tile_ahn2(self, srid, x_min, y_min, x_max, y_max):
        output_ahn2 = self.wcs_ahn2.getCoverage(
            identifier='ahn2_05m_int',
            bbox=[x_min, y_min, x_max, y_max],
            format='GEOTIFF_FLOAT32',
            crs=srid,
            resx=0.5,
            resy=0.5
        )

        return output_ahn2
