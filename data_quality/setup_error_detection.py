#  -*- coding: utf-8 -*-
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

# This is the setup file for the error detection, needs to be run once for a new database.
from datasources.NhiDatasource import NhiDatasource
from detectors import *

print('Starting setup')

# Get the database engine of the NHI database.
nhi_database_engine = NhiDatasource()

# The errors will be saved in a different schema then the 'normal' database, so create this schema.
nhi_database_connection = nhi_database_engine.get_connection()
detector = BrugDetector.BrugDetector()
nhi_database_connection.execute('''
    CREATE SCHEMA {quality_schema}
'''.format(quality_schema=detector.get_quality_schema_name()))

print('Creating cross section lines')

# For the checking of the hydroobject and dwarsprofiellen, an extra table is made with the proper objects for the lines.
# To the table an id is added as primary key, and an index on the geometry to speed up searching.
nhi_database_connection = nhi_database_engine.get_connection()
nhi_database_connection.execute('''
    CREATE TABLE {quality_schema}.cross_section_lines AS
    SELECT st_makeline(table_alias.geometriepunt) as cross_section, 
            table_alias.profielcode,
            table_alias.administratiefgebiedid,
            table_alias.typeprofielid
        FROM (SELECT
                geometriepunt,
                profielcode,
                administratiefgebiedid,
                typeprofielid
              FROM public.dwarsprofiel
            ORDER BY codevolgnummer
            ) table_alias
            GROUP BY table_alias.profielcode, table_alias.administratiefgebiedid, table_alias.typeprofielid; 
        
    CREATE INDEX cross_section_geom_idx
        ON {quality_schema}.cross_section_lines
        USING GIST (cross_section);
     
    ALTER TABLE {quality_schema}.cross_section_lines   
        ADD cross_section_linesid SERIAL NOT NULL;
    
    CREATE UNIQUE INDEX cross_section_lines_id_uindex
            ON {quality_schema}.cross_section_lines (cross_section_linesid);
            
    ALTER TABLE {quality_schema}.cross_section_lines
    ADD CONSTRAINT cross_section_lines_pk
           PRIMARY KEY (cross_section_linesid);
'''.format(quality_schema=detector.get_quality_schema_name()))

print('Creating suggestion tables')

detector_list = [
    AanvoergebiedDetector.AanvoergebiedDetector(),
    AdministratiefgebiedDetector.AdministratiefgebiedDetector(),
    AfsluitmiddelDetector.AfsluitmiddelDetector(),
    AfvoergebiedDetector.AfvoergebiedDetector(),
    AfwateringsgebiedDetector.AfwateringsgebiedDetector(),
    AquaductDetector.AquaductDetector(),
    BijzonderhydraulischeObjectDetector.BijzonderhydraulischeObjectDetector(),
    BodemvalDetector.BodemvalDetector(),
    BrugDetector.BrugDetector(),
    CrossSectionLinesDetector.CrossSectionLinesDetector(),
    DoorstroomopeningDetector.DoorstroomopeningDetector(),
    DuikersifonhevelDetector.DuikersifonhevelDetector(),
    DwarsprofielDetector.DwarsprofielDetector(),
    GemaalDetector.GemaalDetector(),
    GrondwaterInfolijnDetector.GrondwaterInfolijnDetector(),
    GrondwaterInfopuntDetector.GrondwaterInfopuntDetector(),
    GrondwaterKoppellijnDetector.GrondwaterKoppellijnDetector(),
    GrondwaterKoppelpuntDetector.GrondwaterKoppelpuntDetector(),
    HydraulischeRandvoorwaardeDetector.HydraulischeRandvoorwaardeDetector(),
    HydroobjectDetector.HydroobjectDetector(),
    LateraleknoopDetector.LateraleknoopDetector(),
    MeetlocatieDetector.MeetlocatieDetector(),
    NormgeparametriseerdProfielDetector.NormgeparametriseerdProfielDetector(),
    PompDetector.PompDetector(),
    StuwDetector.StuwDetector()
]

for detector in detector_list:
    # Now the schema is there the error tables for the detection can be created.
    print(detector.get_table_name())
    detector.create_suggestion_table()

    # To view the errors as a layer, for each error table a database view is created. This view links the error tables
    # to a table with the objects in the public schema. This view can then be loaded as a layer in a tool like QGIS.
    detector.create_view()

print('Setup finished')
