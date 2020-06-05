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
#
from abc import ABC, abstractmethod
from threading import Thread
from typing import List, Dict, Iterable

from datasources.NhiDatasource import NhiDatasource


# An error detector searches for errors in the PostGIS database of the NHI and links errors to the objects
class Detector(ABC):
    # This is the name of the schema that holds the HyDAMO data.
    __hydamo_schema_name = None

    # This is the name of the schema that will be generated and used for the data quality suggestions. Also new tables
    # that are added like cross_section_lines, are in the data quality section of the database.
    __quality_schema_name = None

    # This is the name of the table in the HyDAMO schema that the inheriting class will use to check for errors.
    __table_name = ''

    # This property will hold a database engine to access the NHI database.
    __database = None

    # Every detector has an instance of the database engine to connect to the NHI database
    def __init__(self) -> None:
        self.__database = NhiDatasource()
        self.set_hydamo_schema_name('public')
        self.set_quality_schema_name('data_quality')

    # This should return the name of the table that is checked for errors
    def get_table_name(self) -> str:
        return self.__table_name

    # The set function for the HyDAMO table name.
    def set_table_name(self, name: str) -> None:
        self.__table_name = name

    # This function returns the HyDAMO database engine.
    def get_datasource(self):
        return self.__database

    # This function returns the schema used for the data quality tables.
    def get_quality_schema_name(self) -> str:
        return self.__quality_schema_name

    # This function sets the schema used for the data quality tables.
    def set_quality_schema_name(self, name: str) -> None:
        self.__quality_schema_name = name

    # This function returns the name of the HyDAMO schema (default is 'public')
    def get_hydamo_schema_name(self) -> str:
        return self.__hydamo_schema_name

    # This sets the HyDAMO schema name.
    def set_hydamo_schema_name(self, name: str) -> None:
        self.__hydamo_schema_name = name

    def create_view(self) -> None:
        # Create a view to display the errors with. The view is a join of the errors to the original tables, to get a
        # picture of the object that are erroneous. The join table id needs to be added, otherwise there is no unique
        # id (a object can have more then one error).
        with self.__database.get_connection() as connection:
            connection.execute('''
                CREATE VIEW {quality_schema}.{table_name}_suggestions_view AS
                SELECT {table_name}.*,
                    {table_name}_suggestions.code as suggestion_code, 
                    {table_name}_suggestions.id as unique_suggestion_id,
                    {table_name}_suggestions.description as suggestion
                    FROM {schema_name}.{table_name}
                    INNER JOIN {quality_schema}.{table_name}_suggestions 
                    ON ({table_name}.{table_name}id = {table_name}_suggestions.{table_name}_id)
            '''.format(
                table_name=self.get_table_name(),
                schema_name=self.get_hydamo_schema_name(),
                quality_schema=self.get_quality_schema_name()
            ))

    def create_suggestion_table(self) -> None:
        # Every error checker needs a join table to join the errors to the erroneous object. The table holds the foreign
        # key of the error table and the table that is checked for errors.
        # The primary key in the tables is mostly the 'code' column, but this has errors in it for some tables. The
        # tables also have id column (like hydroobjectid) that also has a unique constrained, so this is used as the
        # foreign key to join the tables on.
        with self.__database.get_connection() as connection:
            connection.execute('''
                create table {quality_schema}.{table_name}_suggestions
                (
                    id          serial not null
                        constraint {table_name}_suggestions_pk
                            primary key,
                        {table_name}_id   int,
                    code int,
                    description text
                );
            '''.format(
                table_name=self.get_table_name(),
                schema_name=self.get_hydamo_schema_name(),
                quality_schema=self.get_quality_schema_name()
            ))

    # This function takes two values, where in between the value in the record must lie.
    def check_attribute_value_range(
            self, attribute_name: str, range_min: float, range_max: float, suggestion: Dict
    ) -> None:

        with self.get_datasource().get_connection() as connection:
            out_of_range_objects = connection.execute('''
                SELECT *
                FROM {table_name}
                WHERE {attribute_name} < {range_min}
                    OR {attribute_name} > {range_max}
            '''.format(
                table_name=self.get_table_name(),
                attribute_name=attribute_name,
                range_max=range_max,
                range_min=range_min
            ))

        self.insert_suggestions_records(out_of_range_objects, suggestion)

    # This function takes a list of erroneous objects and the dict with the error code and description, and inserts it
    # into the error table corresponding to the object table.
    def insert_suggestions_records(self, records: Iterable, suggestion_description: Dict) -> None:
        # Insert all the errors that are found with a code and description.
        for record in records:
            with self.get_datasource().get_connection() as connection:
                connection.execute('''
                INSERT INTO {quality_schema}.{table_name}_suggestions ({table_name}_id, code, description)
                    VALUES ({foreign_key}, {code}, '{suggestion}')
                '''.format(foreign_key=getattr(record, self.get_table_name() + 'id'),
                           code=suggestion_description['code'],
                           suggestion=suggestion_description['suggestion'],
                           table_name=self.get_table_name(),
                           quality_schema=self.get_quality_schema_name()
                           ))

    # This function empties the suggestions table.
    def remove_old_errors(self) -> None:
        with self.__database.get_connection() as connection:
            connection.execute('''
                DELETE FROM {quality_schema}.{table_name}_suggestions'''.format(
                table_name=self.get_table_name(),
                quality_schema=self.get_quality_schema_name()
            ))

    @abstractmethod
    def build_threads(self) -> List[Thread]:
        pass
