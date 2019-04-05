# Copyright (c) 2019, IRIS-HEP
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# * Neither the name of the copyright holder nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


import re

from abc import ABCMeta, abstractmethod


class Dataset(metaclass=ABCMeta):

    def __init__(self, name):
        self.name = name

    @abstractmethod
    def count(self):
        """

        :return:
        """

    @property
    @abstractmethod
    def columns(self):
        """
        Fetch the list of column names from the dataset
        :return: List of string column names
        """

    @property
    @abstractmethod
    def columns_with_types(self):
        """
        Fetch the list of column names along with their datatypes
        :return: List of tuples with column name and datatype as string
        """

    def columns_for_physics_objects(self, physics_objects):
        """
        Return a list of columns that form part of the requested physics_objects
        This will include all properties of the pyhsics object, or a count
        variable associated with the object such as nElectrons
        :param physics_objects:
        :return: list of columns
        """
        # Create column Names for the count properties (nElectrons, nMuons, etc)
        physics_obj_count_cols = ["n" + col for col in physics_objects]

        # Join together into a series of alternate REs
        physics_obj_count_re = "(" + ")|(".join(physics_obj_count_cols) + ")"

        # Create an RE that will match a physics object's properties
        # i.e. Electon_.*
        physics_obj_re = "(" + ")|(".join(physics_objects) + ")_.*"

        # Now create a composite RE that will match a count or a
        # physics obj property
        r = re.compile(
            "(" + physics_obj_re + ")|(" + physics_obj_count_re + ")")

        # Filter and return list
        # noinspection PyTypeChecker
        return [col for col in self.columns if r.match(col)]

    @staticmethod
    def count_column_for_physics_object(physics_object):
        """
        Generate a column name that represents the count property for a physics
        object. i.e. nElectron
        :param physics_object: the name of the physics object
        :return: Column name for the count of this object
        """
        return "n" + physics_object

    @abstractmethod
    def select_columns(self, columns):
        """
        Create a new dataset object that contains only the specified columns.
        For techincal reasons there are some identifying columns that will
        be included in the result even if they are not requested. Columns
        with a type that is not supported by pyarrow will be casted to a
        supported type
        :param columns: List of column names
        :return: New dataframe with only the requested columns
        """

    def udf_arguments(self, physics_objects):
        """
        Construct the set of argumnents to UDFs on this dataset based on the
        requested Physics Objects.
        For now we also include the dataset name column for bookkeeping
        :param physics_objects: List of physics object names
        :return: List of colums for passing in as arguments to UDF
        """
        return ["dataset"]+self.columns_for_physics_objects(physics_objects)

    @abstractmethod
    def show(self):
        """
        Print out a friendly representation of the dataframe
        :return: None
        """

    @abstractmethod
    def repartition(self, num_partitions):
        """
        Distribute the dataframe across the given number of partitions
        :param num_partitions: Number of partitions
        :return: None
        """

    @abstractmethod
    def execute_udf(self, user_func):
        """
        Execute the UDF locally
        :param user_func: Instance of UserFunction to execute
        :return:
        """
