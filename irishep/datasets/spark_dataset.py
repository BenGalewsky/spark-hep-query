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
from irishep.datasets.dataset import Dataset
# noinspection PyUnresolvedReferences
from pyspark.sql.functions import lit


class SparkDataset(Dataset):
    # There is no support in arrow for certain datatypes. Avoid exceptions by
    # casting the column to a supported datatype
    pyarrow_column_coverters = {
        "array<boolean>": lambda col: col.cast("array<int >")
    }

    def _pyarrow_compatble_column(self, col, col_type):
        """
        Convert the colummn into a cast statement if the column's type is not
        supported by pyArrow
        :param col: Column Object
        :param col_type: Type name as a string
        :return: Column, or casted column
        """
        if col_type in self.pyarrow_column_coverters:
            return self.pyarrow_column_coverters[col_type](col)
        else:
            return col

    def __init__(self, name, dataframe):
        super().__init__(name)

        if 'dataset' not in dataframe.columns:
            self.dataframe = dataframe.withColumn("dataset", lit(name))
        else:
            self.dataframe = dataframe

    def count(self):
        return self.dataframe.count()

    @property
    def columns(self):
        """
        Fetch the list of column names from the dataset
        :return: List of string column names
        """
        return self.dataframe.columns

    @property
    def columns_with_types(self):
        """
        Fetch the list of column names along with their datatypes
        :return: List of tuples with column name and datatype as string
        """
        return self.dataframe.dtypes

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
        columns2 = set(columns).union(
            ["dataset", "run", "luminosityBlock", "event"])

        projected = self.dataframe.select(list(columns2))

        columns3 = [self._pyarrow_compatble_column(projected[c[0]], c[1]) for c
                    in projected.dtypes]

        return SparkDataset(name=self.name,
                            dataframe=self.dataframe.select(list(columns3)))

    def show(self):
        """
        Print out a friendly representation of the dataframe
        :return: None
        """
        self.dataframe.show()

    def repartition(self, num_partitions):
        """
        Distribute the dataframe across the given number of partitions
        :param num_partitions: Number of partitions
        :return: None
        """
        self.dataframe = self.dataframe.repartition(num_partitions)
