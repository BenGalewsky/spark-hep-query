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
import awkward as awk
import numpy as np


class AwkwardDataset(Dataset):
    def __init__(self, name, array_dict):
        super().__init__(name)

        self.arrays = {key.decode("utf-8"): value for key, value in
                       array_dict.items()}

        # Add in the technical dataset column if not already there
        if 'dataset' not in self.arrays.keys():
            dataset_dtype = '<U%d' % (len(name))
            self.arrays['dataset'] = np.full(
                (self._count_for_array_dict(self.arrays)),
                name, dtype=dataset_dtype)

    @staticmethod
    def _count_for_array_dict(array_dict):
        """
        Return the number of events. Assume that the count of the first element
        is representative of the whole ttree
        :param array_dict:
        :return:
        """
        return len(next(iter(array_dict.values())))

    def count(self):
        """
        :return:
        """
        return self._count_for_array_dict(self.arrays)

    @property
    def columns(self):
        return list(self.arrays.keys())

    def _type_for_col(self, col_name):
        """
        Interogate the dictionary and extract the dtype for the given column
        regardless of its underlying implementation
        :param col_name:
        :return:
        """
        col = self.arrays[col_name]

        if isinstance(col, np.ndarray):
            return col.dtype.name
        elif isinstance(col, awk.JaggedArray):
            return col.content.dtype.name
        else:
            return "???"

    @property
    def columns_with_types(self):
        return [(col_name, self._type_for_col(col_name)) for col_name in
                self.columns]

    def show(self):
        pass

    def repartition(self, num_partitions):
        pass

    def select_columns(self, columns):
        pass

    def execute_udf(self, user_func):
        return user_func.function(self.arrays)
