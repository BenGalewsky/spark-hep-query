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
from irishep.datasets.dataset_manager import DatasetManager
import csv


class InMemoryFilesDatasetManager(DatasetManager):
    """
    Dataset manager that is backed by a csv file. The file must contain entries
    for each file associated with datasets.

    The format of the file is:
    name, path
    Where "name" is the dataset name, and "path" is the path to the underlying
    file. Datasets can be made up of multiple files. This is represented by
    mulitple entries sharing the same name.
    """

    def __init__(self, database_file):
        self.database_file = database_file
        self.provisioned = False
        self.database = None

    def provision(self, app):
        """
        Provision the manager after the app has been set up by reading the csv
        file and storing the resulting dataframe
        :param app: The initialized Query Service App
        :return: None
        """
        with open(self.database_file) as f:
            reader = csv.reader(f, skipinitialspace=True)
            header = next(reader)
            self.database = [dict(zip(header, map(str, row))) for row in reader]

        self.provisioned = True

    def get_names(self):
        """
        return the names of the datasets in the database
        :return: list of dataset names
        """
        return list(set([x["name"] for x in self.database]))

    def get_file_list(self, dataset_name):
        """
        Get paths to the files that make up the given dataset
        :param dataset_name:
        :return: List of paths
        """
        return [rec["path"] for rec in self.database if
                rec["name"] == dataset_name]
