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


from pyspark.sql import SparkSession

from irishep.datasets.dataset import Dataset


class App:
    def __init__(self, config):
        self.spark = SparkSession.builder \
            .master(config.master) \
            .appName(config.app_name) \
            .config("spark.jars.packages",
                    "org.diana-hep:spark-root_2.11:0.1.15") \
            .getOrCreate()

        self.dataset_manager = config.dataset_manager

    @property
    def datasets(self):
        """
        Fetch an initialized dataset manager instance
        :return: the dataset manager instance
        """
        if not self.dataset_manager.provisioned:
            self.dataset_manager.provision(self)
        return self.dataset_manager

    def read_dataset(self, dataset_name):
        """
        Creates a dataset from files on disk. For now assumes that the files are
        in ROOT format
        :param dataset_name: Name of the dataset to read. Gets filenames from
            the dataset_manager
        :return: A populated Dataset instance
        """
        files = self.datasets.get_file_list(dataset_name)
        print(files)

        result_df = None

        # Sparkroot can't handle list of files
        for file in files:
            file_df = self.spark.read.format("org.dianahep.sparkroot") \
                .option("tree", "Events") \
                .load(file)

            # So just append each file's datafrane into one big one
            result_df = file_df if not result_df else result_df.union(file_df)

        return Dataset(dataset_name, result_df)
