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

from irishep.datasets.spark_dataset import SparkDataset
from irishep.executors.executor import Executor


class SparkExecutor(Executor):
    def __init__(self, master, app_name, num_partitions):
        super().__init__(app_name)
        self.spark = SparkSession.builder \
            .master(master) \
            .appName(app_name) \
            .config("spark.jars.packages",
                    "org.diana-hep:spark-root_2.11:0.1.15") \
            .getOrCreate()
        self.num_partitions = num_partitions

    def read_files(self, dataset_name, files):
        result_df = None
        # Sparkroot can't handle list of files
        for file in files:
            file_df = self.spark.read.format("org.dianahep.sparkroot") \
                .option("tree", "Events") \
                .load(file)

            # So just append each file's datafrane into one big one
            result_df = file_df if not result_df else result_df.union(file_df)

        dataset = SparkDataset(dataset_name, result_df)
        dataset.repartition(self.num_partitions)

        return dataset

    def register_accumulator(self, initial_value, accumulator):
        return self.spark.sparkContext.accumulator(initial_value, accumulator)

    def register_broadcast_var(self, var):
        return self.spark.sparkContext.broadcast(var)
