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
import sys

from pyspark.sql.functions import PandasUDFType, pandas_udf
from pyspark.sql.types import DoubleType

from analysis.nanoaod_columnar_analysis import NanoAODColumnarAnalysis
from fnal_column_analysis_tools import lookup_tools

from irishep.executors.uproot_executor import UprootExecutor
from irishep.executors.spark_executor import SparkExecutor
from irishep.app import App
from irishep.config import Config
from irishep.datasets.inmemory_files_dataset_manager import \
    InMemoryFilesDatasetManager
from zpeak_analysis import ZpeakAnalysis

executor = UprootExecutor("zpeak")
# executor = SparkExecutor("local", "ZPeak", 20)

config = Config(
    executor = executor,
    dataset_manager=InMemoryFilesDatasetManager(database_file="demo_datasets.csv")
)
app = App(config=config)
print(app.datasets.get_names())
print(app.datasets.get_file_list("ZJetsToNuNu_HT-600To800_13TeV-madgraph"))


# Create a broadcast variable for the non-event data
weightsext = lookup_tools.extractor()
correctionDescriptions = open("newCorrectionFiles.txt").readlines()
weightsext.add_weight_sets(correctionDescriptions)
weightsext.finalize()
weights_eval = weightsext.make_evaluator()


dataset = app.read_dataset("DY Jets")
print(dataset.columns)
print(dataset.count())

slim = dataset.select_columns(["nElectron",
                               "Electron_pt",
                               "Electron_eta",
                               "Electron_phi",
                               "Electron_mass",
                               "Electron_cutBased",
                               "Electron_pdgId",
                               "Electron_pfRelIso03_all",
                               "nMuon",
                               "Muon_pt",
                               "Muon_eta",
                               "Muon_phi",
                               "Muon_mass",
                               "Muon_tightId",
                               "Muon_pdgId",
                               "Muon_pfRelIso04_all"])

print(slim.count())
print(slim.columns)
print(slim.columns_with_types)

my_analysis = ZpeakAnalysis(app, weights_eval)

analysis = NanoAODColumnarAnalysis(app, my_analysis)

f = analysis.generate_udf(slim, ["Electron", "Muon"],
                          "1.0")

print(slim.execute_udf(f))
print(my_analysis.accumulators["zMass"].accumulator.value.values())
