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

from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import DoubleType

from analysis.nanoaod_columnar_analysis import NanoAODColumnarAnalysis
from demo.zpeak.zpeak_analysis import ZpeakAnalysis
from irishep.app import App
from irishep.config import Config
from irishep.datasets.files_dataset_manager import FilesDatasetManager

config = Config(
    dataset_manager=FilesDatasetManager(database_file="../demo_datasets.csv")
)
app = App(config=config)

dataset = app.read_dataset("DY Jets")

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

my_analysis = ZpeakAnalysis(app)
analysis = NanoAODColumnarAnalysis(my_analysis)

f = analysis.generate_udf(slim, ["Electron", "Muon"],
                          "pd.Series(np.ones(Electron_pt.size))")

zpeak_udf = pandas_udf(f, DoubleType(), PandasUDFType.SCALAR)

# The Describe operation forces the DAG to execute
slim.dataframe.select(
    zpeak_udf(*slim.udf_arguments(["Electron", "Muon"]))).describe()

print(my_analysis.accumulators["zMass"].accumulator.value.values())
