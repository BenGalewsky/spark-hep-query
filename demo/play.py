import time
from collections import OrderedDict
import sys

import numpy as np
import pandas as pd
from pyspark.accumulators import AccumulatorParam
from pyspark.shell import spark
from pyspark.sql import functions
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import DoubleType

import fnal_column_analysis_tools.hist as hist
from fnal_column_analysis_tools.analysis_objects import JaggedCandidateArray


if len(sys.argv) != 2:
    sys.stderr.write("Please provide a path to your root file")
    sys.exit(-1)
else:
    root_file_name = sys.argv[1]
    print("Analyzing "+root_file_name)

# Implement accumulatorparam class for FNAL Hists
class FLNAL_Hist_AccumulatorParam(AccumulatorParam):
    def zero(self, value):
        return value

    def addInPlace(self, val1, val2):
        val1 += val2
        return val1


# Create a histogram accumulator for ZMass
hists = OrderedDict()

dataset_axis = hist.Cat("dataset", "DAS name")
channel_cat_axis = hist.Cat("channel", "dilepton flavor")

hists['zMass'] = {
    "accumulator":
        spark.sparkContext.accumulator(
            hist.Hist("Events", dataset_axis, channel_cat_axis,
                      hist.Bin("mass", "$m_{\ell\ell}$ [GeV]", 120, 0, 120),
                      ),
            FLNAL_Hist_AccumulatorParam()
        ),
    "dataset_axis": hist.Cat("dataset", "DAS name"),
    "channel_cat_axis": hist.Cat("channel", "dilepton flavor")
}


# ZPeak UDF that reduces data to a zMass histogram
def compute_zpeak(dataset,
                  nElectron, 
                  Electron_pt,
                  Electron_eta,
                  Electron_phi,
                  Electron_mass,
                  Electron_cutBased,
                  Electron_pdgId,
                  Electron_pfRelIso03_all,
                  nMuon,
                  Muon_pt,
                  Muon_eta,
                  Muon_phi,
                  Muon_mass,
                  Muon_tightId,
                  Muon_pdgId,
                  Muon_pfRelIso04_all):

    global hists
    tic = time.time()

    electrons = JaggedCandidateArray.candidatesfromcounts(
            nElectron.array,
            pt=Electron_pt.array[0].base,
            eta=Electron_eta.array[0].base,
            phi=Electron_phi.array[0].base,
            mass=Electron_mass.array[0].base,
            cutBased=Electron_cutBased.array[0].base,
            pdgId=Electron_pdgId.array[0].base,
            pfRelIso03_all=Electron_pfRelIso03_all.array[0].base,
        )

    ele = electrons[(electrons.pt > 20) &
                    (np.abs(electrons.eta) < 2.5) &
                    (electrons.cutBased >= 4)]


    muons = JaggedCandidateArray.candidatesfromcounts(
            nMuon.values,
            pt=Muon_pt.array[0].base,
            eta=Muon_eta.array[0].base,
            phi=Muon_phi.array[0].base,
            mass=Muon_mass.array[0].base,
            tightId=Muon_tightId.array[0].base,
            pdgId=Muon_pdgId.array[0].base,
            pfRelIso04_all=Muon_pfRelIso04_all.array[0].base,
        )

    mu = muons[(muons.pt > 20) &
               (np.abs(muons.eta) < 2.4) &
               (muons.tightId > 0)]

    ee = ele.distincts()
    mm = mu.distincts()
    em = ele.cross(mu)

    dileptons = {}
    dileptons['ee'] = ee[
        (ee.i0.pdgId * ee.i1.pdgId == -11 * 11) & (ee.i0.pt > 25)]
    dileptons['mm'] = mm[(mm.i0.pdgId * mm.i1.pdgId == -13 * 13)]
    dileptons['em'] = em[(em.i0.pdgId * em.i1.pdgId == -11 * 13)]

    channels = {}
    channels['ee'] = (ee.counts == 1) & (mu.counts == 0)
    channels['mm'] = (mm.counts == 1) & (ele.counts == 0)
    channels['em'] = (em.counts == 1) & (ele.counts == 1) & (
        mu.counts == 1)

    dupe = np.zeros(Muon_pt.size, dtype=bool)
    tot = 0

    isRealData = True
    for channel, cut in channels.items():
        zcands = dileptons[channel][cut]
        dupe |= cut
        tot += cut.sum()
        weight = np.array(1.)

        zMassHist = hists["zMass"]["accumulator"]

        zMass = hist.Hist("Events", hists["zMass"]["dataset_axis"],
                          hists["zMass"]["channel_cat_axis"],
                          hist.Bin("mass", "$m_{\ell\ell}$ [GeV]", 120, 0, 120),
                          )

        zMass.fill(dataset=dataset[0], channel=channel,
                   mass=zcands.mass.flatten(),
                   weight=weight.flatten())
        zMassHist.add(zMass)


    dt = time.time() - tic
    return pd.Series(np.ones(Electron_pt.size) * dt/Electron_pt.size)

zpeak_udf = pandas_udf(compute_zpeak, DoubleType(), PandasUDFType.SCALAR)

spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.fallback.enabled", "false")
df = spark.read.parquet("file:"+root_file_name)
df = df.withColumn("dataset", functions.lit("my_dataset"))
slim = df.select(df.event,
                 df.nElectron,
                 df.Electron_pt,
                 df.Electron_eta,
                 df.Electron_phi,
                 df.Electron_mass,
                 df.Electron_cutBased,
                 df.Electron_pdgId,
                 df.Electron_pfRelIso03_all,
                 df.nMuon,
                 df.Muon_pt,
                 df.Muon_eta,
                 df.Muon_phi,
                 df.Muon_mass,
                 df.Muon_tightId.cast("array<int >"),
                 df.Muon_pdgId,
                 df.Muon_pfRelIso04_all,
                 df.dataset
                 )

analysis = slim.select(zpeak_udf(
    "dataset",
    "nElectron",
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
    "Muon_pfRelIso04_all"))

# This triggers the accumulators
print(analysis.describe().toPandas())

print(hists["zMass"]["accumulator"].value.values())
