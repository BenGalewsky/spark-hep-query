from pyspark.sql import functions
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.accumulators import AccumulatorParam
from pyspark.shell import spark
from collections import OrderedDict
import time

import awkward as awk
import numpy as np
import pandas as pd
import re
import fnal_column_analysis_tools.hist as hist

from pyspark.sql.types import IntegerType, DoubleType

from fnal_column_analysis_tools.analysis_objects import JaggedCandidateArray


class FLNAL_Hist_AccumulatorParam(AccumulatorParam):
    def zero(self, value):
        return value

    def addInPlace(self, val1, val2):
        val1 += val2
        return val1


hists = OrderedDict()

dataset_axis = hist.Cat("dataset", "DAS name")
channel_cat_axis = hist.Cat("channel", "dilepton flavor")

# underflow = negative weight sum, overflow = positive weight sum
hists['genw'] = hist.Hist("Events", dataset_axis, hist.Bin("genw", "Gen weight", [0.]))

hists['lepton_pt'] = hist.Hist("Events", dataset_axis, channel_cat_axis,
                               hist.Bin("lep0_pt", "Leading lepton $p_{T}$ [GeV]", 50, 0, 500),
                               hist.Bin("lep1_pt", "Trailing lepton $p_{T}$ [GeV]", 50, 0, 500),
                               )
hists['zMass'] = spark.sparkContext.accumulator(
    hist.Hist("Events", dataset_axis, channel_cat_axis,
              hist.Bin("mass", "$m_{\ell\ell}$ [GeV]", 120, 0, 120),
              ),
    FLNAL_Hist_AccumulatorParam()
)

hists['profile'] = hist.Hist("RowGroups",
                    hist.Cat("op", "Operation", sorting='placement'),
                    hist.Bin("dt", "$\Delta t$ [$\mu s$]", 100, 0, 10),
                   )



class NumpyVectorAccumulatorParam(AccumulatorParam):
    def zero(self, value):
        return value

    def addInPlace(self, val1, val2):
        val1 += val2
        return val1


spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.fallback.enabled", "false")
df = spark.read.parquet("file:/Users/ncsmith/src/spark-hep-query/demo/bignano.parquet")
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
    global hist, dataset_axis, channel_cat_axis
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

        zMassHist = hists["zMass"]

        zMass = hist.Hist("Events", dataset_axis, channel_cat_axis,
                          hist.Bin("mass", "$m_{\ell\ell}$ [GeV]", 120, 0, 120),
                          )

        zMass.fill(dataset=dataset[0], channel=channel,
                   mass=zcands.mass.flatten(),
                   weight=weight.flatten())
        zMassHist.add(zMass)


    dt = time.time() - tic
    return pd.Series(np.ones(Electron_pt.size) * dt/Electron_pt.size)

# foo_udf = pandas_udf(foo, IntegerType(), PandasUDFType.SCALAR)
# slim.select(foo_udf("Electron_pt")).describe()
#

def foo(dataset,
        nElectron,
        Electron_pt,
        Electron_eta,
        Electron_phi,
        Electron_mass,
        Electron_cutBased,
        Electron_pdgId,
        Electron_pfRelIso03_all):
    import sys
    from fnal_column_analysis_tools.analysis_objects import JaggedCandidateArray

    almost_electrons = awk.JaggedArray.zip(
        {"pt": awk.JaggedArray.fromiter(Electron_pt),
         "eta": awk.JaggedArray.fromiter(Electron_eta),
         "phi": awk.JaggedArray.fromiter(Electron_phi),
         "mass": awk.JaggedArray.fromiter(Electron_mass),
         "cutBased": awk.JaggedArray.fromiter(Electron_cutBased),
         "pdgId": awk.JaggedArray.fromiter(Electron_pdgId),
         "pfRelIso03_all": awk.JaggedArray.fromiter(Electron_pfRelIso03_all)
    })
    electrons = JaggedCandidateArray.candidatesfromoffsets(almost_electrons.offsets,
                                                           pt=almost_electrons["pt"].content,
                                                           eta=almost_electrons["eta"].content,
                                                           phi=almost_electrons["phi"].content,
                                                           mass=almost_electrons["mass"].content,
                                                           cutBased=almost_electrons["cutBased"].content,
                                                           pdgId=almost_electrons["pdgId"].content,
                                                           pfRelIso03_all=almost_electrons["pfRelIso03_all"].content
                                                           )
    return nElectron


foo_udf = pandas_udf(foo, DoubleType(), PandasUDFType.SCALAR)

slim.select(foo_udf("dataset", "nElectron",     "Electron_pt",
    "Electron_eta",
    "Electron_phi",
    "Electron_mass",
    "Electron_cutBased",
    "Electron_pdgId",
    "Electron_pfRelIso03_all")).describe()


zpeak_udf = pandas_udf(compute_zpeak, DoubleType(), PandasUDFType.SCALAR)
numpyret2 = slim.select(zpeak_udf(
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
print(numpyret2.describe().toPandas())

print(hists["zMass"].value.values())

#slim.write.parquet("slim")

