from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.accumulators import AccumulatorParam
from pyspark.shell import spark

import awkward as awk
import numpy as np
import pandas as pd
import re

from pyspark.sql.types import IntegerType

import aghast.connect.numpy as connect_numpy


class NumpyVectorAccumulatorParam(AccumulatorParam):
    def zero(self, value):
        return value

    def addInPlace(self, val1, val2):
        val1 += val2
        return val1

spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.fallback.enabled", "false")
df=spark.read.format("org.dianahep.sparkroot").option("tree", "Events").load("hdfs:/coffea/DYJetsToLL_M-50_HT-100to200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8.root")
slim = df.select(df.event,
                 df.nElectron,
                 df.Electron_pt,
                 df.Electron_eta,
                 df.Electron_phi,
                 df.Electron_mass,
                 df.Electron_cutBased,
                 df.Electron_pdgId,
                 df.Electron_pfRelIso03_all,
                 df.Muon_pt,
                 df.Muon_eta,
                 df.Muon_phi,
                 df.Muon_mass,
                 df.Muon_tightId.cast("array<int >"),
                 df.Muon_pdgId,
                 df.Muon_pfRelIso04_all
                 )


BINS = 10
HIST_RANGE = (0.05, 0.2)

zeroHist = np.histogram([], bins=BINS, range=HIST_RANGE)

numpyvec_accum2 = spark.sparkContext.accumulator(
    connect_numpy.fromnumpy(zeroHist),
    NumpyVectorAccumulatorParam())


def foo(Electron_pt):
    return pd.Series(np.zeros(Electron_pt.size))

def compute_zpeak(Electron_pt,
                  Electron_eta,
                  Electron_phi,
                  Electron_mass,
                  Electron_cutBased,
                  Electron_pdgId,
                  Electron_pfRelIso03_all,
                  Muon_pt,
                  Muon_eta,
                  Muon_phi,
                  Muon_mass,
                  Muon_tightId,
                  Muon_pdgId,
                  Muon_pfRelIso04_all):
    global numpyvec_accum2, BINS, HIST_RANGE

    electrons = awk.JaggedArray.zip(
        {"pt": awk.JaggedArray.fromiter(Electron_pt),
         "eta": awk.JaggedArray.fromiter(Electron_eta),
         "phi": awk.JaggedArray.fromiter(Electron_phi),
         "mass": awk.JaggedArray.fromiter(Electron_mass),
         "cutBased": awk.JaggedArray.fromiter(Electron_cutBased),
         "pdgId": awk.JaggedArray.fromiter(Electron_pdgId),
         "pfRelIso03_all": awk.JaggedArray.fromiter(Electron_pfRelIso03_all)
    })

    ele = electrons[(electrons["pt"] > 20) &
                    (np.abs(electrons["eta"]) < 2.5) &
                    (electrons["cutBased"] >= 4)]

    muons = awk.JaggedArray.zip(
        {
            "pt": awk.JaggedArray.fromiter(Muon_pt),
            "eta": awk.JaggedArray.fromiter(Muon_eta),
            "phi": awk.JaggedArray.fromiter(Muon_phi),
            "mass": awk.JaggedArray.fromiter(Muon_mass),
            "tightId": awk.JaggedArray.fromiter(Muon_tightId),
            "pdgId": awk.JaggedArray.fromiter(Muon_pdgId),
            "pfRelIso04_all": awk.JaggedArray.fromiter(Muon_pfRelIso04_all)
        })

    mu = muons[(muons["pt"] > 20) &
               (np.abs(muons["eta"]) < 2.4) &
               (muons["tightId"] > 0)]

    ee = ele.distincts()
    mm = mu.distincts()
    em = ele.cross(mu)

    dileptons = {}
    dileptons['ee'] = ee[
        (ee.i0["pdgId"] * ee.i1["pdgId"] == -11 * 11) & (ee.i0["pt"] > 25)]
    dileptons['mm'] = mm[(mm.i0["pdgId"] * mm.i1["pdgId"] == -13 * 13)]
    dileptons['em'] = em[(em.i0["pdgId"] * em.i1["pdgId"] == -11 * 13)]

    channels = {}
    channels['ee'] = (ee.counts == 1) & (mu.counts == 0)
    channels['mm'] = (mm.counts == 1) & (ele.counts == 0)
    channels['em'] = (em.counts == 1) & (ele.counts == 1) & (
        mu.counts == 1)

    dupe = np.zeros(Muon_pt.size, dtype=bool)
    tot = 0

    isRealData = True
    channel = 'ee'
    cut = channels[channel]
    zcands = dileptons[channel][cut]
    dupe |= cut
    tot += cut.sum()
    weight = np.array(1.)
    zMass = np.histogram(zcands.i0["mass"].flatten(), bins=BINS, range=HIST_RANGE)

    numpyvec_accum2.add(connect_numpy.fromnumpy(zMass))
    return pd.Series(np.zeros(Electron_pt.size))

# foo_udf = pandas_udf(foo, IntegerType(), PandasUDFType.SCALAR)
# slim.select(foo_udf("Electron_pt")).describe()
#
zpeak_udf = pandas_udf(compute_zpeak, IntegerType(), PandasUDFType.SCALAR)
numpyret2 = slim.select(zpeak_udf(
    "Electron_pt",
    "Electron_eta",
    "Electron_phi",
    "Electron_mass",
    "Electron_cutBased",
    "Electron_pdgId",
    "Electron_pfRelIso03_all",
    "Muon_pt",
    "Muon_eta",
    "Muon_phi",
    "Muon_mass",
    "Muon_tightId",
    "Muon_pdgId",
    "Muon_pfRelIso04_all"))

# This triggers the accumulators
numpyret2.describe()

print(numpyvec_accum2.value.dump())


