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
import numpy as np

from analysis.fnal_hist_accumulator import FnalHistAccumulator
from analysis.user_analysis import UserAnalysis
import fnal_column_analysis_tools.hist as hist


class ZpeakAnalysis(UserAnalysis):
    def __init__(self, app):
        self.accumulators = {
            "zMass": FnalHistAccumulator(dataset_axis=hist.Cat("dataset", "DAS name"),
                                 channel_cat_axis=hist.Cat("channel",
                                                           "dilepton flavor"),
                                 spark_context=app.spark.sparkContext
                                 )}

    def calc(self, physics_objects, dataset_name):
        electrons = physics_objects["Electron"]
        ele = electrons[(electrons.pt > 20) &
                        (np.abs(electrons.eta) < 2.5) &
                        (electrons.cutBased >= 4)]

        muons = physics_objects["Muon"]
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

        # dupe = np.zeros(Muon_pt.size, dtype=bool)
        tot = 0

        isRealData = True
        for channel, cut in channels.items():
            zcands = dileptons[channel][cut]
            # dupe |= cut
            tot += cut.sum()
            weight = np.array(1.)

            zMassHist = self.accumulators["zMass"]
            zMass = hist.Hist("Events", zMassHist.dataset_axis,
                              zMassHist.channel_cat_axis,
                              hist.Bin("mass", "$m_{\ell\ell}$ [GeV]",
                                       120, 0, 120),
                              )

            zMass.fill(dataset=dataset_name, channel=channel,
                       mass=zcands.mass.flatten(),
                       weight=weight.flatten())

            zMassHist.accumulator.add(zMass)
