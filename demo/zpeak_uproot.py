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
import uproot
import os
import numpy as np
from fnal_column_analysis_tools import hist

from fnal_column_analysis_tools.analysis_objects.JaggedCandidateArray import \
    JaggedCandidateArray

BINS = 10
HIST_RANGE = (0.05, 0.2)

dataset_axis=hist.Cat("dataset", "DAS name")
channel_cat_axis=hist.Cat("channel", "dilepton flavor")

file = uproot.open(os.path.join("..","..","data","DYJetsToLL_M-50_HT-100to200_TuneCUETP8M1_13TeV-madgraphMLM-pythia8.root"))
events = file["Events"]

arrays = events.arrays(["nElectron",
                   "Electron_pt",
                   "Electron_eta",
                   "Electron_phi",
                   "Electron_mass",
                   "Electron_cutBased",
                   "Electron_pdgId",
                   "Electron_pfRelIso03_all"])

physics_objects = {}

physics_objects["Electron"] = \
    JaggedCandidateArray.candidatesfromcounts(arrays[b'nElectron'],
                                              pt=arrays[b"Electron_pt"].content,
                                              eta=arrays[b"Electron_eta"].content,
                                              phi=arrays[b"Electron_phi"].content,
                                              mass=arrays[b"Electron_mass"].content,
                                              cutBased=arrays[
                                                  b"Electron_cutBased"].content,
                                              pdgId=arrays[b"Electron_pdgId"].content,
                                              pfRelIso03_all=arrays[
                                                  b"Electron_pfRelIso03_all"].content)

electrons = physics_objects["Electron"]

print(electrons.pt.size)
print("---->",electrons[(electrons.pt > 20)])

sys.exit(0)

e_counts = ele_arrays.pop(b'nElectron')
ele_arrays = {'_'.join(key.decode().split('_')[1:]): array.content for key , array in ele_arrays.items()}

electrons = JaggedCandidateArray.candidatesfromcounts(e_counts, **ele_arrays)

muon_arrays = events.arrays([
    "nMuon",
    "Muon_pt",
    "Muon_eta",
    "Muon_phi",
    "Muon_mass",
    "Muon_tightId",
    "Muon_pdgId",
    "Muon_pfRelIso04_all"])

m_counts = muon_arrays.pop(b'nMuon')
muon_arrays = {'_'.join(key.decode().split('_')[1:]): array.content for key , array in muon_arrays.items()}
muons = JaggedCandidateArray.candidatesfromcounts(m_counts, **muon_arrays)

ele = electrons[(electrons.pt > 20) &
                (np.abs(electrons.eta) < 2.5) &
                (electrons.cutBased >= 4)]

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

dupe = np.zeros(events.array("Muon_pt").size, dtype=bool)
tot = 0

isRealData = True
for channel, cut in channels.items():
    zcands = dileptons[channel][cut]
    # dupe |= cut
    tot += cut.sum()
    weight = np.array(1.)
    zMass = hist.Hist("Events", dataset_axis,
                      channel_cat_axis,
                      hist.Bin("mass", "$m_{\ell\ell}$ [GeV]",
                               120, 0, 120),
                      )

    zMass.fill(dataset="uproot", channel=channel,
               mass=zcands.mass.flatten(),
               weight=weight.flatten())

    print(zMass.values())

