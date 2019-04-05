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
import unittest
from unittest.mock import Mock, MagicMock

from jinja2 import Environment, Template

from irishep.analysis.nanoaod_columnar_analysis import NanoAODColumnarAnalysis
from irishep.analysis.user_analysis import UserAnalysis
from irishep.datasets.dataset import Dataset


class TestNanoAODColumnarAnalysis(unittest.TestCase):
    def test_render(self):
        mock_dataset = Mock(Dataset)
        mock_dataset.columns = ["dataset", "run", "luminosityBlock", "event",
                                "nElectron,", "Electron_pt", "Electron_eta",
                                "nMuon", "Muon_pt", "Muon_eta"]

        mock_dataset.columns_for_physics_objects = Mock(
            return_value=["nElectron", "Electron_pt", "Electron_eta", "nMuon",
                          "Muon_pt", "Muon_eta"])
        mock_dataset.count_column_for_physics_object = Mock(
            side_effect=["nElectrons", "nMuons"])
        mock_dataset.udf_arguments = Mock(
            return_value=['dataset', 'nElectron', 'Electron_pt', 'Electron_eta',
                          'nMuon', 'Muon_pt', 'Muon_eta'])

        mock_user_analysis = Mock(UserAnalysis)

        mock_app = Mock()
        mock_app.executor = Mock()
        mock_app.executor.templates = {"nanoAOD": "mytemplate.py"}

        analysis = NanoAODColumnarAnalysis(mock_app, mock_user_analysis)
        analysis.env = MagicMock(Environment)
        mock_template = MagicMock(Template)
        mock_template.render = Mock(return_value="def udf(): pass")
        analysis.env.get_template = Mock(return_value=mock_template)

        analysis.generate_udf(mock_dataset, ["Electron", "Muon"],
                              "Electron_pt")

        analysis.env.get_template.assert_called_with("mytemplate.py")

        mock_template.render.assert_called_with(
            cols=['dataset', 'nElectron', 'Electron_pt', 'Electron_eta',
                  'nMuon',
                  'Muon_pt', 'Muon_eta'],
            physics_objects={'Electron': [
                {'physics_obj_property': 'pt', 'col': 'Electron_pt'},
                {'physics_obj_property': 'eta', 'col': 'Electron_eta'}
            ],
                'Muon': [
                    {'physics_obj_property': 'pt', 'col': 'Muon_pt'},
                    {'physics_obj_property': 'eta', 'col': 'Muon_eta'}
                ]},
            counts={'Electron': 'nElectrons', 'Muon': 'nMuons'},
            return_expr='Electron_pt')

    def test_render_no_template(self):
        mock_app = Mock()
        mock_app.executor = Mock()
        mock_app.executor.templates = {"notNanoAOD": "mytemplate.py"}
        mock_user_analysis = MagicMock(UserAnalysis)

        analysis = NanoAODColumnarAnalysis(mock_app, mock_user_analysis)
        mock_dataset = MagicMock(Dataset)

        self.assertRaises(
            ValueError,
            analysis.generate_udf, mock_dataset, ["Electron", "Muon"],
            "Electron_pt")
