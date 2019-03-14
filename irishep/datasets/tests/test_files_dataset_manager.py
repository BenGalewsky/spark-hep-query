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
from unittest.mock import MagicMock, Mock

from pyspark.sql import DataFrame

from irishep.datasets.files_dataset_manager import FilesDatasetManager


class TestFilesDataset(unittest.TestCase):
    class DatasourceResult:
        def __init__(self, name, path="/tmp/foo.root"):
            self.name = name
            self.path = path

    def test_init_database(self):
        dsm = FilesDatasetManager("/foo/bar")
        self.assertFalse(dsm.provisioned)
        self.assertEqual(dsm.database_file, "/foo/bar")

        mock_app = MagicMock()
        mock_app.spark = MagicMock()
        mock_app.spark.read = MagicMock()
        mock_dataframe = MagicMock(DataFrame)
        mock_app.spark.read.csv = Mock(return_value=mock_dataframe)
        dsm.provision(mock_app)

        mock_app.spark.read.csv.assert_called_with("/foo/bar", header=True)
        self.assertTrue(dsm.provisioned)

    def test_get_names(self):
        dsm = FilesDatasetManager("/foo/bar")
        mock_dataframe = MagicMock(DataFrame)
        mock_dataframe.name = "Name"
        dsm.df = mock_dataframe
        mock_dataframe.select = Mock(return_value=mock_dataframe)
        mock_dataframe.distinct = Mock(return_value=mock_dataframe)

        result = [TestFilesDataset.DatasourceResult("a"),
                  TestFilesDataset.DatasourceResult("b")]
        mock_dataframe.collect = Mock(return_value=result)
        self.assertEqual(['a', 'b'], dsm.get_names())

    def test_get_files(self):
        dsm = FilesDatasetManager("/foo/bar")
        mock_dataframe = MagicMock(DataFrame)
        mock_dataframe.name = "Name"
        dsm.df = mock_dataframe
        mock_dataframe.select = Mock(return_value=mock_dataframe)
        mock_dataframe.filter = Mock(return_value=mock_dataframe)
        mock_dataframe.path = "path"
        mock_dataframe.name = "a"

        result = [TestFilesDataset.DatasourceResult("a", "/tmp/foo.root"),
                  TestFilesDataset.DatasourceResult("b", "/tmp/bar.root")]
        mock_dataframe.collect = Mock(return_value=result)

        files = dsm.get_file_list("a")
        self.assertEqual(['/tmp/foo.root', '/tmp/bar.root'], files)

        # Will test df.name == "a"
        mock_dataframe.filter.assert_called_with(True)
