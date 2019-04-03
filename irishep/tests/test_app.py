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

import pyspark.sql
from pyspark.sql import SparkSession

from irishep.executors.executor import Executor
from irishep.config import Config
from irishep.datasets.dataset_manager import DatasetManager
from irishep.app import App


class TestApp(unittest.TestCase):
    def test_app_create(self):
        builder = pyspark.sql.session.SparkSession.Builder()
        mock_session = MagicMock(SparkSession)

        builder.master = Mock(return_value=builder)
        builder.appName = Mock(return_value=builder)
        builder.getOrCreate = Mock(return_value=mock_session)

        mock_dataset_manager = MagicMock(DatasetManager)
        mock_executor = MagicMock(Executor)

        a = App(Config(
            executor=mock_executor,
            dataset_manager=mock_dataset_manager
        ))

        assert a
        self.assertEqual(a.dataset_manager, mock_dataset_manager)
        self.assertEqual(a.executor, mock_executor)

    def test_provisioned_dataset_manager(self):
        mock_datasource_manager = Mock(DatasetManager)
        mock_executor = MagicMock(Executor)

        mock_datasource_manager.provisioned = True
        a = App(Config(
            executor=mock_executor,
            dataset_manager=mock_datasource_manager
        ))
        self.assertTrue(a.datasets)

    def test_unprovisioned_dataset_manager(self):
        mock_datasource_manager = Mock(DatasetManager)
        mock_datasource_manager.provision = Mock()

        mock_executor = MagicMock(Executor)

        mock_datasource_manager.provisioned = False
        a = App(Config(
            executor=mock_executor,
            dataset_manager=mock_datasource_manager
        ))

        self.assertTrue(a.datasets)
        mock_datasource_manager.provision.assert_called_once()

    def test_read_dataset(self):
        # Create a datasource manager that returns our two files
        mock_datasource_manager = Mock(DatasetManager)
        mock_datasource_manager.provisioned = True
        mock_datasource_manager.get_file_list = Mock(
            return_value=["/tmp/foo.root", "/tmp/bar.root"])

        mock_dataset = Mock()
        mock_executor = Mock(Executor)
        mock_executor.read_files = Mock(return_value=mock_dataset)

        a = App(Config(
            executor=mock_executor,
            num_partitions=42,
            dataset_manager=mock_datasource_manager))

        rslt = a.read_dataset("mydataset")
        mock_datasource_manager.get_file_list.assert_called_with("mydataset")
        mock_executor.read_files.assert_called_with("mydataset",
                                                    ["/tmp/foo.root",
                                                     "/tmp/bar.root"])
        self.assertEqual(rslt, mock_dataset)
