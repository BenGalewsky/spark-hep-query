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
from unittest.mock import patch, Mock, MagicMock

import pyspark.sql
from pyspark.sql import SparkSession

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

        with patch('pyspark.sql.SparkSession.builder', new=builder):
            a = App(Config(
                app_name="foo",
                master="spark-master",
                dataset_manager=mock_dataset_manager
            ))

            assert a
            builder.master.assert_called_with("spark-master")
            builder.appName.assert_called_with("foo")
            builder.getOrCreate.assert_called_once()
            self.assertEqual(a.dataset_manager, mock_dataset_manager)

    def _construct_app(self, config):
        builder = pyspark.sql.session.SparkSession.Builder()
        mock_session = MagicMock(SparkSession)
        builder.getOrCreate = Mock(return_value=mock_session)
        with patch('pyspark.sql.SparkSession.builder', new=builder):
            return App(config)

    def test_provisioned_dataset_manager(self):
        builder = pyspark.sql.session.SparkSession.Builder()
        mock_session = MagicMock(SparkSession)
        builder.getOrCreate = Mock(return_value=mock_session)

        mock_datasource_manager = Mock(DatasetManager)
        mock_datasource_manager.provisioned = True
        a = self._construct_app(Config(dataset_manager=mock_datasource_manager))
        self.assertTrue(a.datasets)

    def test_unprovisioned_dataset_manager(self):
        builder = pyspark.sql.session.SparkSession.Builder()
        mock_session = MagicMock(SparkSession)
        builder.getOrCreate = Mock(return_value=mock_session)

        mock_datasource_manager = Mock(DatasetManager)
        mock_datasource_manager.provisioned = False
        mock_datasource_manager.provision = Mock()
        a = self._construct_app(Config(dataset_manager=mock_datasource_manager))
        self.assertTrue(a.datasets)
        mock_datasource_manager.provision.assert_called_once()
