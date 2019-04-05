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
from unittest.mock import MagicMock
from unittest.mock import patch

from irishep.datasets.inmemory_files_dataset_manager import \
    InMemoryFilesDatasetManager


class TestInmemoryFilesDataset(unittest.TestCase):
    @staticmethod
    def _mock_open(*args, **kargs):
        f_open = unittest.mock.mock_open(*args, **kargs)
        f_open.return_value.__iter__ = lambda vals: iter(vals.readline, '')
        return f_open

    def _init_database(self, dsm, import_db):
        mock_app = MagicMock()

        with patch("builtins.open",
                   self._mock_open(read_data=import_db)):
            dsm.provision(mock_app)

    def test_init_database(self):
        dsm = InMemoryFilesDatasetManager("/foo/bar")
        self.assertFalse(dsm.provisioned)
        self.assertEqual(dsm.database_file, "/foo/bar")
        mock_app = MagicMock()

        with patch("builtins.open", self._mock_open(
                read_data="name,path\nfoo,/tmp/bar\nbaz,/usr/bob")) \
                as mock_datafile:
            dsm.provision(mock_app)
            mock_datafile.assert_called_with("/foo/bar")

            self.assertEqual(2, len(dsm.database))
            self.assertEqual(dsm.database[0]["name"], "foo")
            self.assertEqual(dsm.database[0]["path"], "/tmp/bar")
            self.assertEqual(dsm.database[1]["name"], "baz")
            self.assertEqual(dsm.database[1]["path"], "/usr/bob")

            self.assertTrue(dsm.provisioned)

    def test_get_names(self):
        dsm = InMemoryFilesDatasetManager("/foo/bar")
        self._init_database(dsm, "\n".join(
            ["name,path", "foo,/tmp/bar", "baz,/usr/bob", "baz,/usr/bob2"]))

        self.assertEqual(sorted(['foo', 'baz']), sorted(dsm.get_names()))

    def test_get_files(self):
        dsm = InMemoryFilesDatasetManager("/foo/bar")
        self._init_database(dsm, "\n".join(
            ["name,path", "foo,/tmp/bar", "baz,/usr/bob", "baz,/usr/bob2"]))

        self.assertEqual(['/tmp/bar'], dsm.get_file_list('foo'))
        self.assertEqual(['/usr/bob', '/usr/bob2'], dsm.get_file_list('baz'))
