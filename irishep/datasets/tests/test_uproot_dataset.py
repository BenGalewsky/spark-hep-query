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
from unittest.mock import Mock, patch

from irishep.datasets.awkward_dataset import AwkwardDataset
from irishep.datasets.uproot_dataset import UprootDataset


class TestUprootDataset(unittest.TestCase):
    def test_init(self):
        ttree = Mock()
        d = UprootDataset("foo", ttree)
        self.assertEqual(d.name, "foo")
        self.assertEqual(d.ttree, ttree)

    def test_count(self):
        ttree = [1, 2, 3]
        d = UprootDataset("foo", ttree)
        self.assertEqual(3, d.count())

    def test_columns(self):
        ttree = {b'a': 1, b'b': 2, b'c': 3}
        d = UprootDataset("foo", ttree)
        cols = d.columns
        self.assertEqual(["a", "b", "c"], cols)

    def test_select_colmns(self):
        ttree = Mock()
        ttree.arrays = Mock(return_value=[1, 2, 3])
        with patch("irishep.datasets.awkward_dataset.AwkwardDataset.__init__",
                   return_value=None) as awk_dataset:
            d = UprootDataset("foo", ttree)
            d2 = d.select_columns(["a", "b", "c"])
            awk_dataset.assert_called_with("foo", [1, 2, 3])
            self.assertIsInstance(d2, AwkwardDataset)

    def test_columns_with_types(self):
        d = UprootDataset("foo", [1, 2, 3])

        with self.assertRaises(NotImplementedError):
            # noinspection PyStatementEffect
            d.columns_with_types

    def test_show(self):
        d = UprootDataset("foo", [1, 2, 3])

        with self.assertRaises(NotImplementedError):
            d.show()

    def test_repartition(self):
        d = UprootDataset("foo", [1, 2, 3])

        with self.assertRaises(NotImplementedError):
            d.repartition(43)

    def test_execute_udf(self):
        d = UprootDataset("foo", [1, 2, 3])

        with self.assertRaises(NotImplementedError):
            d.execute_udf(43)
