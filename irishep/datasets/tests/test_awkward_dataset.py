import unittest
from unittest.mock import Mock

import awkward as awk
import numpy as np

from irishep.datasets.awkward_dataset import AwkwardDataset


class TestAwkwardDataset(unittest.TestCase):
    def test_init(self):
        array_dict = {b'Electron': np.zeros((3,))}
        d = AwkwardDataset("foo", array_dict)

        self.assertEqual("foo", d.name)
        self.assertTrue("dataset" in d.columns)
        self.assertTrue("Electron" in d.arrays.keys())

    def test_init_with_existing_dataset(self):
        array_dict = {b'Electron': np.zeros((3,)), b"dataset": ["a"]}
        d = AwkwardDataset("foo", array_dict)
        self.assertTrue("dataset" in d.columns)

    def test_count(self):
        array_dict = {b'Electron': np.zeros((3,))}
        d = AwkwardDataset("foo", array_dict)
        self.assertEqual(3, d.count())

    def test_columns(self):
        array_dict = {
            b"nElectron": np.zeros((3,)),
            b"Electron_pt": np.zeros((3,)),
            b"Electron_eta": np.zeros((3,)),
            b"Electron_phi": np.zeros((3,))
        }
        d = AwkwardDataset("foo", array_dict)
        self.assertTrue(sorted(["dataset",
                                "nElectron",
                                "Electron_pt",
                                "Electron_eta",
                                "Electron_phi",
                                ]), sorted(d.columns))

    def test_columns_with_types(self):
        array_dict = {
            b"nElectron": np.zeros((3,), dtype="float64"),
            b"Electron_pt": np.zeros((3,), dtype='int16'),
            b"Electron_eta": awk.JaggedArray.fromiter([[1, 2, 3], [1.0, 3.14]]),
            b"Electron_phi": [1, 2, 3]
        }

        d = AwkwardDataset("foo", array_dict)
        array_dict = {col_name: dtype for (col_name, dtype) in
                      d.columns_with_types}
        self.assertEqual("float64", array_dict["nElectron"])
        self.assertEqual("int16", array_dict["Electron_pt"])
        self.assertEqual("???", array_dict["Electron_phi"])
        self.assertEqual("float64", array_dict["Electron_eta"])

    @staticmethod
    def _create_test_dataframe():
        return AwkwardDataset("foo", {b'Electron': np.zeros((3,))})

    def test_show(self):
        self._create_test_dataframe().show()

    def test_repartition(self):
        self._create_test_dataframe().repartition(42)

    def test_select_columns(self):
        self._create_test_dataframe().select_columns([])

    def test_execute_udf(self):
        udf = Mock()
        udf.function = Mock()

        df = self._create_test_dataframe()
        df.execute_udf(udf)
        udf.function.assert_called_with(df.arrays)


if __name__ == '__main__':
    unittest.main()
