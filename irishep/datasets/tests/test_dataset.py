import unittest
from unittest.mock import Mock

import pyspark.sql

from irishep.datasets.dataset import Dataset


class TestDataset(unittest.TestCase):
    def test_constuctor(self):
        mock_dataframe = Mock(pyspark.sql.DataFrame)
        a_dataset = Dataset("my dataset", mock_dataframe)
        self.assertEqual(a_dataset.name, "my dataset")
        self.assertEqual(a_dataset.dataframe, mock_dataframe)

    def test_count(self):
        mock_dataframe = Mock(pyspark.sql.DataFrame)
        mock_dataframe.count = Mock(return_value=42)
        a_dataset = Dataset("my dataset", mock_dataframe)
        count = a_dataset.count()
        self.assertEqual(42, count)
        mock_dataframe.count.assert_called_once()


if __name__ == '__main__':
    unittest.main()
