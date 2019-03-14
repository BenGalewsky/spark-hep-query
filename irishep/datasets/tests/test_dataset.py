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

    def test_columns(self):
        mock_dataframe = Mock(pyspark.sql.DataFrame)
        mock_dataframe.columns = ['a','b','c']
        a_dataset = Dataset("my dataset", mock_dataframe)
        cols = a_dataset.columns
        self.assertEqual(cols, ['a','b','c'])

    def test_columns_with_types(self):
        mock_dataframe = Mock(pyspark.sql.DataFrame)
        mock_dataframe.dtypes = [('a', 'int'),('b', 'string')]
        a_dataset = Dataset("my dataset", mock_dataframe)
        cols = a_dataset.columns_with_types
        self.assertEqual(cols, [('a', 'int'),('b', 'string')])


if __name__ == '__main__':
    unittest.main()
