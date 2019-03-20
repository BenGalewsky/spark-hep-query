import unittest
from unittest.mock import Mock, patch

import pyspark.sql

from irishep.datasets.dataset import Dataset


class TestDataset(unittest.TestCase):
    @staticmethod
    def _generate_mock_dataframe():
        mock_dataframe = Mock(pyspark.sql.DataFrame)
        mock_dataframe.columns = ['dataset', 'run']
        return mock_dataframe

    def test_constuctor(self):
        # Mocking with the spark sql lit function is a bit tricky. We patch
        # _active_spark_context to handle the interactions with the jvm
        mock_lit = Mock()
        with patch('pyspark.SparkContext._active_spark_context', new=mock_lit):
            mock_dataframe = self._generate_mock_dataframe()
            mock_dataframe.columns = ['run', 'event']
            mock_dataframe.withColumn = Mock(return_value=mock_dataframe)
            a_dataset = Dataset("my dataset", mock_dataframe)
            self.assertEqual(a_dataset.name, "my dataset")
            self.assertEqual(a_dataset.dataframe, mock_dataframe)

    def test_constuctor_with_dataset_name_in_dataframe(self):
        mock_dataframe = self._generate_mock_dataframe()
        a_dataset = Dataset("my dataset", mock_dataframe)
        self.assertEqual(a_dataset.name, "my dataset")
        self.assertEqual(a_dataset.dataframe, mock_dataframe)

    def test_count(self):
        mock_dataframe = self._generate_mock_dataframe()
        mock_dataframe.count = Mock(return_value=42)
        a_dataset = Dataset("my dataset", mock_dataframe)
        count = a_dataset.count()
        self.assertEqual(42, count)
        mock_dataframe.count.assert_called_once()

    def test_columns(self):
        mock_dataframe = self._generate_mock_dataframe()
        mock_dataframe.columns = ['dataset', 'a', 'b', 'c']
        a_dataset = Dataset("my dataset", mock_dataframe)
        cols = a_dataset.columns
        self.assertEqual(cols, ['dataset', 'a', 'b', 'c'])

    def test_columns_with_types(self):
        mock_dataframe = self._generate_mock_dataframe()
        mock_dataframe.dtypes = [('a', 'int'), ('b', 'string')]
        a_dataset = Dataset("my dataset", mock_dataframe)
        cols = a_dataset.columns_with_types
        self.assertEqual(cols, [('a', 'int'), ('b', 'string')])

    def test_select_provide_technical_fields(self):
        mock_dataframe = self._generate_mock_dataframe()
        mock_dataframe2 = self._generate_mock_dataframe()
        mock_dataframe.select = Mock(return_value=mock_dataframe2)

        a_dataset = Dataset("my dataset", mock_dataframe)
        a_dataset2 = a_dataset.select_columns(
            ["dataset", "run", "luminosityBlock", "event", "Electron_pt"])

        self.assertEqual(mock_dataframe2, a_dataset2.dataframe)
        self.assertEqual("my dataset", a_dataset2.name)

        # The actual order of the selected columns is hard to predict. Use
        # sorted column names to test
        call_args = mock_dataframe.select.call_args[0][0]
        self.assertEqual(sorted(
            ["dataset", "run", "luminosityBlock", "event", "Electron_pt"]),
            sorted(call_args))

    def test_select_without_technical_fields(self):
        mock_dataframe = self._generate_mock_dataframe()
        mock_dataframe2 = self._generate_mock_dataframe()
        mock_dataframe.select = Mock(return_value=mock_dataframe2)

        a_dataset = Dataset("my dataset", mock_dataframe)
        a_dataset2 = a_dataset.select_columns(["Electron_pt"])

        self.assertEqual(mock_dataframe2, a_dataset2.dataframe)
        self.assertEqual("my dataset", a_dataset2.name)

        # The actual order of the selected columns is hard to predict. Use
        # sorted column names to test
        call_args = mock_dataframe.select.call_args[0][0]
        self.assertEqual(sorted(
            ["dataset", "run", "luminosityBlock", "event", "Electron_pt"]),
            sorted(call_args))

    def test_show(self):
        mock_dataframe = self._generate_mock_dataframe()
        mock_dataframe.show = Mock()
        a_dataset = Dataset("my dataset", mock_dataframe)
        a_dataset.show()

        mock_dataframe.show.assert_called_once()


if __name__ == '__main__':
    unittest.main()
