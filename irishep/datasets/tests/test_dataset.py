import unittest
from unittest.mock import Mock, patch, MagicMock
import pyspark.sql
from irishep.datasets.dataset import Dataset


class TestDataset(unittest.TestCase):
    @staticmethod
    def _generate_mock_dataframe():
        mock_dataframe = MagicMock(pyspark.sql.DataFrame)
        mock_dataframe.columns = ['dataset', 'run']
        mock_dataframe.dtypes = [
            ('Electron_pt', 'string'),
            ('event', 'string'), ('luminosityBlock', 'string'),
            ("dataset", "string"), ('run', 'string')]

        # Mock dataframe subscripting to get columns
        mock_dataframe.__getitem__.side_effect = ['Electron_pt', 'dataset',
                                                  'event', 'luminosityBlock',
                                                  'run']

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

    def test_columns_for_physics_objects(self):
        mock_dataframe = self._generate_mock_dataframe()
        mock_dataframe.columns = ["dataset", "run", "luminosityBlock", "event",
                                  "nElectrons", "Electron_pt", "Electron_eta",
                                  "nMuons", "Muon_pt", "Muon_eta"]
        a_dataset = Dataset("my dataset", mock_dataframe)
        rslt = a_dataset.columns_for_physics_objects(["Electron", "Muon"])
        self.assertEqual(rslt,
                         ['nElectrons', 'Electron_pt', 'Electron_eta', 'nMuons',
                          'Muon_pt', 'Muon_eta'])

    def test_count_column_for_physics_object(self):
        mock_dataframe = self._generate_mock_dataframe()
        a_dataset = Dataset("my dataset", mock_dataframe)
        self.assertEqual("nElectron",
                         a_dataset.count_column_for_physics_object("Electron"))

    # Given I included the technical fields in a select. When I perform the
    # select then I should see a dataframe that holds my fields
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

    # Given I included the technical fields in a select
    # and I include a field that has a type that is not supported by arrow.
    # When I perform the select then I should see a dataframe that holds my
    # fields with the non-supported column cast appropriately
    def test_select_non_arrow_type(self):
        mock_dataframe = self._generate_mock_dataframe()
        mock_dataframe2 = self._generate_mock_dataframe()
        mock_dataframe2.dtypes = [
            ('Muon_tightId', 'array<boolean>'),
            ('event', 'string'), ('luminosityBlock', 'string'),
            ("dataset", "string"), ('run', 'string')]
        mock_dataframe.columns = ['Muon_tightId',
                                  'event', 'luminosityBlock',
                                  "dataset", 'run']

        # Mock dataframe subscripting to get columns
        muon_tight_id_mock = MagicMock()
        mock_dataframe2.__getitem__.side_effect = [muon_tight_id_mock,
                                                   'dataset',
                                                   'event', 'luminosityBlock',
                                                   'run']

        mock_dataframe.select = Mock(return_value=mock_dataframe2)

        a_dataset = Dataset("my dataset", mock_dataframe)

        a_dataset2 = a_dataset.select_columns(
            ["dataset", "run", "luminosityBlock", "event", "Muon_tightId"])

        self.assertEqual(mock_dataframe2, a_dataset2.dataframe)
        self.assertEqual("my dataset", a_dataset2.name)

        muon_tight_id_mock.cast.assert_called_with("array<int >")

    # Given I did not inlude the technical fields in a select. When I perform
    # the select then I should see a dataframe that holds my fields plus the
    # technical fields
    def test_select_without_technical_fields(self):
        mock_dataframe = self._generate_mock_dataframe()
        mock_dataframe2 = self._generate_mock_dataframe()
        mock_dataframe2.dtypes = [
            ('Electron_pt', 'string'),
            ('event', 'string'), ('luminosityBlock', 'string'),
            ("dataset", "string"), ('run', 'string')]

        mock_dataframe.select = Mock(return_value=mock_dataframe2)

        a_dataset = Dataset("my dataset", mock_dataframe)
        a_dataset2 = a_dataset.select_columns(["Electron_pt"])

        self.assertEqual(mock_dataframe2, a_dataset2.dataframe)
        self.assertEqual("my dataset", a_dataset2.name)

        # The actual order of the selected columns is hard to predict. Use
        # sorted column names to test
        call_args = mock_dataframe.select.call_args[0][0]
        self.assertEqual(sorted(call_args),
                         sorted(["dataset", "run", "luminosityBlock", "event",
                                 "Electron_pt"]))

    def test_udf_arguments(self):
        mock_dataframe = self._generate_mock_dataframe()
        mock_dataframe.columns = ["dataset", "run", "luminosityBlock", "event",
                                  "nElectrons", "Electron_pt", "Electron_eta",
                                  "nMuons", "Muon_pt", "Muon_eta"]
        a_dataset = Dataset("my dataset", mock_dataframe)
        result = a_dataset.udf_arguments(["Electron"])
        self.assertEqual(
            ['dataset', 'nElectrons', 'Electron_pt', 'Electron_eta'], result)

    def test_show(self):
        mock_dataframe = self._generate_mock_dataframe()
        mock_dataframe.show = Mock()
        a_dataset = Dataset("my dataset", mock_dataframe)
        a_dataset.show()

        mock_dataframe.show.assert_called_once()

    def test_repartition(self):
        mock_dataframe = self._generate_mock_dataframe()
        mock_dataframe.repartition = Mock()
        a_dataset = Dataset("my dataset", mock_dataframe)

        a_dataset.repartition(42)
        mock_dataframe.repartition.assert_called_with(42)


if __name__ == '__main__':
    unittest.main()
