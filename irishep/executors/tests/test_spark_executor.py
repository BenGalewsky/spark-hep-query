import unittest
from unittest.mock import patch, Mock, MagicMock, call

import pyspark.sql
from pyspark.sql import SparkSession

from irishep.executors.spark_executor import SparkExecutor


class TestSparkExecutor(unittest.TestCase):
    @staticmethod
    def _construct_exector():
        builder = pyspark.sql.session.SparkSession.Builder()
        mock_session = MagicMock(SparkSession)

        builder.master = Mock(return_value=builder)
        builder.appName = Mock(return_value=builder)
        builder.getOrCreate = Mock(return_value=mock_session)

        with patch('pyspark.sql.SparkSession.builder', new=builder):
            e = SparkExecutor(app_name="foo",
                              master="spark-master", num_partitions=42)

            return e

    def test_init(self):
        builder = pyspark.sql.session.SparkSession.Builder()
        mock_session = MagicMock(SparkSession)

        builder.master = Mock(return_value=builder)
        builder.appName = Mock(return_value=builder)
        builder.getOrCreate = Mock(return_value=mock_session)

        with patch('pyspark.sql.SparkSession.builder', new=builder):
            e = SparkExecutor(app_name="foo",
                              master="spark-master", num_partitions=42)

            assert e
            builder.master.assert_called_with("spark-master")
            builder.appName.assert_called_with("foo")
            builder.getOrCreate.assert_called_once()
            self.assertEqual(e.spark, mock_session)

    def test_read_files(self):
        executor = self._construct_exector()

        # There will be dataframes generated for each file as part of the load
        mock_file_dataframes = [Mock(pyspark.sql.DataFrame),
                                Mock(pyspark.sql.DataFrame)]
        executor.spark.read.format = Mock(return_value=executor.spark)
        executor.spark.option = Mock(return_value=executor.spark)
        executor.spark.load = Mock(side_effect=mock_file_dataframes)

        # The first dataframe will be union'ed with the second, resulting in a
        # new dataframe
        mock_union_dataframe = Mock(pyspark.sql.DataFrame)
        mock_union_dataframe.columns = ['dataset', 'a', 'b']
        mock_file_dataframes[0].union = Mock(return_value=mock_union_dataframe)
        mock_union_dataframe.repartition = Mock(
            return_value=mock_union_dataframe)

        # Perform the read
        dataset = executor.read_files("mydataset",
                                      ["/tmp/foo.root", "/tmp/bar.root"])

        executor.spark.read.format.assert_called_with("org.dianahep.sparkroot")

        executor.spark.load.assert_has_calls(
            [call("/tmp/foo.root"), call("/tmp/bar.root")])

        self.assertEqual(dataset.name, "mydataset")
        self.assertEqual(dataset.dataframe, mock_union_dataframe)

        # Verify that the first file's dataframe was unioned with the
        # second file
        mock_file_dataframes[0].union.assert_called_with(
            mock_file_dataframes[1])

        # Verify that the resulting dataframe was repartitioned
        mock_union_dataframe.repartition.assert_called_with(42)

    def test_register_accumulator(self):
        executor = self._construct_exector()
        mock_accumulator = Mock()
        executor.spark.sparkContext.accumulator = Mock()
        executor.register_accumulator("init", mock_accumulator)
        executor.\
            spark.sparkContext.\
            accumulator.assert_called_with("init", mock_accumulator)

    def test_register_broadcast_var(self):
        executor = self._construct_exector()
        executor.spark.sparkContext.broadcast = Mock()
        executor.register_broadcast_var("myVar")
        executor.\
            spark.sparkContext.\
            broadcast.assert_called_with("myVar")


if __name__ == '__main__':
    unittest.main()
