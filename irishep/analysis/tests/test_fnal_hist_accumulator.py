import unittest
from unittest.mock import Mock

from pyspark import SparkContext

import fnal_column_analysis_tools.hist as hist
from irishep.analysis.fnal_hist_accumulator import FnalHistAccumulator


class TestFNALHistAccumulator(unittest.TestCase):
    @staticmethod
    def _create_fnal_accumulator():
        ds_axis = Mock(hist.Cat)
        cat_axis = Mock(hist.Cat)
        mock_spark = Mock(SparkContext)
        mock_accumulator = Mock()
        mock_spark.accumulator = Mock(return_value=mock_accumulator)
        return FnalHistAccumulator(ds_axis, cat_axis, mock_spark)

    def test_init(self):
        ds_axis = Mock(hist.Cat)
        cat_axis = Mock(hist.Cat)
        mock_spark = Mock(SparkContext)
        mock_accumulator = Mock()
        mock_spark.accumulator = Mock(return_value=mock_accumulator)
        accum = FnalHistAccumulator(ds_axis, cat_axis, mock_spark)

        self.assertEqual(accum.dataset_axis, ds_axis)
        self.assertEqual(accum.channel_cat_axis, cat_axis)
        self.assertEqual(accum.accumulator, mock_accumulator)

        mock_spark.accumulator.assert_called_with(None, accum)

    def test_zero(self):
        accum = self._create_fnal_accumulator()
        result = accum.zero(1)
        self.assertEqual(result, 1)

    def test_add_in_place_first_time(self):
        accum = self._create_fnal_accumulator()
        result = accum.addInPlace(None, 1)
        self.assertEqual(result, 1)

    def test_add_in_place_second_time(self):
        accum = self._create_fnal_accumulator()
        result = accum.addInPlace(2, 1)
        self.assertEqual(result, 3)


if __name__ == '__main__':
    unittest.main()
