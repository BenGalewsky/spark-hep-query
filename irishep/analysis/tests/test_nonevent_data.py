import unittest
from unittest.mock import Mock

from pyspark import SparkContext

from irishep.analysis.nonevent_data import NonEventData


class TestNoneventData(unittest.TestCase):
    @staticmethod
    def _create_mock_app():
        mock_app = Mock()
        mock_app.spark = Mock()
        mock_app.spark.sparkContext = Mock(SparkContext)
        return mock_app

    def test_init(self):
        app = self._create_mock_app()
        mock_broadcast = Mock()
        app.spark.sparkContext.broadcast = Mock(return_value=mock_broadcast)

        data = "hi"
        nonevent_data = NonEventData(app, data)
        self.assertTrue(nonevent_data)
        app.spark.sparkContext.broadcast.assert_called_with(data)

    def test_value(self):
        app = self._create_mock_app()
        mock_broadcast = Mock()
        app.spark.sparkContext.broadcast = Mock(return_value=mock_broadcast)
        mock_broadcast.value = "hi"

        nonevent_data = NonEventData(app, {})
        self.assertEqual("hi", nonevent_data.value)


if __name__ == '__main__':
    unittest.main()
