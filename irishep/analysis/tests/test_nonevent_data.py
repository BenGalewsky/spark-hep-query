import unittest
from unittest.mock import Mock

from irishep.analysis.nonevent_data import NonEventData


class TestNoneventData(unittest.TestCase):
    @staticmethod
    def _create_mock_app():
        mock_app = Mock()
        mock_app.executor = Mock()
        mock_app.executor.register_broadcast_var = Mock()
        return mock_app

    def test_init(self):
        mock_app = self._create_mock_app()
        mock_broadcast = Mock()
        mock_app.executor.register_broadcast_var = Mock(
            return_value=mock_broadcast)

        data = "hi"
        nonevent_data = NonEventData(mock_app, data)
        self.assertTrue(nonevent_data)
        mock_app.executor.register_broadcast_var.assert_called_with(data)

    def test_value(self):
        app = self._create_mock_app()
        mock_broadcast = Mock()
        app.executor.register_broadcast_var = Mock(return_value=mock_broadcast)
        mock_broadcast.value = "hi"

        nonevent_data = NonEventData(app, {})
        self.assertEqual("hi", nonevent_data.value)


if __name__ == '__main__':
    unittest.main()
