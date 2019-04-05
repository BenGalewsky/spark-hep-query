import unittest
from unittest.mock import Mock, patch

from irishep.executors.uproot_executor import UprootExecutor


class TestUprootExecutor(unittest.TestCase):
    def test_init(self):
        e = UprootExecutor("foo")
        self.assertEqual(e.app_name, "foo")

    def test_read_files(self):
        e = UprootExecutor("e")

        with patch("uproot.open",
                   return_value={"Events": "test"}) as uproot_mock:
            d = e.read_files("bar", ["/tmp/baz.root"])
            uproot_mock.assert_called_with("/tmp/baz.root")
            self.assertEqual("bar", d.name)
            self.assertEqual("test", d.ttree)

    def test_read_files_multiple(self):
        e = UprootExecutor("e")

        with patch("uproot.open") as uproot_mock:
            e.read_files("bar", ["/tmp/baz.root", "/tmp/bat.root"])
            uproot_mock.assert_called_with("/tmp/baz.root")

    def test_register_accumulator(self):
        e = UprootExecutor("e")
        mock_accum = Mock()
        mock_accum.addInPlace = Mock(return_value="xxx")
        a = e.register_accumulator("val", mock_accum)
        self.assertEqual("val", a.value)
        self.assertEqual(mock_accum, a.accumulator)

        a.add("yyy")
        mock_accum.addInPlace.assert_called_with("val", "yyy")
        self.assertEqual("xxx", a.value)

    def test_register_broadcast_var(self):
        e = UprootExecutor("e")
        b = e.register_broadcast_var("hi")
        self.assertEqual("hi", b.value)


if __name__ == '__main__':
    unittest.main()
