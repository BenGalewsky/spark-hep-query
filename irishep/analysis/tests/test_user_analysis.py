import unittest

from irishep.analysis.user_analysis import UserAnalysis


class MyTestCase(unittest.TestCase):
    @staticmethod
    def test_base_class():
        class MySubclass(UserAnalysis):
            @staticmethod
            def calc(pyhsics_objects):
                pass

        MySubclass()


if __name__ == '__main__':
    unittest.main()
