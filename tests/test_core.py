import unittest
from core.mapping import Mapping, DataFrameMappings
import pandas as pd


class MyMappings(DataFrameMappings):
    mappings = [
        Mapping('fname', 'family'),
        Mapping('pnames', 'parents'),
        Mapping('knames', 'kids'),
        Mapping('children', 'members')
    ]

    test_table = {
        'fname': ['TANNER', 'SIMPSON', 'BUNDY', 'SMITH'],
        'pnames': ['Kate, Willie', 'March, Homer', 'Peggy, Al', 'Jane, John'],
        'knames': ['Lynn, Brian, Alf', 'Bart, Lisa, Maggie', 'Kelly, Bud', ''],
        'children': [3, 2, 2, 0]
    }

    def prepare(self):
        self.source_df = pd.DataFrame(self.test_table)


class TestMappings(unittest.TestCase):
    def setUp(self):
        self.test_mappings = MyMappings()
        self.test_mappings.prepare()

    def test_mapping(self):
        res_table = self.test_mappings.run_mappings()
        self.assertEqual(len(res_table), 4)
