import unittest
from collections import OrderedDict
from core.mapping import Mapping, DataFrameMappings
import pandas as pd


def family_with_kids(val: str, row: pd.Series, results: OrderedDict) -> bool:
    return True if int(row['children']) > 0 else False


class MyMappings(DataFrameMappings):
    mappings = [
        Mapping('fname', 'family').condition(family_with_kids),
        Mapping('children', 'members').condition(family_with_kids).modify(lambda x: int(x) + 2),
        Mapping('pnames', 'parents').condition(family_with_kids),
        Mapping('knames', 'kids').condition(family_with_kids),
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
        self.assertEqual(len(res_table.index), 3)
