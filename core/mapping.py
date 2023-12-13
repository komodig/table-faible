import logging
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import Callable

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

NOT_SET = np.NaN


class MappingCondition:
    def __init__(self, callback_function: Callable):
        self.condition_callable = callback_function

    def evaluate(self, val: str, row: pd.Series, results: OrderedDict) -> bool:
        res = self.condition_callable(val, row, results)
        assert isinstance(res, bool)
        return res


class Modification:
    def __init__(self, callback_function: Callable):
        self.modification_callable = callback_function

    def apply(self, val: str, row: pd.Series, results: OrderedDict) -> str:
        try:
            return self.modification_callable(val, row, results)
        except TypeError:
            # in case of lambda function with single arg
            return self.modification_callable(val)


class Mapping:
    def __init__(self,
                 src_column: str, dst_column: str,
                 append: bool = False, const: bool = False):
        self.src = src_column
        self.dst = dst_column
        # True will consider self.src not as column in the source table
        # but as (string) constant to be copied to dst column in each row
        self.const = const
        # value from src column is appended to existing value of dst column
        self.append = append
        # Modification instance
        self.mod = None
        # Condition instance
        self.cond = None
        # if mapping effects more than just the destination column
        # side effects can be specified as kwargs: {'column_name': mod_function}
        # the difference to simple modification function this mod_function will require
        # the entire data-row to manipulate more than dest_column
        self.side_effects = None

    def condition(self, cond_cb: Callable):
        self.cond = MappingCondition(cond_cb)
        return self

    def modify(self, mod_cb: Callable):
        self.mod = Modification(mod_cb)
        return self

    def side_effects(self, column_functions: OrderedDict):
        self.side_effects = column_functions
        return self


class DataFrameMappings(ABC):
    """
    Derived classes have to define mappings (see examples)
    and implement prepare method for source data to run mappings on.
    """
    mappings = None

    def __init__(self):
        self.source_df = None

    @abstractmethod
    def prepare(self):
        """
        Implementation must prepare source data to fit mapping definitions
        """
        pass

    def run_mappings(self) -> pd.DataFrame:
        self.prepare()

        rows = []
        for _, row in self.source_df.iterrows():
            results = OrderedDict()
            for ma in self.mappings:
                if ma.const:
                    val = ma.src
                else:
                    try:
                        val = str(row[ma.src]).strip()
                    except KeyError:
                        val = NOT_SET
                        if ma.cond is None or ma.cond.evaluate(val, row, results) is True:
                            # no error if src column wasn't required
                            logger.error(f'missing source column: "{ma.src}"')

                if ma.cond is not None and not ma.cond.evaluate(val, row, results):
                    continue

                if ma.mod is not None and val != NOT_SET:
                    val = ma.mod.apply(val, row, results)

                if ma.append:
                    results[ma.dst] += val
                else:
                    results.update({ma.dst: val})

                if ma.side_effects:
                    for col, mod_func in ma.side_effects.items():
                        new_val = mod_func(val, row, results)
                        results.update({col: new_val})

            if len(results):
                rows.append(results)

        return pd.DataFrame(rows)
