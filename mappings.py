import logging
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import Callable

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

NOT_SET = np.NaN


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
        # call-back function to modify data
        self.mod = None
        # call-back function for conditional data-mappings
        self.cond = None
        # if mapping effects more than just the destination column
        # side effects can be specified as kwargs: {'column_name': mod_function}
        # the difference to simple modification function this mod_function will require
        # the entire data-row to manipulate more than dest_column
        self.side_effects = None

    def condition(self, cond_callback: Callable):
        self.cond = cond_callback
        return self

    def modify(self, mod_callback: Callable):
        self.mod = mod_callback
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
            results = OrderedDict
            for ma in self.mappings:
                if ma.const:
                    val = ma.src
                else:
                    try:
                        val = str(row[ma.src]).strip()
                    except KeyError:
                        val = NOT_SET
                        if ma.cond is None \
                                or (
                                    ma.cond is not None
                                    and ma.cond(val, row, results)
                                ):
                            # no error if src column wasn't required
                            logger.error(f'missing source column: "{ma.src}"')

                if ma.cond is not None and not ma.cond(val, row, results):
                    continue

                if ma.mod is not None and val != NOT_SET:
                    val = ma.mod(val)

                if ma.append:
                    results[ma.dst] += val
                else:
                    results.update({ma.dst: val})

            rows.append(results)

        return pd.DataFrame(rows)
