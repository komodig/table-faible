"""
from abc import ABC, abstractmethod
from collections import OrderedDict
"""
from typing import Callable

import numpy as np

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

    def condition(self, cond_callback: Callable):
        self.cond = cond_callback
        return self

    def modify(self, mod_callback: Callable):
        self.mod = mod_callback
        return self
