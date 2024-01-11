from typing import Tuple
from enum import Enum
import functools

Bbox = Tuple[float, float, float, float]


@functools.total_ordering
class OrderedEnum(Enum):
    """Source taken from https://github.com/woodruffw/ordered_enum

    Copyright (c) 2020 William Woodruff <william @ yossarian.net>
    """
    @classmethod
    @functools.lru_cache(None)
    def _member_list(cls):
        return list(cls)

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            member_list = self.__class__._member_list()
            return member_list.index(self) < member_list.index(other)
        return NotImplemented
