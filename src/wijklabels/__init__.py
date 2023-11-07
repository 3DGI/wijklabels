from typing import Tuple
from enum import Enum

Bbox = Tuple[float, float, float, float]

class OrderedEnum(Enum):

    def __ge__(self, other):

        if self.__class__ is other.__class__:
            return self.value >= other.value

        return NotImplemented

    def __gt__(self, other):

        if self.__class__ is other.__class__:
            return self.value > other.value

        return NotImplemented

    def __le__(self, other):

        if self.__class__ is other.__class__:
            return self.value <= other.value

        return NotImplemented

    def __lt__(self, other):

        if self.__class__ is other.__class__:
            return self.value < other.value

        return NotImplemented