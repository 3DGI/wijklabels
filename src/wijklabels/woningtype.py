"""Classify dwellings (woningen)

Copyright 2023 3DGI
"""
from enum import StrEnum

from wijklabels import OrderedEnum


class Woningtype(StrEnum):
    VRIJSTAAND = "vrijstaand"
    TWEE_ONDER_EEN_KAP = "2 onder 1 kap"
    RIJWONING_TUSSEN = "rijwoning tussen"
    RIJWONING_HOEK = "rijwoning hoek"


class Bouwperiode(OrderedEnum):
    """Needs an ordered enum so that it can be grouped by pandas"""
    UNTIL_1945 = 0, 1945
    UNTIL_1964 = 0, 1964
    FROM_1946_UNTIL_1964 = 1946, 1964
    FROM_1965_UNTIL_1974 = 1965, 1974
    FROM_1975_UNTIL_1991 = 1975, 1991
    FROM_1992_UNTIL_2005 = 1992, 2005
    FROM_2006_UNTIL_2014 = 2006, 2014
    FROM_2015 = 2015, 9999

    @classmethod
    def from_year_type(cls, oorspronkelijkbouwjaar: int, woningtype: Woningtype):
        """Classify the oorspronkelijkbouwjaar of a BAG object into the 8 construction
        year periods that are defined in the 2022 update of the WoON2018 study.
        All classes are inclusive of their date limits."""
        if woningtype == Woningtype.VRIJSTAAND or woningtype == Woningtype.TWEE_ONDER_EEN_KAP:
            if oorspronkelijkbouwjaar <= 1964:
                return cls((0, 1964))
            elif 1965 <= oorspronkelijkbouwjaar <= 1974:
                return cls((1965, 1974))
            elif 1975 <= oorspronkelijkbouwjaar <= 1991:
                return cls((1975, 1991))
            elif 1992 <= oorspronkelijkbouwjaar <= 2005:
                return cls((1992, 2005))
            elif 2006 <= oorspronkelijkbouwjaar <= 2014:
                return cls((2006, 2014))
            elif 2015 <= oorspronkelijkbouwjaar:
                return cls((2015, 9999))
            else:
                raise ValueError(oorspronkelijkbouwjaar, woningtype)
        elif woningtype == Woningtype.RIJWONING_HOEK or woningtype == Woningtype.RIJWONING_TUSSEN:
            if oorspronkelijkbouwjaar <= 1945:
                return cls((0, 1945))
            elif 1946 <= oorspronkelijkbouwjaar <= 1964:
                return cls((1946, 1964))
            elif 1965 <= oorspronkelijkbouwjaar <= 1974:
                return cls((1965, 1974))
            elif 1975 <= oorspronkelijkbouwjaar <= 1991:
                return cls((1975, 1991))
            elif 1992 <= oorspronkelijkbouwjaar <= 2005:
                return cls((1992, 2005))
            elif 2006 <= oorspronkelijkbouwjaar <= 2014:
                return cls((2006, 2014))
            elif 2015 <= oorspronkelijkbouwjaar:
                return cls((2015, 9999))
            else:
                raise ValueError(oorspronkelijkbouwjaar, woningtype)
