"""Classify dwellings (woningen)

Copyright 2023 3DGI
"""
import math
from enum import StrEnum

from pandas import NA

from wijklabels import OrderedEnum


# The actual woningtype classification is done in the classify_woningtype_full.sql
# script.

def to_woningtype(w):
    try:
        return Woningtype(w)
    except ValueError:
        return NA


class Woningtype(StrEnum):
    """The classification per NTA method, since 2021-01-01"""
    VRIJSTAAND = "vrijstaande woning"
    TWEE_ONDER_EEN_KAP = "2 onder 1 kap"
    RIJWONING_TUSSEN = "rijwoning tussen"
    RIJWONING_HOEK = "rijwoning hoek"
    APPARTEMENT_HOEKVLOER = "appartement - hoekvloer"
    APPARTEMENT_HOEKMIDDEN = "appartement - hoekmidden"
    APPARTEMENT_HOEKDAK = "appartement - hoekdak"
    APPARTEMENT_HOEKDAKVLOER = "appartement - hoekdakvloer"
    APPARTEMENT_TUSSENVLOER = "appartement - tussenvloer"
    APPARTEMENT_TUSSENMIDDEN = "appartement - tussenmidden"
    APPARTEMENT_TUSSENDAK = "appartement - tussendak"
    APPARTEMENT_TUSSENDAKVLOER = "appartement - tussendakvloer"


class WoningtypePreNTA8800(StrEnum):
    """The pre-2021 (pre-NTA) classification"""
    VRIJSTAAND = "vrijstaande woning"
    TWEE_ONDER_EEN_KAP = "2 onder 1 kap"
    RIJWONING_TUSSEN = "rijwoning tussen"
    RIJWONING_HOEK = "rijwoning hoek"
    MAISONNETTE = "maisonnette"
    GALERIJ = "galerij"
    PORTIEK = "portiek"
    OVERIG = "overig"


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


def distribute_vbo_on_floor(vbo_ids: list[str], nr_floors: int, vbo_count: int) -> list[
    tuple[str, str]]:
    """Distribute the Verblijfsobjecten in one Pand across its floors.

    Returns a list of tuples with the (VBO ID, position), where 'position' is one of
    'vloer', 'midden', 'dak', 'dakvloer'.
    """
    if nr_floors == 1:
        return [(i, "dakvloer") for i in vbo_ids]
    vbo_per_floor = float(vbo_count) / float(nr_floors)
    vbo_per_floor_int = round(vbo_per_floor)
    if vbo_per_floor_int > len(vbo_ids):
        return [(i, "vloer") for i in vbo_ids]
    else:
        vbo_positions = []
        # 1x vbo_per_floor is assigned to the ground floor
        vbo_positions.extend((i, "vloer") for i in vbo_ids[:vbo_per_floor_int])
        del vbo_ids[:vbo_per_floor_int]
        # 1x vbo_per_floor is assigned to the roof or top floor
        vbo_positions.extend((i, "dak") for i in vbo_ids[:vbo_per_floor_int])
        del vbo_ids[:vbo_per_floor_int]
        # the rest of vbo_per floor is in the sandwich
        vbo_positions.extend((i, "midden") for i in vbo_ids[:vbo_per_floor_int])
        return vbo_positions


def classify_apartments(woningtype: Woningtype,
                         vbo_positions: list[tuple[str, str]]) -> list[
    tuple[str, Woningtype]]:
    # We assume that all VBO-s have the same woningtype at this point, because the
    # woningtype was estimated for the whole Pand
    if woningtype in [Woningtype.VRIJSTAAND, Woningtype.TWEE_ONDER_EEN_KAP]:
        return [(i, Woningtype(f"appartement - hoek{position}")) for i, position in
                vbo_positions]
    elif woningtype == Woningtype.RIJWONING_HOEK:
        # It's the end of a row of houses, so we assume that one side of the building
        # is touching a neighbour, thus we assume that half of the apparements are
        # 'tussen', and half of them are 'hoek'.
        half = math.floor(len(vbo_positions) / 2)
        tussen = [(i, Woningtype(f"appartement - tussen{position}")) for i, position in vbo_positions[:half]]
        hoek = [(i, Woningtype(f"appartement - hoek{position}")) for i, position in vbo_positions[half:]]
        return hoek + tussen
    elif woningtype == Woningtype.RIJWONING_TUSSEN:
        return [(i, Woningtype(f"appartement - tussen{position}")) for i, position in
                vbo_positions]
