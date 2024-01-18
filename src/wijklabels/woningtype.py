"""Classify dwellings (woningen)

Copyright 2023 3DGI
"""
from enum import StrEnum
import random
import logging
import itertools

import pandas as pd
from pandas import NA

from wijklabels import OrderedEnum

log = logging.getLogger("main")


# The actual woningtype classification is done in the classify_woningtype_full.sql
# script.


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

    @classmethod
    def from_str(cls, string: str):
        """Converts a string to an Woningtype

        :returns: a Woningtype object or pandas.NA if the string is invalid Woningtype
        """
        try:
            return cls(string)
        except ValueError:
            return pd.NA


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

    @classmethod
    def from_nta8800(cls, woningtype: Woningtype, oorspronkelijkbouwjaar: int):
        """Map an NTA8800 Woningtype to a pre-NTA8800 Woningtype"""
        if woningtype is pd.NA:
            return pd.NA
        elif woningtype == Woningtype.VRIJSTAAND:
            return cls.VRIJSTAAND
        elif woningtype == Woningtype.TWEE_ONDER_EEN_KAP:
            return cls.TWEE_ONDER_EEN_KAP
        elif woningtype == Woningtype.RIJWONING_TUSSEN:
            return cls.RIJWONING_TUSSEN
        elif woningtype == Woningtype.RIJWONING_HOEK:
            return cls.RIJWONING_HOEK
        else:
            # Choose one of the apartement types from a distribution that was
            # calculated from the EP-Online data.
            if oorspronkelijkbouwjaar <= 1964:
                return random.choice(APARTEMENTS_DISTRIBUTION_PRE_NTA8800[(0, 1964)])
            elif 1965 <= oorspronkelijkbouwjaar <= 1974:
                return random.choice(APARTEMENTS_DISTRIBUTION_PRE_NTA8800[(1965, 1974)])
            elif 1975 <= oorspronkelijkbouwjaar <= 1991:
                return random.choice(APARTEMENTS_DISTRIBUTION_PRE_NTA8800[(1975, 1991)])
            elif 1992 <= oorspronkelijkbouwjaar:
                return random.choice(APARTEMENTS_DISTRIBUTION_PRE_NTA8800[(1992, 9999)])
            else:
                raise ValueError(
                    f"cannot determine apartement type from {oorspronkelijkbouwjaar=}, {woningtype=}")

    @classmethod
    def from_str(cls, string: str):
        """Converts a string to an WoningtypePreNTA8800

        :returns: a WoningtypePreNTA8800 object or pandas.NA if the string is invalid
            WoningtypePreNTA8800
        """
        try:
            return cls(string)
        except ValueError:
            return pd.NA


# The distribution of these types are compoted from the EP-Online data, from the records
# before 2021-01-01
APARTEMENTS_DISTRIBUTION_PRE_NTA8800 = {
    (0, 1964): list(itertools.chain(
        (WoningtypePreNTA8800.OVERIG for _ in range(78)),
        (WoningtypePreNTA8800.GALERIJ for _ in range(2)),
        (WoningtypePreNTA8800.MAISONNETTE for _ in range(16)),
        (WoningtypePreNTA8800.PORTIEK for _ in range(4)),
    )),
    (1965, 1974): list(itertools.chain(
        (WoningtypePreNTA8800.OVERIG for _ in range(84)),
        (WoningtypePreNTA8800.GALERIJ for _ in range(9)),
        (WoningtypePreNTA8800.MAISONNETTE for _ in range(6)),
        (WoningtypePreNTA8800.PORTIEK for _ in range(2)),
    )),
    (1975, 1991): list(itertools.chain(
        (WoningtypePreNTA8800.OVERIG for _ in range(78)),
        (WoningtypePreNTA8800.GALERIJ for _ in range(4)),
        (WoningtypePreNTA8800.MAISONNETTE for _ in range(15)),
        (WoningtypePreNTA8800.PORTIEK for _ in range(2)),
    )),
    (1992, 9999): list(itertools.chain(
        (WoningtypePreNTA8800.OVERIG for _ in range(85)),
        (WoningtypePreNTA8800.GALERIJ for _ in range(7)),
        (WoningtypePreNTA8800.MAISONNETTE for _ in range(7)),
        (WoningtypePreNTA8800.PORTIEK for _ in range(1)),
    ))
}


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
    FROM_1992 = 1992, 9999

    def __repr__(self):
        return str(self.value)

    def __str__(self):
        return str(self.value)

    def format_pretty(self):
        s = " - ".join(map(str, self.value))
        if "0" == s[0]:
            s = s.replace("0 -", "<")
        elif "9999" in s:
            s = s.replace("- 9999", "<")
        return s

    @classmethod
    def from_year_type(cls, oorspronkelijkbouwjaar: int,
                       woningtype: WoningtypePreNTA8800):
        """Classify the oorspronkelijkbouwjaar of a BAG object into the 8 construction
        year periods that are defined in the 2022 update of the WoON2018 study.
        All classes are inclusive of their date limits."""
        if woningtype == WoningtypePreNTA8800.VRIJSTAAND or woningtype == WoningtypePreNTA8800.TWEE_ONDER_EEN_KAP:
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
        elif woningtype == WoningtypePreNTA8800.RIJWONING_HOEK or woningtype == WoningtypePreNTA8800.RIJWONING_TUSSEN:
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
        elif woningtype == WoningtypePreNTA8800.GALERIJ or woningtype == WoningtypePreNTA8800.MAISONNETTE or woningtype == WoningtypePreNTA8800.OVERIG:
            if oorspronkelijkbouwjaar <= 1964:
                return cls((0, 1964))
            elif 1965 <= oorspronkelijkbouwjaar <= 1974:
                return cls((1965, 1974))
            elif 1975 <= oorspronkelijkbouwjaar <= 1991:
                return cls((1975, 1991))
            elif 1992 <= oorspronkelijkbouwjaar:
                return cls((1992, 9999))
            else:
                raise ValueError(oorspronkelijkbouwjaar, woningtype)
        elif woningtype == WoningtypePreNTA8800.PORTIEK:
            if oorspronkelijkbouwjaar <= 1945:
                return cls((0, 1945))
            elif 1946 <= oorspronkelijkbouwjaar <= 1964:
                return cls((1946, 1964))
            elif 1965 <= oorspronkelijkbouwjaar <= 1974:
                return cls((1965, 1974))
            elif 1975 <= oorspronkelijkbouwjaar <= 1991:
                return cls((1975, 1991))
            elif 1992 <= oorspronkelijkbouwjaar:
                return cls((1992, 9999))
            else:
                raise ValueError(oorspronkelijkbouwjaar, woningtype)

    @classmethod
    def from_year_type_new(cls, oorspronkelijkbouwjaar: int, woningtype: Woningtype):
        """Classify the oorspronkelijkbouwjaar of a BAG object into the 8 construction
        year periods that are defined in the 2022 update of the WoON2018 study.
        Using the NTA8800 woningtypen.
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
        else:
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

    @classmethod
    def from_year(cls, oorspronkelijkbouwjaar: int):
        """Classify the oorspronkelijkbouwjaar of a BAG object into the 8 construction
        year periods that are defined in the 2022 update of the WoON2018 study.
        Without considering the woningtype.
        All classes are inclusive of their date limits."""
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
            raise ValueError(oorspronkelijkbouwjaar)

    @classmethod
    def from_str(cls, string: str):
        """Converts a string to an Bouwperiode, where the string must represent a tuple
        of integers of a valid Bouwperiode, e.g. '(1946, 1964)'.

        :returns: a Bouwperiode object or pandas.NA if the string is invalid Bouwperiode
        """
        try:
            return cls(eval(string))
        except ValueError:
            return pd.NA


def distribute_vbo_on_floor(group: pd.DataFrame) -> pd.DataFrame | None:
    """Distribute the Verblijfsobjecten in one Pand across its floors.
    Takes a DataFrame group and adds two attributes, `_position` which is one of
    'vloer', 'midden', 'dak', 'dakvloer' and `_floor` which is the 0-indexed floor
    number.

    Returns the updated copy of the group.
    """
    group_copy = group.copy()
    group_copy["_position"] = pd.NA
    group_copy["_position"] = group_copy["_position"].astype("object")
    group_copy["_floor"] = pd.NA
    group_copy["_floor"] = group_copy["_floor"].astype("Int64")

    pid = group.index.get_level_values("pand_identificatie")[0]
    vbo_pand_ids = list(group.index)
    if len(vbo_pand_ids) == 0:
        log.debug(f"Skipping Pand {pid}, because vbo_ids is empty")
        return None
    nr_floors = group["nr_floors"].values[0]
    if nr_floors is None:
        log.debug(f"Skipping Pand {pid}, because nr_floors is None")
        return None
    vbo_count = group["vbo_count"].values[0]
    if vbo_count is None:
        log.debug(f"Skipping Pand {pid}, because vbo_count is None")
        return None
    wtype_pand = group["woningtype"].values[0]
    if wtype_pand is None:
        log.debug(f"Skipping Pand {pid}, because wtype_pand is None")
        return None
    vbo_per_floor = round(float(vbo_count) / float(nr_floors))

    if vbo_per_floor >= vbo_count:
        # These are single-storey buildings with one dwelling, or multi-storey buildings
        # with one dwelling that spans across the floors.
        group_copy["_position"] = "dakvloer"
        group_copy["_floor"] = 0
    else:
        remaining_vbo_to_distribute = vbo_pand_ids
        # 1x vbo_per_floor is assigned to the ground floor
        selected_vbos = remaining_vbo_to_distribute[:vbo_per_floor]
        del remaining_vbo_to_distribute[:vbo_per_floor]
        group_copy.loc[selected_vbos, "_position"] = "vloer"
        group_copy.loc[selected_vbos, "_floor"] = 0

        # 1x vbo_per_floor is assigned to the roof or top floor
        if len(remaining_vbo_to_distribute) == 0:
            return group_copy
        selected_vbos = remaining_vbo_to_distribute[:vbo_per_floor]
        del remaining_vbo_to_distribute[:vbo_per_floor]
        group_copy.loc[selected_vbos, "_position"] = "dak"
        # We have 0-indexed floor numbers
        group_copy.loc[selected_vbos, "_floor"] = nr_floors - 1

        # the rest of vbo_per_floor is in the sandwich
        floor = 1
        for floor in range(1, nr_floors - 1):
            if len(remaining_vbo_to_distribute) > 0:
                selected_vbos = remaining_vbo_to_distribute[:vbo_per_floor]
                del remaining_vbo_to_distribute[:vbo_per_floor]
                group_copy.loc[selected_vbos, "_position"] = "midden"
                group_copy.loc[selected_vbos, "_floor"] = floor
            else:
                return group_copy
        if len(remaining_vbo_to_distribute) > 0:
            # We have remaining VBOs, so we assign them to the last floor of the
            # "midden". This can happen if the we are on the last floor of loop
            # and the vbo_per_floor is less than the remaining VBO
            group_copy.loc[remaining_vbo_to_distribute, "_position"] = "midden"
            group_copy.loc[remaining_vbo_to_distribute, "_floor"] = floor
    return group_copy


def classify_apartments(group: pd.DataFrame) -> pd.DataFrame | None:
    group_copy = group.copy()
    woningtype = group["woningtype"].values[0]

    pid = group.index.get_level_values("pand_identificatie")[0]
    vbo_pand_ids = list(group.index)
    if len(vbo_pand_ids) == 0:
        log.debug(f"Skipping Pand {pid}, because vbo_ids is empty")
        return None
    nr_floors = group["nr_floors"].values[0]
    if nr_floors is None:
        log.debug(f"Skipping Pand {pid}, because nr_floors is None")
        return None
    vbo_count = group["vbo_count"].values[0]
    if vbo_count is None:
        log.debug(f"Skipping Pand {pid}, because vbo_count is None")
        return None
    vbo_per_floor = round(float(vbo_count) / float(nr_floors))

    # We assume a rectangular footprint
    # We can have the apartements in a single or double row.
    # We choose randomly between the two layouts.
    # The single row, has two hoek apartements and N tussen per floor. The double row
    # has 2x2 hoek and N tussen per floor.
    # An improved version would take into account the shape of the footprint to
    # determine the most likely layout.
    # 1 is double row, 0 is single row
    double_row = random.binomialvariate()
    if vbo_per_floor <= 3:
        double_row = False
    nr_hoek = None
    if woningtype == Woningtype.VRIJSTAAND:
        nr_hoek = 4 if double_row else 2
    elif woningtype == Woningtype.TWEE_ONDER_EEN_KAP or woningtype == Woningtype.RIJWONING_HOEK:
        nr_hoek = 2 if double_row else 1
    elif woningtype == Woningtype.RIJWONING_TUSSEN:
        nr_hoek = 0
    for _floor, g in group.groupby("_floor"):
        hoek = g["_position"].iloc[:nr_hoek].map("appartement - hoek{}".format).map(
            Woningtype)
        tussen = g["_position"].iloc[nr_hoek:].map("appartement - tussen{}".format).map(
            Woningtype)
        group_copy.loc[hoek.index, "woningtype"] = hoek
        group_copy.loc[tussen.index, "woningtype"] = tussen
    return group_copy
