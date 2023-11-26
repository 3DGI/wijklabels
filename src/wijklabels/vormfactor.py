"""Calculate the vormfactor of buildings in the 3DBAG

Copyright 2023 3DGI
"""
import logging

import pandas as pd
from pandas import DataFrame
from wijklabels import OrderedEnum

log = logging.getLogger()

class VormfactorClass(OrderedEnum):
    UNDER_050 = (float("-inf"), 0.5)
    FROM_050_UNTIL_100 = (0.5, 1.0)
    FROM_100_UNTIL_150 = (1.0, 1.5)
    FROM_150_UNTIL_200 = (1.5, 2.0)
    FROM_200_UNTIL_250 = (2.0, 2.5)
    FROM_250_UNTIL_300 = (2.5, 3.0)
    FROM_300_UNTIL_350 = (3.0, 3.5)
    ABOVE_350 = (3.5, float("inf"))

    @classmethod
    def from_vormfactor(cls, vormfactor):
        """Classify the vormfactor into into one of the bins defined in the energy
        label distributions study."""
        vormfcls = list(filter(lambda c: c.value[0] <= vormfactor < c.value[1], cls))
        try:
            return vormfcls[0]
        except IndexError:
            log.error(f"couldn't classify vormfactor {vormfactor}")



def vormfactorclass(row):
    vof = vormfactor(row=row)
    try:
        return VormfactorClass.from_vormfactor(vof)
    except ValueError:
        return pd.NA

def vormfactor(row=None, cityobject_id: str=None, cityobject: dict=None, vbo_df: DataFrame=None,
               floor_area=True) -> float | None:
    """Calculate the vormfactor for a single CityObject.

    The vormfactor is calculated as `verliesoppervlakte / oppervlakte`.
    """
    if row is not None:
        vopp = verliesoppervlakte(row=row)
        opp = row["oppervlakte"]
    else:
        vopp = verliesoppervlakte(cityobject)
        if floor_area:
            opp = gebruiksoppervlakte(cityobject_id, vbo_df)
        else:
            opp = oppervlakte(cityobject)
    if vopp is None or opp is None:
        return None
    else:
        return vopp / opp


def verliesoppervlakte(row=None, cityobject: dict = None) -> float | None:
    """Calculate the verliesoppervlakte for a single CityObject.

    The verliesoppervlakte is also abbreviated as `A_is` in the Dutch terminology,
    see https://www.rvo.nl/onderwerpen/wetten-en-regels-gebouwen/standaard-streefwaarden-woningisolatie

    Het verliesoppervlak is de totale oppervlakte van alle scheidingsconstructies
    (buitenmuren, buitenpanelen, ramen, buitendeuren, daken, beganegrondvloeren
    en dergelijke) die het 'beschermd volume' (het deel van het gebouw dat
    normaliter verwarmd wordt) van een gebouw omhullen of omsluiten.

    The verliesoppervlakte is computed from these 3DBAG attributes:
        - b3_opp_buitenmuur
        - b3_opp_grond
        - b3_opp_dak_plat
        - b3_opp_dak_schuin
    """
    if cityobject is not None:
        if "attributes" not in cityobject:
            log.error("CityObject does not have attributes")
            return None
        try:
            return cityobject["attributes"]["b3_opp_buitenmuur"] + \
                cityobject["attributes"]["b3_opp_dak_plat"] + \
                cityobject["attributes"]["b3_opp_dak_schuin"] + \
                cityobject["attributes"]["b3_opp_grond"]
        except KeyError as e:
            log.error(e)
            return None
    elif row is not None:
        return row["b3_opp_buitenmuur"] + row["b3_opp_dak_plat"] + row["b3_opp_dak_schuin"] + row["b3_opp_grond"]
    else:
        raise ValueError


def oppervlakte(cityobject: dict) -> float | None:
    """Calculate the total surface area for a single CityObject.

    The total surface area is also abbreviated as `A_g` in the Dutch terminology,
    see https://www.rvo.nl/onderwerpen/wetten-en-regels-gebouwen/standaard-streefwaarden-woningisolatie

    The total surface area is computed as the sum of these 3DBAG attributes:
        - b3_opp_buitenmuur
        - b3_opp_scheidingsmuur
        - b3_opp_grond
        - b3_opp_dak_plat
        - b3_opp_dak_schuin
    """
    if "attributes" not in cityobject:
        log.error("CityObject does not have attributes")
        return None
    try:
        return cityobject["attributes"]["b3_opp_buitenmuur"] + \
            cityobject["attributes"]["b3_opp_dak_plat"] + \
            cityobject["attributes"]["b3_opp_dak_schuin"] + \
            cityobject["attributes"]["b3_opp_grond"] + \
            cityobject["attributes"]["b3_opp_scheidingsmuur"]
    except KeyError as e:
        log.error(e)
        return None


def gebruiksoppervlakte(cityobject_id: str, vbo_df: DataFrame) -> float | None:
    """Calculate the total floor area of a dwelling.

    The total surface area is also abbreviated as `A_g` in the Dutch terminology,
    see https://www.rvo.nl/onderwerpen/wetten-en-regels-gebouwen/standaard-streefwaarden-woningisolatie
    """
    opp = vbo_df.loc[cityobject_id, "oppervlakte"]
    try:
        return opp.item()
    except ValueError:
        log.error(f"Multiple rows returned when subsetting VBO with {cityobject_id}")
        return None
