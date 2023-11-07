"""Calculate the vormfactor of buildings in the 3DBAG

Copyright 2023 3DGI
"""
import logging
from wijklabels import OrderedEnum


class VormfactorClass(OrderedEnum):
    UNDER_050 = (-99.0, 0.5)
    FROM_050_UNTIL_100 = (0.5, 1.0)
    FROM_100_UNTIL_150 = (1.0, 1.5)
    FROM_150_UNTIL_200 = (1.5, 2.0)
    FROM_200_UNTIL_250 = (2.0, 2.5)
    FROM_250_UNTIL_300 = (2.5, 3.0)
    FROM_300_UNTIL_350 = (3.0, 3.5)
    ABOVE_350 = (3.5, 99.0)

    @classmethod
    def from_vormfactor(cls, vormfactor):
        """Classify the vormfactor into into one of the bins defined in the energy
        label distributions study."""
        return list(filter(lambda c: c.value[0] <= vormfactor < c.value[1], cls))[0]


def vormfactor(cityobject: dict) -> float | None:
    """Calculate the vormfactor for a single CityObject.

    The vormfactor is calculated as `verliesoppervlakte / oppervlakte`.
    """
    vopp = verliesoppervlakte(cityobject)
    opp = oppervlakte(cityobject)
    if vopp is None or opp is None:
        return None
    else:
        return vopp / opp


def verliesoppervlakte(cityobject: dict) -> float | None:
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
    if "attributes" not in cityobject:
        logging.error("CityObject does not have attributes")
        return None
    try:
        return cityobject["attributes"]["b3_opp_buitenmuur"] + \
            cityobject["attributes"]["b3_opp_dak_plat"] + \
            cityobject["attributes"]["b3_opp_dak_schuin"] + \
            cityobject["attributes"]["b3_opp_grond"]
    except KeyError as e:
        logging.error(e)
        return None


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
        logging.error("CityObject does not have attributes")
        return None
    try:
        return cityobject["attributes"]["b3_opp_buitenmuur"] + \
            cityobject["attributes"]["b3_opp_dak_plat"] + \
            cityobject["attributes"]["b3_opp_dak_schuin"] + \
            cityobject["attributes"]["b3_opp_grond"] + \
            cityobject["attributes"]["b3_opp_scheidingsmuur"]
    except KeyError as e:
        logging.error(e)
        return None
