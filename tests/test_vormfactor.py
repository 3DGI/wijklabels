from wijklabels.load import CityJSONLoader
from wijklabels.vormfactor import verliesoppervlakte, vormfactor


def test_verliesoppervlakte(data_dir):
    p = data_dir / "one.city.json"
    cmloader = CityJSONLoader()
    cm = cmloader.load(files=[p, ])
    for coid, co in cm.j["CityObjects"].items():
        print(f"Computing verliesoppervlak of {coid}")
        res = verliesoppervlakte(co)
        print(res)


def test_vormfactor(data_dir):
    p = data_dir / "one.city.json"
    cmloader = CityJSONLoader()
    cm = cmloader.load(files=[p, ])
    for coid, co in cm.j["CityObjects"].items():
        print(f"Computing vormfactor of {coid}")
        res = vormfactor(co)
        print(res)
