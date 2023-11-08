from wijklabels.load import CityJSONLoader
from wijklabels.vormfactor import verliesoppervlakte, vormfactor


def test_verliesoppervlakte(data_dir):
    p = data_dir / "one.city.json"
    cmloader = CityJSONLoader(files=[p, ])
    cm = cmloader.load()
    for coid, co in cm.j["CityObjects"].items():
        print(f"Computing verliesoppervlak of {coid}")
        res = verliesoppervlakte(co)
        print(res)


def test_vormfactor(data_dir, vbo_df):
    p = data_dir / "one.city.json"
    cmloader = CityJSONLoader(files=[p, ])
    cm = cmloader.load()
    for coid, co in cm.j["CityObjects"].items():
        print(f"Computing vormfactor of {coid}")
        res = vormfactor(cityobject_id=coid, cityobject=co, vbo_df=vbo_df,
                         floor_area=True)
        print(res)
