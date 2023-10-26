import logging
from wijklabels import load, vormfactor

if __name__ == "__main__":
    files = ["../../tests/data/9-316-552.city.json",]
    vbo_csv = "../../tests/data/vbo.csv"
    cmloader = load.CityJSONLoader(files=files)
    cm = cmloader.load()
    vboloader = load.VBOLoader(file=vbo_csv)
    vbo_df = vboloader.load()

    # We select only those Pand that have a single VBO, which means that they are
    # houses, not appartaments
    pdcnt = vbo_df.groupby("pd_identificatie").count()
    houses = pdcnt[pdcnt["huisnummer"] == 1].index

    vbo_df["vormfactor"] = None
    for h_id in houses:
        try:
            co = cm.j["CityObjects"][h_id]
        except KeyError:
            logging.error(f"Did not find {h_id} in the city model")
            continue
        vbo_single = vbo_df.loc[vbo_df["pd_identificatie"] == h_id]
        if vbo_single.empty:
            logging.error(f"Did not find {h_id} in the VBO data frame")
            continue
        elif len(vbo_single) > 1:
            logging.error(f"VBO data frame subset for {h_id} returned multiple rows, but there should be on one")
            continue
        vf = vormfactor.vormfactor(cityobject_id=h_id, cityobject=co, vbo_df=vbo_df)
        vbo_df.loc[vbo_df["pd_identificatie"] == h_id, "vormfactor"] = vf

    print(vbo_df.head())
    vbo_df.to_csv("../../tests/data/vormfactor.csv")

    print("done")