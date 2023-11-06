import logging

import pandas as pd

from wijklabels import load, vormfactor
from wijklabels.labels import parse_energylabel_ditributions

log = logging.getLogger()

if __name__ == "__main__":
    files = ["../../tests/data/9-316-552.city.json", ]
    vbo_csv = "../../tests/data/vbo.csv"
    label_distributions_path = "../../resources/Illustraties spreiding Energielabel in WoON2018 per Voorbeeldwoning 2022 - 2023 01 25.xlsx"
    woningtype_path = "../../tests/data/tmp_clusters.csv"
    cmloader = load.CityJSONLoader(files=files)
    cm = cmloader.load()
    vboloader = load.VBOLoader(file=vbo_csv)
    vbo_df = vboloader.load()
    excelloader = load.ExcelLoader(file=label_distributions_path)
    label_distributions_excel = excelloader.load()
    woningtypeloader = load.WoningtypeLoader(file=woningtype_path)
    woningtype = woningtypeloader.load()
    woningtype.rename(columns={"identificatie": "pd_identificatie"}, inplace=True)

    # We select only those Pand that have a single VBO, which means that they are
    # houses, not appartaments
    pdcnt = vbo_df.groupby("pd_identificatie").count()
    houses = pdcnt[pdcnt["huisnummer"] == 1].index

    for h_id in houses:
        try:
            co = cm.j["CityObjects"][h_id]
        except KeyError:
            log.error(f"Did not find {h_id} in the city model")
            continue
        vbo_single = vbo_df.loc[vbo_df["pd_identificatie"] == h_id]
        if vbo_single.empty:
            log.error(f"Did not find {h_id} in the VBO data frame")
            continue
        elif len(vbo_single) > 1:
            log.error(
                f"VBO data frame subset for {h_id} returned multiple rows, but there should be on one")
            continue
        vf = vormfactor.vormfactor(cityobject_id=h_id, cityobject=co, vbo_df=vbo_df)
        vbo_df.loc[vbo_df["pd_identificatie"] == h_id, "vormfactor"] = vf
        bouwjaar = co["attributes"]["oorspronkelijkbouwjaar"]
        vbo_df.loc[vbo_df["pd_identificatie"] == h_id, "oorspronkelijkbouwjaar"] = int(bouwjaar)
    vbo_df["oorspronkelijkbouwjaar"] = vbo_df["oorspronkelijkbouwjaar"].astype("Int64")

    vbo_df.to_csv("../../tests/data/vormfactor.csv")

    distributions = parse_energylabel_ditributions(
        label_distributions_excel=label_distributions_excel,
        label_distributions_path=label_distributions_path)

    # match data
    panden = vbo_df.merge(woningtype, on="pd_identificatie", how="left")

    panden.groupby(by=["woningtype", "oorspronkelijkbouwjaar"]).count()

    print("done")
