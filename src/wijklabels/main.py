import logging
import random

from wijklabels import load
from wijklabels.vormfactor import VormfactorClass, vormfactor
from wijklabels.labels import parse_energylabel_ditributions, reshape_for_classification, classify
from wijklabels.woningtype import Bouwperiode

log = logging.getLogger()
# Do we need reproducible randomness?
SEED = 1

# DEBUG
import os

os.chdir("/home/balazs/Development/wijklabels/src/wijklabels")

if __name__ == "__main__":
    files = ["../../tests/data/9-316-552.city.json", ]
    vbo_csv = "../../tests/data/vbo.csv"
    label_distributions_path = "../../tests/data/Illustraties spreiding Energielabel in WoON2018 per Voorbeeldwoning 2022 - 2023 01 25.xlsx"
    woningtype_path = "../../tests/data/woningtypen.csv"
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

    for coid, co in cm.j["CityObjects"].items():
        if co["type"] == "Building":
            vbo_single = vbo_df.loc[vbo_df["pd_identificatie"] == coid]
            if vbo_single.empty:
                log.error(f"Did not find {coid} in the VBO data")
                continue
            elif len(vbo_single) > 1:
                log.error(
                    f"VBO data frame subset for {coid} returned multiple rows, but there should be only one")
                continue
            vf = vormfactor(cityobject_id=coid, cityobject=co, vbo_df=vbo_df,
                            floor_area=True)
            vbo_df.loc[vbo_df["pd_identificatie"] == coid, "vormfactor"] = vf
            try:
                vbo_df.loc[vbo_df["pd_identificatie"] == coid, "vormfactorclass"] = VormfactorClass.from_vormfactor(vf)
            except ValueError as e:
                pass
            bouwjaar = co["attributes"]["oorspronkelijkbouwjaar"]
            vbo_df.loc[
                vbo_df["pd_identificatie"] == coid, "oorspronkelijkbouwjaar"] = bouwjaar
    vbo_df["oorspronkelijkbouwjaar"] = vbo_df["oorspronkelijkbouwjaar"].astype("Int64")

    vbo_df.to_csv("../../tests/data/vormfactor.csv")

    _d = parse_energylabel_ditributions(excelloader)
    distributions = reshape_for_classification(_d)

    # match data
    panden = vbo_df.merge(woningtype, on="pd_identificatie", how="left")
    bouwperiode = panden[
        ["pd_identificatie", "oorspronkelijkbouwjaar", "woningtype",
         "vormfactorclass"]].dropna()
    bouwperiode["bouwperiode"] = bouwperiode.apply(
        lambda row: Bouwperiode.from_year_type(row["oorspronkelijkbouwjaar"],
                                               row["woningtype"]),
        axis=1)
    bouwperiode["energylabel"] = bouwperiode.apply(
        lambda row: classify(df=distributions,
                             woningtype=row["woningtype"],
                             bouwperiode=row["bouwperiode"],
                             vormfactor=row["vormfactorclass"],
                             random_number=random.random()),
        axis=1
    )
    bouwperiode.to_csv("../../tests/data/results.csv")
