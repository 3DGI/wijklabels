import logging
import random
import csv
from pathlib import Path

import numpy as np
from matplotlib import pyplot as plt
import matplotlib.ticker as mtick

from wijklabels import load
from wijklabels.vormfactor import VormfactorClass, vormfactor
from wijklabels.labels import parse_energylabel_ditributions, \
    reshape_for_classification, classify, EnergyLabel
from wijklabels.woningtype import Bouwperiode

log = logging.getLogger()
# Do we need reproducible randomness?
SEED = 1

# DEBUG
os.chdir("/home/balazs/Development/wijklabels/src/wijklabels")

if __name__ == "__main__":
    files = ["../../tests/data/9-316-552.city.json",
             "../../tests/data/9-316-556.city.json"]
    files += ["../../tests/data/9-312-552.city.json",
              "../../tests/data/9-312-556.city.json",
              "../../tests/data/9-320-552.city.json",
              "../../tests/data/9-320-556.city.json",
              "../../tests/data/9-324-552.city.json",
              "../../tests/data/9-324-556.city.json",
              ]
    vbo_csv = "../../tests/data/vbo_buurt.csv"
    label_distributions_path = "../../tests/data/Illustraties spreiding Energielabel in WoON2018 per Voorbeeldwoning 2022 - 2023 01 25.xlsx"
    woningtype_path = "../../tests/data/woningtypen.csv"
    cmloader = load.CityJSONLoader(files=files)
    cm = cmloader.load()
    # We select only those Pand that have a single VBO, which means that they are
    # houses, not appartaments
    vboloader = load.VBOLoader(file=vbo_csv)
    vbo_df = vboloader.load()
    excelloader = load.ExcelLoader(file=label_distributions_path)
    label_distributions_excel = excelloader.load()
    woningtypeloader = load.WoningtypeLoader(file=woningtype_path)
    woningtype = woningtypeloader.load()

    coid_in_cityjson = []
    for coid, co in cm.j["CityObjects"].items():
        if co["type"] == "Building":
            try:
                vbo_single = vbo_df.loc[coid]
            except KeyError:
                coid_in_cityjson.append((coid, False))
                continue
            if vbo_single.empty:
                log.error(f"Did not find {coid} in the VBO data")
                coid_in_cityjson.append((coid, False))
                continue
            coid_in_cityjson.append((coid, True))
            vf = vormfactor(cityobject_id=coid, cityobject=co, vbo_df=vbo_df,
                            floor_area=True)
            vbo_df.loc[coid, "vormfactor"] = vf
            try:
                vbo_df.loc[coid, "vormfactorclass"] = VormfactorClass.from_vormfactor(
                    vf)
            except ValueError as e:
                pass
            bouwjaar = co["attributes"]["oorspronkelijkbouwjaar"]
            vbo_df.loc[coid, "oorspronkelijkbouwjaar"] = bouwjaar
    vbo_df["oorspronkelijkbouwjaar"] = vbo_df["oorspronkelijkbouwjaar"].astype("Int64")

    # DEBUG
    with open("../../tests/data/coid_in_cityjson.csv", "w") as fo:
        csvwriter = csv.writer(fo)
        csvwriter.writerow(("identificatie", "found_in_vbo"))
        csvwriter.writerows(coid_in_cityjson)
    vbo_df.to_csv("../../tests/data/vormfactor.csv")

    _d = parse_energylabel_ditributions(excelloader)
    distributions = reshape_for_classification(_d)

    # match data
    panden = vbo_df.merge(woningtype, on="identificatie", how="left")
    bouwperiode = panden[
        ["identificatie", "oorspronkelijkbouwjaar", "woningtype",
         "vormfactorclass", "buurtnaam"]].dropna()
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

    # Aggregate per buurt
    buurten_counts = bouwperiode[["buurtnaam"]].value_counts()
    buurten_labels_groups = bouwperiode[["buurtnaam", "energylabel"]].groupby(
        ["buurtnaam", "energylabel"])
    buurten_labels_distribution = (
            buurten_labels_groups.value_counts() / buurten_counts).to_frame(
        name="fraction")
    buurten_labels_wide = buurten_labels_distribution.reset_index(level=1).pivot(
        columns="energylabel", values="fraction")
    for label in EnergyLabel:
        if label not in buurten_labels_wide:
            buurten_labels_wide[label] = np.nan
    buurten_labels_wide = buurten_labels_wide[
        [EnergyLabel.APPPP, EnergyLabel.APPP, EnergyLabel.APP,
         EnergyLabel.AP, EnergyLabel.A, EnergyLabel.B, EnergyLabel.C, EnergyLabel.D,
         EnergyLabel.E, EnergyLabel.F, EnergyLabel.G]]
    buurten_labels_wide.to_csv("../../tests/data/results_buurten.csv")

    # Plot each buurt
    Path("../../plots").mkdir(exist_ok=True)
    for buurt in buurten_labels_wide.index:
        ax = (buurten_labels_wide.loc[buurt] * 100).plot(
            kind="bar",
            title=buurt,
            color={"#1a9641": EnergyLabel.APPPP,
                   "#52b151": EnergyLabel.APPP,
                   "#8acc62": EnergyLabel.APP,
                   "#b8e17b": EnergyLabel.AP,
                   "#dcf09e": EnergyLabel.A,
                   "#ffffc0": EnergyLabel.B,
                   "#ffdf9a": EnergyLabel.C,
                   "#febe74": EnergyLabel.D,
                   "#f69053": EnergyLabel.E,
                   "#e75437": EnergyLabel.F,
                   "#d7191c": EnergyLabel.G
            },
            rot=0,
            xlabel="",
            zorder=3
        )
        ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=100, decimals=0))
        ax.set_yticks([10, 20, 30, 40, 50, 60, 70, 80])
        plt.grid(visible=True, which="major", axis="y", zorder=0)
        plt.tight_layout()
        plt.savefig(f"../../plots/{buurt.lower().replace(' ', '-').replace('.', '')}.png")
