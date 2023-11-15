import logging
import random
import csv
from pathlib import Path

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import matplotlib.ticker as mtick

from wijklabels.load import CityJSONLoader, VBOLoader, ExcelLoader, WoningtypeLoader, \
    EPLoader
from wijklabels.vormfactor import VormfactorClass, vormfactor
from wijklabels.labels import parse_energylabel_ditributions, \
    reshape_for_classification, classify, EnergyLabel
from wijklabels.woningtype import Bouwperiode, Woningtype

log = logging.getLogger()
# Do we need reproducible randomness?
SEED = 1
COLORS = {"#1a9641": EnergyLabel.APPPP,
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
          }

# DEBUG
import os

os.chdir("/home/balazs/Development/wijklabels/src/wijklabels")



def aggregate_to_buurt(df: pd.DataFrame, col_labels: str) -> pd.DataFrame:
    buurten_counts = df[["buurtnaam"]].value_counts()
    buurten_labels_groups = df[["buurtnaam", col_labels]].groupby(
        ["buurtnaam", col_labels])
    buurten_labels_distribution = (
            buurten_labels_groups.value_counts() / buurten_counts).to_frame(
        name="fraction")
    buurten_labels_wide = buurten_labels_distribution.reset_index(level=1).pivot(
        columns=col_labels, values="fraction")
    for label in EnergyLabel:
        if label not in buurten_labels_wide:
            buurten_labels_wide[label] = np.nan
    buurten_labels_wide = buurten_labels_wide[
        [EnergyLabel.APPPP, EnergyLabel.APPP, EnergyLabel.APP,
         EnergyLabel.AP, EnergyLabel.A, EnergyLabel.B, EnergyLabel.C, EnergyLabel.D,
         EnergyLabel.E, EnergyLabel.F, EnergyLabel.G]]
    return buurten_labels_wide


def plot_buurts(dir_plots: str, df: pd.DataFrame):
    Path(dir_plots).mkdir(exist_ok=True)
    for buurt in df.index:
        ax = (df.loc[buurt] * 100).plot(
            kind="bar",
            title=buurt,
            color=COLORS,
            rot=0,
            xlabel="",
            zorder=3
        )
        ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=100, decimals=0))
        ax.set_yticks([10, 20, 30, 40, 50, 60, 70, 80])
        plt.grid(visible=True, which="major", axis="y", zorder=0)
        plt.tight_layout()
        plt.savefig(f"{dir_plots}/{buurt.lower().replace(' ', '-').replace('.', '')}.png")


if __name__ == "__main__":
    use_gebruiksoppervlakte_for_vormfactor = False
    files = ["../../tests/data/input/9-316-552.city.json",
             "../../tests/data/input/9-316-556.city.json"]
    files += ["../../tests/data/input/9-312-552.city.json",
              "../../tests/data/input/9-312-556.city.json",
              "../../tests/data/input/9-320-552.city.json",
              "../../tests/data/input/9-320-556.city.json",
              "../../tests/data/input/9-324-552.city.json",
              "../../tests/data/input/9-324-556.city.json",
              ]
    vbo_csv = "../../tests/data/input/vbo_buurt.csv"
    label_distributions_path = "../../tests/data/input/Illustraties spreiding Energielabel in WoON2018 per Voorbeeldwoning 2022 - 2023 01 25.xlsx"
    woningtype_path = "../../tests/data/input/woningtypen.csv"
    cmloader = CityJSONLoader(files=files)
    cm = cmloader.load()
    # We select only those Pand that have a single VBO, which means that they are
    # houses, not appartaments
    vboloader = VBOLoader(file=vbo_csv)
    vbo_df = vboloader.load()
    excelloader = ExcelLoader(file=label_distributions_path)
    label_distributions_excel = excelloader.load()
    woningtypeloader = WoningtypeLoader(file=woningtype_path)
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
                            floor_area=use_gebruiksoppervlakte_for_vormfactor)
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
    bouwperiode.to_csv("../../tests/data/output/results_individual_labels.csv")

    # Aggregate per buurt
    buurten_labels_wide = aggregate_to_buurt(bouwperiode, col_labels="energylabel")
    buurten_labels_wide.to_csv("../../tests/data/output/results_buurten.csv")

    # Plot each buurt
    plot_buurts("../../plots_estimated", buurten_labels_wide)

    # Verify quality
    bouwperiode.set_index("identificatie", inplace=True)
    eploader = EPLoader(file="/data/energylabel-ep-online/v20231101_v2_csv.csv")
    _g = eploader.load()
    _g.set_index("identificatie", inplace=True)
    _types_implemented = (
        Woningtype.VRIJSTAAND, Woningtype.TWEE_ONDER_EEN_KAP, Woningtype.RIJWONING_TUSSEN,
        Woningtype.RIJWONING_HOEK
    )
    groundtruth = _g.loc[(_g.index.notna() & _g["woningtype"].isin(_types_implemented) & _g["energylabel"].notna()), :]
    print(bouwperiode)
    print(groundtruth)
    _v = bouwperiode.join(groundtruth["energylabel"], how="left",
                                 rsuffix="_true", validate="1:m")
    validated = _v.loc[(_v["energylabel_true"].notna() & _v["energylabel"].notna())]
    buurten_truths_wide = aggregate_to_buurt(validated, "energylabel_true")
    buurten_truths_wide.to_csv("../../tests/data/output/results_buurten_truths.csv")
    plot_buurts("../../plots_truth", buurten_truths_wide)

    # Compare estimated to groundtruth in plots
    dir_plots = "../../plots_comparison"
    Path(dir_plots).mkdir(exist_ok=True)
    for buurt in validated["buurtnaam"].unique():
        b = validated.loc[
            validated["buurtnaam"] == buurt, ["energylabel", "energylabel_true"]]
        estimated = b["energylabel"].value_counts() / len(b) * 100
        truth = b["energylabel_true"].value_counts() / len(b) * 100
        b_df = pd.DataFrame({"estimated": estimated, "truth": truth},
                            index=[EnergyLabel.APPPP, EnergyLabel.APPP, EnergyLabel.APP,
                                   EnergyLabel.AP, EnergyLabel.A, EnergyLabel.B,
                                   EnergyLabel.C, EnergyLabel.D,
                                   EnergyLabel.E, EnergyLabel.F, EnergyLabel.G])
        ax = b_df.plot(kind="bar",
                       title=buurt,
                       rot=0,
                       xlabel="",
                       zorder=3)
        ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=100, decimals=0))
        ax.set_yticks([10, 20, 30, 40, 50, 60, 70, 80])
        plt.grid(visible=True, which="major", axis="y", zorder=0)
        plt.tight_layout()
        plt.savefig(
            f"{dir_plots}/{buurt.lower().replace(' ', '-').replace('.', '')}.png")
