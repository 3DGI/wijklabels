import logging
import random
from pathlib import Path

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import matplotlib.ticker as mtick

from wijklabels.load import VBOLoader, ExcelLoader, WoningtypeLoader, \
    EPLoader, SharedWallsLoader
from wijklabels.vormfactor import vormfactorclass, calculate_surface_areas
from wijklabels.labels import parse_energylabel_ditributions, \
    reshape_for_classification, classify, EnergyLabel
from wijklabels.woningtype import Bouwperiode, Woningtype, WoningtypePreNTA8800, distribute_vbo_on_floor, \
    classify_apartments

log = logging.getLogger("main")
log.setLevel(logging.DEBUG)
# Logger for data validation messages
log_validation = logging.getLogger("VALIDATION")
log_validation.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
ch.setFormatter(formatter)
log_validation.addHandler(ch)

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
        filename = ''.join(e for e in buurt if e.isalnum())
        plt.savefig(f"{dir_plots}/{filename}.png")


if __name__ == "__main__":
    use_gebruiksoppervlakte_for_vormfactor = True
    shared_walls_csv = "../../tests/data/input/rvo_shared_subset_den_haag.csv"
    vbo_csv = "../../tests/data/input/vbo_buurt.csv"
    label_distributions_path = "../../tests/data/input/Illustraties spreiding Energielabel in WoON2018 per Voorbeeldwoning 2022 - 2023 01 25.xlsx"
    woningtype_path = "../../tests/data/input/woningtypen_all_den_haag.csv"
    floors_path = "../../tests/data/input/floors.csv"

    vboloader = VBOLoader(file=vbo_csv)
    _v = vboloader.load()
    log_validation.info(
        f"Loaded BAG Pand {len(_v['identificatie'])}, unique {len(_v['identificatie'].unique())}, VBO {len(_v.index)}, unique {len(_v.index.unique())} from {vbo_csv}")
    duplicate_vbos = _v.index.duplicated(keep="first")
    vbo_df = _v.loc[~duplicate_vbos, :].copy()
    del _v
    log_validation.info(
        f"Removed duplicate VBO which happens when a Pand is split, so there are two different Pand-ID, but the VBO is duplicated {sum(duplicate_vbos)}. Pand {len(vbo_df['identificatie'])}, unique {len(vbo_df['identificatie'].unique())}, VBO {len(vbo_df.index)}, unique {len(vbo_df.index.unique())}")
    excelloader = ExcelLoader(file=label_distributions_path)
    label_distributions_excel = excelloader.load()
    woningtypeloader = WoningtypeLoader(file=woningtype_path)
    _w = woningtypeloader.load()
    _w.set_index("vbo_identificatie", inplace=True)
    # Remove duplicate VBO, which happens when a Pand is split, so there are two
    # different Pand-ID, but the VBO is duplicated
    woningtype_df = _w.loc[~_w.index.duplicated(keep="first"), :].copy()
    del _w
    log_validation.info(
        f"Loaded woningtype Pand {len(woningtype_df['identificatie'])}, unique {len(woningtype_df['identificatie'].unique())}, VBO {len(woningtype_df.index)}, unique {len(woningtype_df.index.unique())} from {woningtype_path}")
    floors_df = pd.read_csv(floors_path, header=0)
    floors_df.set_index("identificatie", inplace=True)
    log_validation.info(
        f"Loaded floors Pand {len(floors_df.index)}, unique {len(floors_df.index.unique())} from {floors_path}")
    shared_walls_loader = SharedWallsLoader(shared_walls_csv)
    shared_walls_df = shared_walls_loader.load().query("_betrouwbaar == True")
    log_validation.info(
        f"Loaded shared walls Pand {len(shared_walls_df.index)}, unique {len(shared_walls_df.index.unique())} from {shared_walls_csv}")

    pand_ids_set_bag = set(vbo_df["identificatie"].unique())
    pand_ids_set_woningtype = set(woningtype_df["identificatie"].unique())
    pand_ids_set_floors = set(floors_df.index.unique())
    pand_ids_set_shared_walls = set(shared_walls_df.index.unique())
    pand_ids_available = pand_ids_set_bag.intersection(pand_ids_set_woningtype).intersection(pand_ids_set_floors).intersection(pand_ids_set_shared_walls)
    log_validation.info(f"Available Pand in all inputs {len(pand_ids_available)}")
    if len(pand_ids_available) == 0:
        raise ValueError(f"The intersection of Pand identificatie of all inputs is empty, there is no data to process.")


    panden = vbo_df.merge(woningtype_df, on="vbo_identificatie", how="inner",
                          validate="1:1", suffixes=("", "_y"))

    pand_ids = vbo_df["identificatie"].unique()
    for pid in pand_ids:
        try:
            vbo_ids = list(vbo_df.loc[vbo_df["identificatie"] == pid].index)
            nr_floors = floors_df.loc[pid, "nr_floors"]
            vbo_count = floors_df.loc[pid, "vbo_count"]
            wtype_pand = woningtype_df.loc[woningtype_df["identificatie"] == pid, "woningtype"].iloc[0]
            vbo_positions = distribute_vbo_on_floor(vbo_ids=vbo_ids,
                                                    nr_floors=nr_floors,
                                                    vbo_count=vbo_count)
            apartment_typen = classify_apartments(woningtype=wtype_pand,
                                                  vbo_positions=vbo_positions)
            for vbo_id, wtype_vbo in apartment_typen:
                panden.loc[vbo_id, "woningtype"] = wtype_vbo
        except KeyError:
            continue
    panden["woningtype_pre_nta8800"] = panden.apply(
        lambda row: WoningtypePreNTA8800.from_nta8800(row["woningtype"]),
        axis=1
    )

    # vbo_df["vormfactor"] = pd.NA
    # vbo_df["vormfactor"] = vbo_df["vormfactor"].astype("Float64")
    panden["vormfactorclass"] = pd.NA
    panden["vormfactorclass"] = panden["vormfactorclass"].astype("object")
    panden.reset_index(inplace=True)
    panden.set_index("identificatie", inplace=True)
    # Compute the vormfactor
    _df = panden.merge(shared_walls_df, on="identificatie", how="inner",
                       suffixes=("", "_y")).merge(floors_df, on="identificatie",
                                                  how="inner", suffixes=("", "_z"))
    # Update the surface areas for each VBO, so that for instance, an apartement
    # only has its own portion of the total Pand surface areas
    groups_with_new_surfaces = []
    for _n, group in _df.groupby("identificatie"):
        try:
            groups_with_new_surfaces.append(calculate_surface_areas(group))
        except BaseException as e:
            log.exception(f"groups_with_new_surfaces for group {_n} returned with exception {e}")
    new_surfaces = pd.concat(groups_with_new_surfaces)

    panden.set_index("vbo_identificatie", inplace=True, append=True)
    panden["vormfactorclass"] = new_surfaces.apply(
        lambda row: vormfactorclass(row=row),
        axis=1
    )
    panden.reset_index(inplace=True)
    panden.set_index("vbo_identificatie", inplace=True)

    # DEBUG
    panden.to_csv("../../tests/data/vormfactor.csv")

    _d = parse_energylabel_ditributions(excelloader)
    distributions = reshape_for_classification(_d)

    # match data
    bouwperiode = panden[["oorspronkelijkbouwjaar", "woningtype", "vormfactorclass",
                          "buurtnaam"]].dropna()
    log_validation.info(f"Available VBO unique {len(bouwperiode.index.unique())}")
    log_validation.info(f"Nr. NA in oorspronkelijkbouwjaar {bouwperiode['oorspronkelijkbouwjaar'].isna().sum()}")
    log_validation.info(f"Nr. NA in woningtype {bouwperiode['woningtype'].isna().sum()}")
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
    eploader = EPLoader(file="/data/energylabel-ep-online/v20231101_v2_csv.csv")
    _g = eploader.load()
    _g.set_index("vbo_identificatie", inplace=True)
    _types_implemented = (
        Woningtype.VRIJSTAAND, Woningtype.TWEE_ONDER_EEN_KAP,
        Woningtype.RIJWONING_TUSSEN,
        Woningtype.RIJWONING_HOEK
    )
    groundtruth = _g.loc[(_g.index.notna() & _g["woningtype"].isin(_types_implemented) &
                          _g["energylabel"].notna()), :]
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
                       rot=0,
                       xlabel="",
                       zorder=3)
        ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=100, decimals=0))
        ax.set_yticks([10, 20, 30, 40, 50, 60, 70, 80])
        plt.grid(visible=True, which="major", axis="y", zorder=0)
        plt.title(f"Nr. woningen: {len(b)}", fontsize=10)
        plt.suptitle(buurt, fontsize=14)
        plt.tight_layout()
        filename = ''.join(e for e in buurt if e.isalnum())
        plt.savefig(
            f"{dir_plots}/{filename}.png")
        plt.close()
