import logging
import random
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import matplotlib.ticker as mtick
import psycopg
from psycopg_pool import ConnectionPool

from wijklabels.load import VBOLoader, ExcelLoader, WoningtypeLoader, \
    EPLoader, SharedWallsLoader
from wijklabels.vormfactor import vormfactorclass, calculate_surface_areas
from wijklabels.labels import parse_energylabel_ditributions, \
    reshape_for_classification, classify, EnergyLabel
from wijklabels.woningtype import Bouwperiode, Woningtype, WoningtypePreNTA8800, \
    distribute_vbo_on_floor, \
    classify_apartments

log = logging.getLogger("main")
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(lineno)d - %(message)s')
ch.setFormatter(formatter)
log.addHandler(ch)
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
    buurten_counts = df[["buurtcode"]].value_counts()
    buurten_labels_groups = df[["buurtcode", col_labels]].groupby(
        ["buurtcode", col_labels])
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


# Get a connection from the connection pool.
# Get a server-side cursor.
# Prepare the query and move the cursor to the required position.
# Fetch all rows in the window of cursor_start + SET_SIZE
def fetch_rows(pool: ConnectionPool, cursor_start: int, colnames: list,
               query_select_all, set_size) -> pd.DataFrame:
    with pool.connection() as conn:
        with psycopg.ServerCursor(conn, name="input_cursor") as cur:
            cur.execute(query_select_all)
            cur.scroll(cursor_start, 'absolute')
            _df = pd.DataFrame(cur.fetchmany(size=set_size),
                               columns=colnames).set_index("vbo_identificatie")
            duplicate_vbos = _df.index.duplicated(keep="first")  #
            log_validation.info(
                f"Removed duplicate VBO which happens when a Pand is split, so there are two different Pand-ID, but the VBO is duplicated {sum(duplicate_vbos)}.")
            return _df.loc[~duplicate_vbos, :]


def estimate_labels(pool, cursor_start, distributions: pd.DataFrame,
                    path_output_csv, colnames: list, query_select_all,
                    set_size) -> pd.DataFrame:
    try:
        df_input = fetch_rows(pool, cursor_start, colnames, query_select_all, set_size).drop(columns=["geometrie"])
        pand_ids = df_input["pand_identificatie"].unique()
        for pid in pand_ids:
            try:
                vbo_ids = list(
                    df_input.loc[df_input["pand_identificatie"] == pid].index)
                if len(vbo_ids) == 0:
                    # log.debug(f"Skipping Pand {pid}, because vbo_ids is empty")
                    continue
                nr_floors = \
                    df_input.loc[df_input["pand_identificatie"] == pid, "nr_floors"].iloc[0]
                if nr_floors is None:
                    # log.debug(f"Skipping Pand {pid}, because nr_floors is None")
                    continue
                vbo_count = \
                    df_input.loc[df_input["pand_identificatie"] == pid, "vbo_count"].iloc[0]
                if vbo_count is None:
                    # log.debug(f"Skipping Pand {pid}, because vbo_count is None")
                    continue
                wtype_pand = \
                    df_input.loc[df_input["pand_identificatie"] == pid, "woningtype"].iloc[0]
                if wtype_pand is None:
                    # log.debug(f"Skipping Pand {pid}, because wtype_pand is None")
                    continue
                vbo_positions = distribute_vbo_on_floor(vbo_ids=vbo_ids,
                                                        nr_floors=nr_floors,
                                                        vbo_count=vbo_count)
                apartment_typen = classify_apartments(woningtype=wtype_pand,
                                                      vbo_positions=vbo_positions)
                if apartment_typen is None:
                    log.error(f"apartment_typen is None")
                for vbo_id, wtype_vbo in apartment_typen:
                    df_input.loc[vbo_id, "woningtype"] = wtype_vbo
            except KeyError:
                continue
        df_input["woningtype_pre_nta8800"] = df_input.apply(
            lambda row: WoningtypePreNTA8800.from_nta8800(row["woningtype"]),
            axis=1
        )

        # vbo_df["vormfactor"] = pd.NA
        # vbo_df["vormfactor"] = vbo_df["vormfactor"].astype("Float64")
        df_input["vormfactorclass"] = pd.NA
        df_input["vormfactorclass"] = df_input["vormfactorclass"].astype("object")
        df_input.reset_index(inplace=True)
        df_input.set_index("pand_identificatie", inplace=True)
        # Compute the vormfactor
        # Update the surface areas for each VBO, so that for instance, an apartement
        # only has its own portion of the total Pand surface areas
        groups_with_new_surfaces = []
        for _n, group in df_input.groupby("pand_identificatie"):
            try:
                groups_with_new_surfaces.append(calculate_surface_areas(group))
            except BaseException as e:
                log.exception(
                    f"groups_with_new_surfaces for group {_n} returned with exception {e}")
        new_surfaces = pd.concat(groups_with_new_surfaces)

        df_input.set_index("vbo_identificatie", inplace=True, append=True)
        df_input["vormfactorclass"] = new_surfaces.apply(
            lambda row: vormfactorclass(row=row),
            axis=1
        )
        df_input.reset_index(inplace=True)
        df_input.set_index("vbo_identificatie", inplace=True)

        # match data
        bouwperiode = df_input[
            ["oorspronkelijkbouwjaar", "woningtype", "woningtype_pre_nta8800",
             "vormfactorclass",
             "buurtcode"]].dropna()
        log_validation.info(f"Available VBO unique {len(bouwperiode.index.unique())}")
        log_validation.info(
            f"Nr. NA in oorspronkelijkbouwjaar {bouwperiode['oorspronkelijkbouwjaar'].isna().sum()}")
        log_validation.info(
            f"Nr. NA in woningtype {bouwperiode['woningtype'].isna().sum()}")
        df_input["bouwperiode"] = bouwperiode.apply(
            lambda row: Bouwperiode.from_year_type(row["oorspronkelijkbouwjaar"],
                                                   row["woningtype_pre_nta8800"]),
            axis=1)
        df_input["energylabel"] = df_input.apply(
            lambda row: classify(df=distributions,
                                 woningtype=row["woningtype_pre_nta8800"],
                                 bouwperiode=row["bouwperiode"],
                                 vormfactor=row["vormfactorclass"],
                                 random_number=random.random()),
            axis=1
        )
        # df_input.to_csv(path_output_csv)
        return df_input
    except BaseException as e:
        log.exception(f"Could not process the row set at cursor position {cursor_start} + {set_size} with exception\n{e}")
        return None


if __name__ == "__main__":
    CREDS = {
        "host": "localhost",
        "user": "postgres",
        "port": 8001,
        "dbname": "postgres",
        "password": "password"
    }
    CONNECTION_STRING = " ".join((f"{k}={v}" for k, v in CREDS.items()))

    QUERY_COUNT = "SELECT count(*) FROM wijklabels.input;"
    QUERY_SELECT_ALL = "SELECT * FROM wijklabels.input;"

    # The number of rows to fetch from the database at once
    SET_SIZE = 5000

    # Determine the number of database requests from the number of rows and the required
    # set size.
    # Since we use absolute cursor positions, we also calculate the cursor start position
    # for each set.
    with psycopg.connect(CONNECTION_STRING) as conn:
        with conn.cursor() as cur:
            cur.execute(QUERY_COUNT)
            row_count = cur.fetchone()[0]
    nr_requests = -(row_count // -SET_SIZE)
    cursor_starts = [i * SET_SIZE for i in range(nr_requests)]

    # Column names to use for the dataframe
    colnames = [
        "pand_identificatie",
        "oorspronkelijkbouwjaar",
        "oppervlakte",
        "vbo_identificatie",
        "geometrie",
        "woningtype",
        "buurtcode",
        "nr_floors",
        "vbo_count",
        "b3_opp_buitenmuur",
        "b3_opp_dak_plat",
        "b3_opp_dak_schuin",
        "b3_opp_grond",
        "b3_opp_scheidingsmuur"
    ]

    path_output_dir = Path("../../tests/data/output").resolve()

    use_gebruiksoppervlakte_for_vormfactor = True
    label_distributions_path = "../../tests/data/input/Illustraties spreiding Energielabel in WoON2018 per Voorbeeldwoning 2022 - 2023 01 25.xlsx"
    excelloader = ExcelLoader(file=label_distributions_path)
    _d = parse_energylabel_ditributions(excelloader)
    distributions = reshape_for_classification(_d)

    # --- Loop
    df_giga_list = []
    with ConnectionPool(CONNECTION_STRING) as pool:
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(estimate_labels, pool, cs, distributions,
                                       path_output_dir.joinpath(
                                           f"labels_{i}").with_suffix(".csv"), colnames,
                                       QUERY_SELECT_ALL, SET_SIZE) for i, cs in
                       enumerate(cursor_starts)]
            for future in as_completed(futures):
                _df = future.result()
                if _df is not None:
                    df_giga_list.append(_df)
    log.debug(f"Concatenating {len(df_giga_list)} dataframes")
    df_labels_individual = pd.concat(df_giga_list)
    df_labels_individual.to_csv(
        path_output_dir.joinpath("labels_individual").with_suffix(".csv"))

    # Aggregate per buurt
    log.info("Aggregating the neigbourhoods")
    buurten_labels_wide = aggregate_to_buurt(df_labels_individual,
                                             col_labels="energylabel")
    buurten_labels_wide.to_csv(
        path_output_dir.joinpath("labels_neighbourhood").with_suffix(".csv"))

    # # Plot each buurt
    # plot_buurts("../../plots_estimated", buurten_labels_wide)
    #
    # # Verify quality
    # eploader = EPLoader(file="/data/energylabel-ep-online/v20231101_v2_csv.csv")
    # _g = eploader.load()
    # _g.set_index("vbo_identificatie", inplace=True)
    # _types_implemented = (
    #     Woningtype.VRIJSTAAND, Woningtype.TWEE_ONDER_EEN_KAP,
    #     Woningtype.RIJWONING_TUSSEN,
    #     Woningtype.RIJWONING_HOEK
    # )
    # groundtruth = _g.loc[(_g.index.notna() & _g["woningtype"].isin(_types_implemented) &
    #                       _g["energylabel"].notna()), :]
    # print(bouwperiode)
    # print(groundtruth)
    # _v = bouwperiode.join(groundtruth["energylabel"], how="left",
    #                       rsuffix="_true", validate="1:m")
    # validated = _v.loc[(_v["energylabel_true"].notna() & _v["energylabel"].notna())]
    # buurten_truths_wide = aggregate_to_buurt(validated, "energylabel_true")
    # buurten_truths_wide.to_csv("../../tests/data/output/results_buurten_truths.csv")
    # plot_buurts("../../plots_truth", buurten_truths_wide)
    #
    # # Compare estimated to groundtruth in plots
    # dir_plots = "../../plots_comparison"
    # Path(dir_plots).mkdir(exist_ok=True)
    # for buurt in validated["buurtcode"].unique():
    #     b = validated.loc[
    #         validated["buurtcode"] == buurt, ["energylabel", "energylabel_true"]]
    #     estimated = b["energylabel"].value_counts() / len(b) * 100
    #     truth = b["energylabel_true"].value_counts() / len(b) * 100
    #     b_df = pd.DataFrame({"estimated": estimated, "truth": truth},
    #                         index=[EnergyLabel.APPPP, EnergyLabel.APPP, EnergyLabel.APP,
    #                                EnergyLabel.AP, EnergyLabel.A, EnergyLabel.B,
    #                                EnergyLabel.C, EnergyLabel.D,
    #                                EnergyLabel.E, EnergyLabel.F, EnergyLabel.G])
    #     ax = b_df.plot(kind="bar",
    #                    rot=0,
    #                    xlabel="",
    #                    zorder=3)
    #     ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=100, decimals=0))
    #     ax.set_yticks([10, 20, 30, 40, 50, 60, 70, 80])
    #     plt.grid(visible=True, which="major", axis="y", zorder=0)
    #     plt.title(f"Nr. woningen: {len(b)}", fontsize=10)
    #     plt.suptitle(buurt, fontsize=14)
    #     plt.tight_layout()
    #     filename = ''.join(e for e in buurt if e.isalnum())
    #     plt.savefig(
    #         f"{dir_plots}/{filename}.png")
    #     plt.close()
