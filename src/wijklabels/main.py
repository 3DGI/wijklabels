import logging
import random
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from itertools import repeat
import argparse

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import matplotlib.ticker as mtick
import psycopg

from wijklabels.load import ExcelLoader
from wijklabels.vormfactor import vormfactorclass, calculate_surface_areas
from wijklabels.labels import parse_energylabel_ditributions, \
    reshape_for_classification, classify, EnergyLabel
from wijklabels.woningtype import Bouwperiode, WoningtypePreNTA8800, \
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
def fetch_rows(connection_string, cursor_start: int, colnames: list,
               query_select_all, set_size) -> pd.DataFrame:
    with psycopg.connect(connection_string) as conn:
        with psycopg.ServerCursor(conn, name="input_cursor") as cur:
            cur.execute(query_select_all)
            cur.scroll(cursor_start, 'absolute')
            _df = pd.DataFrame(cur.fetchmany(size=set_size),
                               columns=colnames).set_index(["vbo_identificatie", "pand_identificatie"])
            # duplicate_vbos = _df.index.duplicated(keep="first")  #
            # log_validation.info(
            #     f"Removed duplicate VBO which happens when a Pand is split, so there are two different Pand-ID, but the VBO is duplicated {sum(duplicate_vbos)}.")
            conn.commit()
            # cur.close()
    # return _df.loc[~duplicate_vbos, :]
    return _df

def estimate_labels(connection_string, cursor_start, distributions: pd.DataFrame,
                    path_output_csv, colnames: list, query_select_all,
                    set_size) -> pd.DataFrame:
    try:
        df_input = fetch_rows(connection_string, cursor_start, colnames,
                              query_select_all,
                              set_size).drop(columns=["geometrie"])
        for pid, group in df_input.groupby("pand_identificatie"):
            if group["vbo_count"].iloc[0] > 1:
                try:
                    vbo_positions = distribute_vbo_on_floor(group)
                    if vbo_positions is None:
                        log.error(f"vbo_positions is None in {pid}")
                        continue
                    elif vbo_positions["_position"].isnull().sum() > 0:
                        log.error(f"did not determine vbo positions for all vbo in {pid}")
                    apartment_typen = classify_apartments(vbo_positions)
                    try:
                        nr_apartments = sum(1 for a in apartment_typen["woningtype"] if a is not pd.NA and "appartement" in a)
                    except TypeError as e:
                        log.exception(f"TypeError in {pid}:\n{e}")
                        continue
                    if apartment_typen is None:
                        log.error(f"apartment_typen is None in {pid}")
                        continue
                    elif nr_apartments < len(apartment_typen):
                        log.error(f"did not determine apartement types for all vbo in {pid}")
                    group["woningtype"] = apartment_typen["woningtype"]
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
        # Compute the vormfactor
        # Update the surface areas for each VBO, so that for instance, an apartement
        # only has its own portion of the total Pand surface areas
        groups_with_new_surfaces = []
        for _pid, group in df_input.groupby("pand_identificatie"):
            try:
                groups_with_new_surfaces.append(calculate_surface_areas(group))
            except BaseException as e:
                log.exception(
                    f"groups_with_new_surfaces for group {_pid} returned with exception {e}")
        new_surfaces = pd.concat(groups_with_new_surfaces)

        df_input["vormfactorclass"] = new_surfaces.apply(
            lambda row: vormfactorclass(row=row),
            axis=1
        )

        # match data
        bouwperiode = df_input[
            ["oorspronkelijkbouwjaar", "woningtype", "woningtype_pre_nta8800",
             "vormfactorclass",
             "buurtcode"]].dropna()
        # log_validation.info(f"Available VBO unique {len(bouwperiode.index.unique())}")
        # log_validation.info(
        #     f"Nr. NA in oorspronkelijkbouwjaar {bouwperiode['oorspronkelijkbouwjaar'].isna().sum()}")
        # log_validation.info(
        #     f"Nr. NA in woningtype {bouwperiode['woningtype'].isna().sum()}")
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
        log.exception(
            f"Could not process the row set at cursor position {cursor_start} + {set_size} with exception\n{e}")
        return None


def parallel_process_labels(CONNECTION_STRING, JOBS, PATH_OUTPUT_DIR, QUERY_SELECT_ALL,
                            SET_SIZE, colnames, cursor_starts, distributions):
    df_giga_list = []
    with ProcessPoolExecutor(max_workers=JOBS) as executor:
        outpaths = [PATH_OUTPUT_DIR.joinpath(f"labels_{i}").with_suffix(".csv") for i in
                    enumerate(cursor_starts)]
        for i, df in enumerate(
                executor.map(estimate_labels,
                             repeat(CONNECTION_STRING, len(cursor_starts)),
                             cursor_starts,
                             repeat(distributions, len(cursor_starts)), outpaths,
                             repeat(colnames, len(cursor_starts)),
                             repeat(QUERY_SELECT_ALL, len(cursor_starts)),
                             repeat(SET_SIZE, len(cursor_starts)))):
            if df is not None:
                df_giga_list.append(df)
            log.info(f"Processed {i} of {len(cursor_starts)} sets")
    return df_giga_list


parser = argparse.ArgumentParser(prog='wijklabels')
parser.add_argument('path_output_dir')
parser.add_argument('path_label_distributions')
parser.add_argument('dbname')
parser.add_argument('--host', default='localhost')
parser.add_argument('--port', type=int, default=5432)
parser.add_argument('user')
parser.add_argument('password')
parser.add_argument('-j', '--jobs', type=int, default=4)
parser.add_argument('-s', '--set_size', type=int, default=10000)


def main_cli():
    args = parser.parse_args()
    PATH_OUTPUT_DIR = Path(args.path_output_dir).resolve()
    PATH_LABEL_DISTRIBUTIONS = Path(args.path_label_distributions).resolve()
    JOBS = args.jobs
    # The number of rows to fetch from the database at once
    SET_SIZE = args.set_size

    CREDS = {
        "host": args.host,
        "user": args.user,
        "port": args.port,
        "dbname": args.dbname,
        "password": args.password
    }
    CONNECTION_STRING = " ".join((f"{k}={v}" for k, v in CREDS.items()))

    QUERY_COUNT = "SELECT count(*) FROM wijklabels.input;"
    QUERY_SELECT_ALL = "SELECT * FROM wijklabels.input;"

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

    use_gebruiksoppervlakte_for_vormfactor = True
    excelloader = ExcelLoader(file=PATH_LABEL_DISTRIBUTIONS)
    _d = parse_energylabel_ditributions(excelloader)
    distributions = reshape_for_classification(_d)

    # --- Loop
    df_giga_list = parallel_process_labels(CONNECTION_STRING, JOBS, PATH_OUTPUT_DIR,
                                           QUERY_SELECT_ALL, SET_SIZE, colnames,
                                           cursor_starts, distributions)
    log.debug(f"Concatenating {len(df_giga_list)} dataframes")
    df_labels_individual = pd.concat(df_giga_list)
    df_labels_individual.to_csv(
        PATH_OUTPUT_DIR
        .joinpath("labels_individual").with_suffix(".csv"))

    # Aggregate per buurt
    log.info("Aggregating the neigbourhoods")
    buurten_labels_wide = aggregate_to_buurt(df_labels_individual,
                                             col_labels="energylabel")
    buurten_labels_wide.to_csv(
        PATH_OUTPUT_DIR
        .joinpath("labels_neighbourhood").with_suffix(".csv"))

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


if __name__ == "__main__":
    main_cli()
