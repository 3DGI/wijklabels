import logging
import random
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from itertools import repeat
import argparse

import pandas as pd
import psycopg

from wijklabels.report import aggregate_to_buurt
from wijklabels.load import ExcelLoader
from wijklabels.vormfactor import vormfactorclass, calculate_surface_areas, vormfactor
from wijklabels.labels import parse_energylabel_ditributions, \
    reshape_for_classification, classify
from wijklabels.woningtype import Bouwperiode, WoningtypePreNTA8800, \
    distribute_vbo_on_floor, classify_apartments

log = logging.getLogger("main")
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(filename)s - %(lineno)d - %(message)s')
ch.setFormatter(formatter)
log.addHandler(ch)

# Do we need reproducible randomness?
SEED = 1


# Download the whole database table to a dataframe.
# We use this method instead of the pd.read_sql_table() so that the created dataframe
# is consistent with what comes out of fetch_rows().
def postgres_table_to_df(connection_string, query_select_all, colnames) -> pd.DataFrame:
    with psycopg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute(query_select_all)
            df = pd.DataFrame(cur.fetchall(),
                               columns=colnames).set_index(
                ["vbo_identificatie", "pand_identificatie"])
    return df

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
                               columns=colnames).set_index(
                ["vbo_identificatie", "pand_identificatie"])
            # duplicate_vbos = _df.index.duplicated(keep="first")  #
            # log_validation.info(
            #     f"Removed duplicate VBO which happens when a Pand is split, so there are two different Pand-ID, but the VBO is duplicated {sum(duplicate_vbos)}.")
            conn.commit()
            # cur.close()
    # return _df.loc[~duplicate_vbos, :]
    return _df


def estimate_labels(group, distributions) -> pd.DataFrame | None:
    pid = group.index.get_level_values("pand_identificatie").values[0]
    try:
        # If the Pand has apartements, set the 'woningtype' to one of the
        # apartement types
        if group["vbo_count"].iloc[0] > 1:
            _gc = classify_aparements(group)
            if _gc is not None:
                group = _gc
        group["woningtype_pre_nta8800"] = group.apply(
            lambda row: WoningtypePreNTA8800.from_nta8800(row["woningtype"]),
            axis=1
        )

        group["vormfactor"] = pd.NA
        group["vormfactor"] = group["vormfactor"].astype("Float64")
        group["vormfactorclass"] = pd.NA
        group["vormfactorclass"] = group["vormfactorclass"].astype("object")
        # Compute the vormfactor
        # Update the surface areas for each VBO, so that for instance, an apartement
        # only has its own portion of the total Pand surface areas
        new_surfaces = calculate_surface_areas(group)
        group["vormfactor"] = new_surfaces.apply(
            lambda row: vormfactor(row=row),
            axis=1
        )
        group["vormfactorclass"] = group.apply(
            lambda row: vormfactorclass(row["vormfactor"]),
            axis=1
        )

        # match data
        bouwperiode = group[
            ["oorspronkelijkbouwjaar", "woningtype", "woningtype_pre_nta8800",
             "vormfactorclass", "buurtcode"]].dropna()
        group["bouwperiode"] = bouwperiode.apply(
            lambda row: Bouwperiode.from_year_type(row["oorspronkelijkbouwjaar"],
                                                   row["woningtype_pre_nta8800"]),
            axis=1)
        group["energylabel"] = group.apply(
            lambda row: classify(df=distributions,
                                 woningtype=row["woningtype_pre_nta8800"],
                                 bouwperiode=row["bouwperiode"],
                                 vormfactor=row["vormfactorclass"],
                                 random_number=random.random()),
            axis=1
        )
        return group
    except BaseException as e:
        log.exception(f"Could not process the group {pid}:\n{e}")
        return None


def classify_aparements(group: pd.DataFrame) -> pd.DataFrame | None:
    pid = group.index.get_level_values("pand_identificatie").values[0]
    try:
        vbo_positions = distribute_vbo_on_floor(group)
        if vbo_positions is None:
            log.error(f"vbo_positions is None in {pid}")
            return None
        elif vbo_positions["_position"].isnull().sum() > 0:
            log.error(
                f"did not determine vbo positions for all vbo in {pid}")
        apartment_typen = classify_apartments(vbo_positions)
        try:
            nr_apartments = sum(1 for a in apartment_typen["woningtype"] if
                                a is not pd.NA and "appartement" in a)
        except TypeError as e:
            log.exception(f"TypeError in {pid}:\n{e}")
            return None
        if apartment_typen is None:
            log.error(f"apartment_typen is None in {pid}")
            return None
        elif nr_apartments < len(apartment_typen):
            log.error(
                f"did not determine apartement types for all vbo in {pid}")
        group.loc[apartment_typen.index, "woningtype"] = apartment_typen[
            "woningtype"]
        return group
    except KeyError as e:
        log.exception(f"KeyError in {pid}:\n{e}")
        return None


def parallel_process_labels(groups, distributions, JOBS):
    # Need to preallocate the list, even though normally not needed in Python, because
    # otherwise we get crazy over-allocation with the dataframe items and all the
    # memory leaks away from the machine.
    df_giga_list = [None for i in range(len(groups))]
    with ProcessPoolExecutor(max_workers=JOBS) as executor:
        le = len(groups)
        cntr = 0
        for df in executor.map(estimate_labels,groups, repeat(distributions, le)):
            if cntr % 1000 == 0:
                log.info(f"Processed {cntr}/{le} groups")
            if df is not None:
                df_giga_list[0] = df
            cntr += 1
    return df_giga_list


parser = argparse.ArgumentParser(prog='wijklabels-process')
parser.add_argument('path_output_dir')
parser.add_argument('path_label_distributions')
parser.add_argument('dbname')
parser.add_argument('--host', default='localhost')
parser.add_argument('--port', type=int, default=5432)
parser.add_argument('user')
parser.add_argument('password')
parser.add_argument('-j', '--jobs', type=int, default=4)
parser.add_argument('-s', '--set_size', type=int, default=10000)


def process_cli():
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

    # Download the whole database table
    df_input = postgres_table_to_df(CONNECTION_STRING, QUERY_SELECT_ALL, colnames).drop(columns=["geometrie"])

    # --- Loop
    # A list of one dataframe per pand_identificatie group
    gb = df_input.groupby("pand_identificatie")
    log.info("Splitting input into groups")
    groups = [gb.get_group(g) for g in gb.groups]
    del gb, df_input
    log.info("Calculating attributes")
    df_giga_list = parallel_process_labels(groups, distributions, JOBS)
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


if __name__ == "__main__":
    process_cli()
