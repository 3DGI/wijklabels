import logging
import random
from pathlib import Path
import argparse
import itertools
from concurrent.futures import ProcessPoolExecutor

import pandas as pd
import psycopg
from psycopg.rows import dict_row

from wijklabels.report import aggregate_to_buurt
from wijklabels.load import ExcelLoader
from wijklabels.vormfactor import calculate_surface_areas, vormfactor, \
    vormfactorclass
from wijklabels.woningtype import distribute_vbo_on_floor, \
    classify_apartments, WoningtypePreNTA8800, Bouwperiode
from wijklabels.labels import estimate_label, parse_energylabel_ditributions, \
    reshape_for_classification


log = logging.getLogger("main")
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(filename)s - %(lineno)d - %(message)s')
ch.setFormatter(formatter)
log.addHandler(ch)

parser = argparse.ArgumentParser(prog='wijklabels-process')
parser.add_argument('path_output_dir')
parser.add_argument('path_label_distributions')
parser.add_argument('dbname')
parser.add_argument('--host', default='localhost')
parser.add_argument('--port', type=int, default=5432)
parser.add_argument('user')
parser.add_argument('password')
parser.add_argument('table', type=str, default='wijklabels.input')
parser.add_argument('-j', '--jobs', type=int, default=4)

# Set seed for the random number generator that is used by the label estimation
random.seed(1, version=2)

def process_cli():
    columns = [
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
    columns_index = ["pand_identificatie", "vbo_identificatie"]
    columns_excluded = ["geometrie"]

    args = parser.parse_args()
    connection_string = f"postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.dbname}"
    path_label_distributions = Path(args.path_label_distributions).resolve()
    path_output_dir = Path(args.path_output_dir).resolve()
    path_output_dir.mkdir(parents=True, exist_ok=True)
    path_output_individual = path_output_dir.joinpath("labels_individual").with_suffix(".csv")
    path_output_aggregate = path_output_dir.joinpath("labels_neighbourhood").with_suffix(".csv")
    jobs = args.jobs
    table = args.table

    query_one = f"SELECT * FROM {table} LIMIT 1;"
    query_pid = f"SELECT DISTINCT pand_identificatie FROM {table};"

    log.info(f"Testing database connection and input table")
    with psycopg.connect(connection_string) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query_one)
            one = cur.fetchone()
            missing = []
            for col in columns:
                if col not in one:
                    missing.append(col)
            if len(missing) > 0:
                raise ValueError(f"Some of the required columns ({missing}) are missing from the input database table {table}.")

    log.info(f"Loading the energy label distributions from {path_label_distributions}")
    excelloader = ExcelLoader(file=path_label_distributions)
    _d = parse_energylabel_ditributions(excelloader)
    distributions = reshape_for_classification(_d)

    log.info("Loading the Pand IDs (identificatie) from the database")
    with psycopg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute(query_pid)
            pand_identificatie_all = [r[0] for r in cur.fetchall()]

    log.info("Calculating attributes and estimating energy labels")
    nr_pand = len(pand_identificatie_all)
    with ProcessPoolExecutor(max_workers=jobs) as executor:
        records = itertools.chain.from_iterable(
            executor.map(
                process_one_pand,
                itertools.repeat(connection_string, nr_pand),
                itertools.repeat(table, nr_pand),
                pand_identificatie_all,
                itertools.repeat(columns_index, nr_pand),
                itertools.repeat(columns_excluded, nr_pand),
                itertools.repeat(distributions, nr_pand)
            )
        )
    df_labels_individual = pd.DataFrame.from_records(records, index=columns_index)

    log.info(f"Writing individual labels to {path_output_individual}")
    df_labels_individual.to_csv(path_output_individual)

    log.info("Aggregating by neighborhood")
    buurten_labels_wide = aggregate_to_buurt(df_labels_individual,
                                             col_labels="energylabel")

    log.info(f"Writing neighbourhood labels to {path_output_aggregate}")
    buurten_labels_wide.to_csv(path_output_aggregate)


def process_one_pand(connection_str: str,
                     table: str,
                     pand_identificatie: str,
                     columns_index: list[str],
                     columns_excluded: list[str],
                     distributions: pd.DataFrame) -> list[dict] | None:
    """Compute some of the required attributes and then estimate the energy labels for
    the Verblijfsobjecten in one Pand.

    The attributes computed in this function are the:
    - dwelling type (woningtype) for apartements
    - form factor (vormfactor)
    - construction year period (bouwperiode)

    :param connection_str: PostgreSQL connection string.
    :param table: Name of the input table.
    :param pand_identificatie: Pand `identificatie` to process.
    :param columns_index: Column names to use as index in the dataframe.
    :param columns_excluded: Column names to exclude from the dataframe.
    :param distributions: Energy label distributions in long-form.
    :return: A list of dictionaries which included calculated attributes in addition to
        the input attributes. Returns `None` if the Pand cannot be processed.
    """
    pand_rows = get_pand(connection_str, table, pand_identificatie)

    pand_df = pd.DataFrame.from_records(pand_rows,
                                        index=columns_index,
                                        exclude=columns_excluded)
    try:
        estimate_apartement_types(pand_df)

        convert_to_pre_nta8800(pand_df)

        calculate_vormfactor(pand_df)

        determine_construction_period(pand_df)

        estimate_labels(pand_df, distributions)

        return pand_df.reset_index().to_dict("records")
    except BaseException as e:
        log.exception(f"Could not process the Pand {pand_identificatie}:\n{e}")
        return None


def estimate_labels(pand_df, distributions: pd.DataFrame) -> None:
    """Estimate the energy label for each Verblijfsobject in the Pand.

    Adds the 'energylabel' column to the input dataframe.
    """
    pand_df["energylabel"] = pand_df.apply(
        lambda row: estimate_label(df=distributions,
                                   woningtype=row["woningtype_pre_nta8800"],
                                   bouwperiode=row["bouwperiode"],
                                   vormfactor=row["vormfactorclass"],
                                   random_number=random.random()),
        axis=1
    )


def determine_construction_period(pand_df):
    """Determine the construction period (bouwperiode) from the construction year
    (oorspronkelijkbouwjaar) and the pre-NTA8800 dwelling type.
    The construction periods are defined in the WoON2022 study.

    Adds the 'bouwperiode' column to the input dataframe.
    """
    bouwperiode = pand_df[["oorspronkelijkbouwjaar", "woningtype",
                           "woningtype_pre_nta8800", "vormfactorclass",
                           "buurtcode"]].dropna()
    pand_df["bouwperiode"] = bouwperiode.apply(
        lambda row: Bouwperiode.from_year_type(row["oorspronkelijkbouwjaar"],
                                               row["woningtype_pre_nta8800"]),
        axis=1)


def convert_to_pre_nta8800(pand_df):
    """Convert the dwelling type (woningtype) to the pre-NTA8800 classification, such as
    maisonette, portiek, galerij, flat.

    Adds the 'woningtype_pre_nta8800' column to the input dataframe.
    """
    pand_df["woningtype_pre_nta8800"] = pand_df.apply(
        lambda row: WoningtypePreNTA8800.from_nta8800(
            row["woningtype"], row["oorspronkelijkbouwjaar"]),
        axis=1
    )


def calculate_vormfactor(pand_df):
    """Calculate the form factor (vormfactor) and its category.

    The form factor categories are defined in the WoON2022 study.

    Adds the 'vormfactor' and 'vormfactorclass' columns to the input dataframe.
    """
    pand_df["vormfactor"] = pd.NA
    pand_df["vormfactor"] = pand_df["vormfactor"].astype("Float64")
    pand_df["vormfactorclass"] = pd.NA
    pand_df["vormfactorclass"] = pand_df["vormfactorclass"].astype("object")
    # Compute the vormfactor
    # Update the surface areas for each VBO, so that for instance, an apartment
    # only has its own portion of the total Pand surface areas
    new_surfaces = calculate_surface_areas(pand_df)
    pand_df["vormfactor"] = new_surfaces.apply(
        lambda row: round(vormfactor(row=row), 2),
        axis=1
    )
    pand_df["vormfactorclass"] = pand_df.apply(
        lambda row: vormfactorclass(row["vormfactor"]),
        axis=1
    )


def estimate_apartement_types(pand_df: pd.DataFrame) -> bool:
    """Estimate the NTA8800 apartement types based on the number of floors in the
    Pand.

    The NTA8800 apartement types are for instance `appartement - hoekmidden`.

    Updates the 'woningtype' column in the input dataframe if the Pand has a
    `vbo_count > 1`.
    """
    if pand_df["vbo_count"].iloc[0] > 1:
        # The Pand has apartements, so we set the 'woningtype' to one of the
        # apartement types
        pand_identificatie = pand_df.index.get_level_values("pand_identificatie").values[0]
        try:
            vbo_positions = distribute_vbo_on_floor(pand_df)
            if vbo_positions is None:
                log.error(f"vbo_positions is None in {pand_identificatie}")
                return False
            elif vbo_positions["_position"].isnull().sum() > 0:
                log.error(
                    f"did not determine vbo positions for all vbo in {pand_identificatie}")
            apartment_typen = classify_apartments(vbo_positions)
            try:
                nr_apartments = sum(1 for a in apartment_typen["woningtype"] if
                                    a is not pd.NA and "appartement" in a)
            except TypeError as e:
                log.exception(f"TypeError in {pand_identificatie}:\n{e}")
                return False
            if apartment_typen is None:
                log.error(f"apartment_typen is None in {pand_identificatie}")
                return False
            elif nr_apartments < len(apartment_typen):
                log.error(f"did not determine apartement types for all vbo in {pand_identificatie}")
            pand_df.loc[apartment_typen.index, "woningtype"] = apartment_typen["woningtype"]
            return True
        except KeyError as e:
            log.exception(f"KeyError in {pand_identificatie}:\n{e}")
            return False


def get_pand(connection_str: str, table: str, pand_identificatie: str) -> list[dict]:
    """Get the records from the database for the given pand identificatie.

    Returns a list of rows as a dictionaries.
    """
    with psycopg.connect(connection_str) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"SELECT * FROM {table} WHERE pand_identificatie = %s",
                (pand_identificatie,)
            )
            return cur.fetchall()


if __name__ == "__main__":
    process_cli()
