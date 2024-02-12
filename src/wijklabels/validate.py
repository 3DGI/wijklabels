import argparse
import itertools
from pathlib import Path
import logging

import pandas as pd

from wijklabels import AggregateUnit
from wijklabels.report import (aggregate_to_unit, plot_comparison,
                               calculate_distance_stats_for_area)
from wijklabels.load import EPLoader, ExcelLoader
from wijklabels.labels import parse_energylabel_ditributions, \
    reshape_for_classification, EnergyLabel
from wijklabels.vormfactor import VormfactorClass
from wijklabels.woningtype import WoningtypePreNTA8800, Bouwperiode

# Logger for data validation messages
log = logging.getLogger("VALIDATION")
log.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
ch.setFormatter(formatter)
log.addHandler(ch)


def join_with_ep_online(estimated_labels: pd.DataFrame,
                        ep_online: pd.DataFrame) -> pd.DataFrame:
    """Join the the dataframe with the estimated labels onto the EP-Online dataframe."""
    groundtruth = ep_online.loc[ep_online["energylabel"].notna(), :]
    _v = estimated_labels.join(groundtruth["energylabel"], how="left",
                               rsuffix="_ep_online", validate="1:m")
    validated = _v.loc[
        (_v["energylabel_ep_online"].notna() & _v["energylabel"].notna())]
    return validated


def calculate_accuracy(df_with_truth: pd.DataFrame, within, woningtype=None):
    matches = df_with_truth.apply(
        lambda row: row.energylabel.within(row.energylabel_ep_online, within),
        axis=1)
    if woningtype is None:
        types_selected = df_with_truth.apply(lambda row: True, axis=1)
    elif woningtype == "eengezins":
        types_selected = ~df_with_truth["woningtype"].str.contains("appartement")
    elif woningtype == "meergezins":
        types_selected = df_with_truth["woningtype"].str.contains("appartement")
    else:
        raise NotImplementedError
    nr_matches = (matches & types_selected).sum()
    nr_total = len(df_with_truth.loc[df_with_truth["energylabel_ep_online"].notnull()])
    return nr_matches / nr_total


parser_validate = argparse.ArgumentParser(prog='wijklabels-validate')
parser_validate.add_argument('path_estimated_labels_csv')
parser_validate.add_argument('path_ep_online_csv')
parser_validate.add_argument('path_label_distributions_xlsx')
parser_validate.add_argument('path_output_dir')
parser_validate.add_argument('-e', '--energylabel', default="energylabel",
                             help="Name of the column that contains the energy labels.")
parser_validate.add_argument('--plot', action='store_true',
                             help="Produce a diagram for each neighborhood, comparing the estimated labels to the EP-Online labels. The diagrams are placed into the provided directory.")


def validate_cli():
    args = parser_validate.parse_args()
    # args = parser_validate.parse_args([
    #     "/home/balazs/Development/wijklabels/tests/data/output/labels_individual.csv",
    #     "/data/energylabel-ep-online/v20231101_v2_csv_subset.csv",
    #     "/data/wijklabels/Illustraties spreiding Energielabel in WoON2018 per Voorbeeldwoning 2022 - 2023 01 25.xlsx",
    #     "/home/balazs/Development/wijklabels/tests/data/output/distribution",
    #     "-e", "energylabel"
    # ])
    p_ep = Path(args.path_ep_online_csv).resolve()
    p_el = Path(args.path_estimated_labels_csv).resolve()
    p_dist = Path(args.path_label_distributions_xlsx).resolve()
    PATH_OUTPUT_DIR = Path(args.path_output_dir).resolve()
    PATH_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    log.info("Loading data")
    estimated_labels_df = pd.read_csv(
        p_el,
        converters={
            args.energylabel: EnergyLabel.from_str,
            "bouwperiode": Bouwperiode.from_str,
            "vormfactorclass": VormfactorClass.from_str,
            "woningtype_pre_nta8800": WoningtypePreNTA8800.from_str
        }).set_index(
        ["vbo_identificatie", "pand_identificatie"]
    )
    if args.energylabel not in estimated_labels_df.columns:
        raise ValueError(f"Did not find the required energy label column {args.energylabel} in the input")
    if args.energylabel != "energylabel":
        if "energylabel" in estimated_labels_df.columns:
            estimated_labels_df.rename(columns={"energylabel": "energylabel_bak"}, inplace=True)
        estimated_labels_df.rename(columns={args.energylabel: "energylabel"}, inplace=True)

    ep_online_df = EPLoader(file=p_ep).load()
    df_with_truth = join_with_ep_online(estimated_labels=estimated_labels_df,
                                        ep_online=ep_online_df)

    log.info("Computing estimated label distance to EP-Online labels")
    distance_column = "energylabel_dist_est_ep"
    df_with_truth[distance_column] = df_with_truth.apply(
        lambda row: row["energylabel_ep_online"].distance(row["energylabel"]),
        axis=1
    )

    log.info("Comparing the EP-Online labels to the Voorbeeldwoningen 2022 distributions")
    excelloader = ExcelLoader(file=p_dist)
    _d = parse_energylabel_ditributions(excelloader)
    distributions = reshape_for_classification(_d)

    # Compare individual addresses
    def _ep_in_dist(df_dist, row):
        el_ep_online = row['energylabel_ep_online']
        try:
            return df_dist.loc[
            (row["woningtype_pre_nta8800"], row["bouwperiode"], row["vormfactorclass"]),
            :].query("energylabel == @el_ep_online").probability.notnull().values[0]
        except KeyError:
            return False

    df_with_truth["ep_online_label_in_distributions"] = df_with_truth.apply(
        lambda row: _ep_in_dist(distributions, row),
        axis=1
    )

    p_out = PATH_OUTPUT_DIR.joinpath("labels_individual_ep_online").with_suffix(".csv")
    log.info(f"Writing output to {p_out}")
    df_with_truth.to_csv(p_out)

    nr_no_label = estimated_labels_df["energylabel"].isnull().sum()
    nr_total = len(estimated_labels_df)
    log.info(f"Missing energy label because of gap in energy label distributions in Voorbeeldwoningen 2022: {round(nr_no_label / nr_total * 100)}%")

    within_range = 0
    accuracy_exact = calculate_accuracy(df_with_truth, within=within_range)
    log.info(f"Exact match accuracy {round(accuracy_exact * 100)}%")
    accuracy_exact_eengezins = calculate_accuracy(df_with_truth, within=within_range, woningtype="eengezins")
    log.info(f"Exact match accuracy for eengezinswoningen {round(accuracy_exact_eengezins * 100)}%")
    accuracy_exact_meergezins = calculate_accuracy(df_with_truth, within=within_range, woningtype="meergezins")
    log.info(f"Exact match accuracy for meergezinswoningen {round(accuracy_exact_meergezins * 100)}%")

    within_range = 1
    accuracy_one_dev = calculate_accuracy(df_with_truth, within=within_range)
    log.info(f"Accuracy within one label distance {round(accuracy_one_dev * 100)}%")
    accuracy_one_dev_eengezins = calculate_accuracy(df_with_truth, within=within_range, woningtype="eengezins")
    log.info(f"Accuracy within one label distance for eengezinswoningen {round(accuracy_one_dev_eengezins * 100)}%")
    accuracy_one_dev_meergezins = calculate_accuracy(df_with_truth, within=within_range, woningtype="meergezins")
    log.info(f"Accuracy within one label distance for meergezinswoningen {round(accuracy_one_dev_meergezins * 100)}%")

    nr_impossible_labels = len(df_with_truth[df_with_truth["ep_online_label_in_distributions"] == False])
    nr_total = len(df_with_truth)
    log.info(f"Labels in EP-Online that do not have a corresponding probability in the Voorbeeldwoningen 2022 data: {round(nr_impossible_labels / nr_total * 100)}%")

    possible_labels = df_with_truth[df_with_truth["ep_online_label_in_distributions"] == True]
    within_range = 0
    accuracy_exact = calculate_accuracy(possible_labels, within=within_range)
    log.info(f"Exact match accuracy of possible labels {round(accuracy_exact * 100)}%")
    accuracy_exact_eengezins = calculate_accuracy(possible_labels, within=within_range, woningtype="eengezins")
    log.info(f"Exact match accuracy of possible labels for eengezinswoningen {round(accuracy_exact_eengezins * 100)}%")
    accuracy_exact_meergezins = calculate_accuracy(possible_labels, within=within_range, woningtype="meergezins")
    log.info(f"Exact match accuracy of possible labels for meergezinswoningen {round(accuracy_exact_meergezins * 100)}%")

    within_range = 1
    accuracy_one_dev = calculate_accuracy(possible_labels, within=within_range)
    log.info(f"Accuracy within one label distance of possible labels {round(accuracy_one_dev * 100)}%")
    accuracy_one_dev_eengezins = calculate_accuracy(possible_labels, within=within_range, woningtype="eengezins")
    log.info(f"Accuracy within one label distance of possible labels for eengezinswoningen {round(accuracy_one_dev_eengezins * 100)}%")
    accuracy_one_dev_meergezins = calculate_accuracy(possible_labels, within=within_range, woningtype="meergezins")
    log.info(f"Accuracy within one label distance of possible labels for meergezinswoningen {round(accuracy_one_dev_meergezins * 100)}%")


    # Aggregate per buurt
    log.info("Aggregating the neigbourhoods")
    gen_dist_nl = aggregate_to_unit(df_with_truth, "energylabel",
                                    AggregateUnit.NL)
    gen_dist_gem = aggregate_to_unit(df_with_truth, "energylabel",
                                    AggregateUnit.GEMEENTE)
    gen_dist_wij = aggregate_to_unit(df_with_truth, "energylabel",
                                    AggregateUnit.WIJK)
    gen_dist_buu = aggregate_to_unit(df_with_truth, "energylabel",
                                    AggregateUnit.BUURT)
    df_distributions_units = pd.DataFrame.from_records(
        itertools.chain(gen_dist_nl, gen_dist_gem, gen_dist_wij, gen_dist_buu),
        index="unit_code"
    )
    ## ep-online
    gen_dist_nl = aggregate_to_unit(df_with_truth, "energylabel_ep_online",
                                    AggregateUnit.NL)
    gen_dist_gem = aggregate_to_unit(df_with_truth, "energylabel_ep_online",
                                    AggregateUnit.GEMEENTE)
    gen_dist_wij = aggregate_to_unit(df_with_truth, "energylabel_ep_online",
                                    AggregateUnit.WIJK)
    gen_dist_buu = aggregate_to_unit(df_with_truth, "energylabel_ep_online",
                                    AggregateUnit.BUURT)
    df_distributions_units_ep_online = pd.DataFrame.from_records(
        itertools.chain(gen_dist_nl, gen_dist_gem, gen_dist_wij, gen_dist_buu),
        index="unit_code"
    )

    log.info("Analysing estimated and EP-Online deviations (per address)")
    gen_nl = calculate_distance_stats_for_area(df_with_truth, AggregateUnit.NL,
                                               distance_column)
    gen_gem = calculate_distance_stats_for_area(df_with_truth, AggregateUnit.GEMEENTE,
                                                distance_column)
    gen_wij = calculate_distance_stats_for_area(df_with_truth, AggregateUnit.WIJK,
                                                distance_column)
    gen_buu = calculate_distance_stats_for_area(df_with_truth, AggregateUnit.BUURT,
                                                distance_column)
    df_distance_stats = pd.DataFrame.from_records(
        itertools.chain(gen_nl, gen_gem, gen_wij, gen_buu),
        index="unit_code"
    )

    log.info("Analysing estimated and EP-Online deviations (aggregated)")
    df_dist_long_est = pd.melt(df_distributions_units.reset_index(), id_vars=["unit_code"],
                           value_vars=list(EnergyLabel), var_name="energylabel",
                           value_name="probability").set_index(
        ["unit_code", "energylabel"])
    df_dist_long_ep = pd.melt(df_distributions_units_ep_online.reset_index(),
                                     id_vars=["unit_code"],
                                     value_vars=list(EnergyLabel),
                                     var_name="energylabel",
                                     value_name="probability").set_index(
        ["unit_code", "energylabel"])
    df_dist_long = df_dist_long_est.join(df_dist_long_ep, rsuffix="_ep_online")
    df_dist_long["difference"] = df_dist_long["probability"] - df_dist_long["probability_ep_online"]



    p_out = PATH_OUTPUT_DIR.joinpath("labels_neighbourhood_ep_online").with_suffix(".csv")
    log.info(f"Writing output to {p_out}")
    df_distance_stats.join(df_distributions_units).to_csv(p_out)

    # Possible labels only
    log.info("Aggregating the neigbourhoods of possible labels")
    gen_dist_nl = aggregate_to_unit(possible_labels, "energylabel",
                                    AggregateUnit.NL)
    gen_dist_gem = aggregate_to_unit(possible_labels, "energylabel",
                                    AggregateUnit.GEMEENTE)
    gen_dist_wij = aggregate_to_unit(possible_labels, "energylabel",
                                    AggregateUnit.WIJK)
    gen_dist_buu = aggregate_to_unit(possible_labels, "energylabel",
                                    AggregateUnit.BUURT)
    df_distributions_units = pd.DataFrame.from_records(
        itertools.chain(gen_dist_nl, gen_dist_gem, gen_dist_wij, gen_dist_buu),
        index="unit_code"
    )

    log.info("Analysing estimated and EP-Online deviations")
    gen_nl = calculate_distance_stats_for_area(possible_labels, AggregateUnit.NL,
                                               distance_column)
    gen_gem = calculate_distance_stats_for_area(possible_labels, AggregateUnit.GEMEENTE,
                                                distance_column)
    gen_wij = calculate_distance_stats_for_area(possible_labels, AggregateUnit.WIJK,
                                                distance_column)
    gen_buu = calculate_distance_stats_for_area(possible_labels, AggregateUnit.BUURT,
                                                distance_column)
    df_distance_stats = pd.DataFrame.from_records(
        itertools.chain(gen_nl, gen_gem, gen_wij, gen_buu),
        index="unit_code"
    )

    p_out = PATH_OUTPUT_DIR.joinpath("labels_neighbourhood_ep_online_possible").with_suffix(".csv")
    log.info(f"Writing output to {p_out}")
    df_distance_stats.join(df_distributions_units).to_csv(p_out)

    if args.plot:
        p = PATH_OUTPUT_DIR.joinpath("plots")
        p.mkdir(parents=True, exist_ok=True)

        # Plot NL
        log.info(f"Writing plot of the Netherlands to {p}")
        plot_comparison(df_with_truth, p, aggregate_level=AggregateUnit.NL)
        log.info(f"Writing plots of municipalities to {p}")
        plot_comparison(df_with_truth, p, aggregate_level=AggregateUnit.GEMEENTE)
        log.info(f"Writing plots of wijken to {p}")
        plot_comparison(df_with_truth, p, aggregate_level=AggregateUnit.WIJK)
        log.info(f"Writing plots of neighborhoods to {p}")
        plot_comparison(df_with_truth, p, aggregate_level=AggregateUnit.BUURT)

        p = PATH_OUTPUT_DIR.joinpath("plots_possible")
        p.mkdir(parents=True, exist_ok=True)

        # Plot NL
        log.info(f"Writing plot of the Netherlands to {p}")
        plot_comparison(possible_labels, p, aggregate_level=AggregateUnit.NL)
        log.info(f"Writing plots of municipalities to {p}")
        plot_comparison(possible_labels, p, aggregate_level=AggregateUnit.GEMEENTE)
        log.info(f"Writing plots of wijken to {p}")
        plot_comparison(possible_labels, p, aggregate_level=AggregateUnit.WIJK)
        log.info(f"Writing plots of neighborhoods to {p}")
        plot_comparison(possible_labels, p, aggregate_level=AggregateUnit.BUURT)


if __name__ == "__main__":
    validate_cli()
