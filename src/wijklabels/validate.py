import argparse
import csv
import itertools
from pathlib import Path
import logging
from dataclasses import dataclass, asdict

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


@dataclass
class Accuracy:
    accuracy: float = None
    label_range: int = None
    woningtype: str = None
    only_possible_labels: bool = None


def join_with_ep_online(estimated_labels: pd.DataFrame,
                        ep_online: pd.DataFrame) -> pd.DataFrame:
    """Join the the dataframe with the estimated labels onto the EP-Online dataframe."""
    groundtruth = ep_online.loc[ep_online["energylabel"].notna(), :]
    _v = estimated_labels.join(groundtruth["energylabel"], how="left",
                               rsuffix="_ep_online", validate="1:m")
    validated = _v.loc[
        (_v["energylabel_ep_online"].notna() & _v["energylabel"].notna())]
    return validated


def mark_dwelling_type(df: pd.DataFrame, woningtype=None):
    if woningtype is None:
        return df.apply(lambda row: True, axis=1)
    elif woningtype == "eengezins":
        return ~df["woningtype"].str.contains("appartement")
    elif woningtype == "meergezins":
        return df["woningtype"].str.contains("appartement")
    else:
        raise NotImplementedError


def calculate_accuracy(df_with_truth: pd.DataFrame, within, woningtype=None):
    """Calculate the accuracy within the given label range (e.g. +/-1 label)."""
    matches = df_with_truth.apply(
        lambda row: row.energylabel.within(row.energylabel_ep_online, within), axis=1)
    types_mask = mark_dwelling_type(df_with_truth, woningtype)
    subset = df_with_truth.loc[types_mask, :]
    nr_matches = (matches & types_mask).sum()
    nr_total = len(subset.loc[subset["energylabel_ep_online"].notnull()])
    return nr_matches / nr_total


def accuracy_per_type_and_range(df, accuracies, only_possible_labels):
    _possible = "all labels" if not only_possible_labels else "only possible labels"
    for within_range in (0, 1):
        for woningtype in (None, "eengezins", "meergezins"):
            accuracy = calculate_accuracy(df, within=within_range,
                                          woningtype=woningtype)
            _wt = "all" if woningtype is None else woningtype
            accuracies.append(
                Accuracy(accuracy=accuracy, woningtype=_wt, label_range=within_range,
                         only_possible_labels=only_possible_labels))
            log.info(
                f"Accuracy in label distance {within_range} for {_wt} woningtype for {_possible} is {round(accuracy * 100)}%")
    return accuracies


parser_validate = argparse.ArgumentParser(prog='wijklabels-validate')
parser_validate.add_argument('path_estimated_labels_csv')
parser_validate.add_argument('path_ep_online_csv')
parser_validate.add_argument('path_label_distributions_xlsx')
parser_validate.add_argument('path_output_dir')
parser_validate.add_argument('-e', '--energylabel', default="energylabel",
                             help="Name of the column that contains the energy labels.")
parser_validate.add_argument('--plot-nl', action='store_true',
                             help="Produce a diagram for the Netherlands, comparing the estimated labels to the EP-Online labels.")
parser_validate.add_argument('--plot-gemeente', action='store_true',
                             help="Produce a diagram for each municipality, comparing the estimated labels to the EP-Online labels.")
parser_validate.add_argument('--plot-wijk', action='store_true',
                             help="Produce a diagram for each wijk, comparing the estimated labels to the EP-Online labels.")
parser_validate.add_argument('--plot-buurt', action='store_true',
                             help="Produce a diagram for each buurt, comparing the estimated labels to the EP-Online labels.")
parser_validate.add_argument("--woningtype", choices=["eengezins", "meergezins"],
                             default=None,
                             help="Run the analysis on only the provided dwelling type. If not specified, all dwellings are included.")


def validate_cli():
    args = parser_validate.parse_args()
    p_ep = Path(args.path_ep_online_csv).resolve()
    p_el = Path(args.path_estimated_labels_csv).resolve()
    p_dist = Path(args.path_label_distributions_xlsx).resolve()
    PATH_OUTPUT_DIR = Path(args.path_output_dir).resolve()
    PATH_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    log.info("Loading data")
    estimated_labels_df = pd.read_csv(p_el, converters={
        args.energylabel: EnergyLabel.from_str, "bouwperiode": Bouwperiode.from_str,
        "vormfactorclass": VormfactorClass.from_str,
        "woningtype_pre_nta8800": WoningtypePreNTA8800.from_str}).set_index(
        ["vbo_identificatie", "pand_identificatie"])
    if args.energylabel not in estimated_labels_df.columns:
        raise ValueError(
            f"Did not find the required energy label column {args.energylabel} in the input")
    if args.energylabel != "energylabel":
        if "energylabel" in estimated_labels_df.columns:
            estimated_labels_df.rename(columns={"energylabel": "energylabel_bak"},
                                       inplace=True)
        estimated_labels_df.rename(columns={args.energylabel: "energylabel"},
                                   inplace=True)

    ep_online_df = EPLoader(file=p_ep).load()
    df_with_truth_all = join_with_ep_online(estimated_labels=estimated_labels_df,
                                            ep_online=ep_online_df)
    del ep_online_df

    log.info("Computing estimated label distance to EP-Online labels")
    distance_column = "energylabel_dist_est_ep"
    df_with_truth_all.loc[:, distance_column] = df_with_truth_all.apply(
        lambda row: row["energylabel_ep_online"].distance(row["energylabel"]), axis=1)

    log.info(
        "Comparing the EP-Online labels to the Voorbeeldwoningen 2022 distributions")
    excelloader = ExcelLoader(file=p_dist)
    _d = parse_energylabel_ditributions(excelloader)
    distributions = reshape_for_classification(_d)

    # Compare individual addresses
    def _ep_in_dist(df_dist, row):
        el_ep_online = row['energylabel_ep_online']
        try:
            return df_dist.loc[(row["woningtype_pre_nta8800"], row["bouwperiode"],
                                row["vormfactorclass"]), :].query(
                "energylabel == @el_ep_online").probability.notnull().values[0]
        except KeyError:
            return False

    df_with_truth_all["ep_online_label_in_distributions"] = df_with_truth_all.apply(
        lambda row: _ep_in_dist(distributions, row), axis=1)

    p_out = PATH_OUTPUT_DIR.joinpath("labels_individual_ep_online").with_suffix(".csv")
    log.info(f"Writing output to {p_out}")
    df_with_truth_all.to_csv(p_out)

    nr_no_label = estimated_labels_df["energylabel"].isnull().sum()
    nr_total = len(estimated_labels_df)
    log.info(
        f"Missing energy label because of gap in energy label distributions in Voorbeeldwoningen 2022: {round(nr_no_label / nr_total * 100)}%")

    accuracies = []
    only_possible_labels = False
    accuracies = accuracy_per_type_and_range(df_with_truth_all, accuracies,
                                             only_possible_labels)

    nr_impossible_labels = len(df_with_truth_all[df_with_truth_all[
                                                     "ep_online_label_in_distributions"] == False])
    nr_total = len(df_with_truth_all)
    log.info(f"Labels in EP-Online that do not have a corresponding probability in the Voorbeeldwoningen 2022 data: {round(nr_impossible_labels / nr_total * 100)}%")

    possible_labels = df_with_truth_all[
        df_with_truth_all["ep_online_label_in_distributions"] == True]
    only_possible_labels = True
    accuracies = accuracy_per_type_and_range(possible_labels, accuracies,
                                             only_possible_labels)
    p_out = PATH_OUTPUT_DIR.joinpath("accuracies").with_suffix(".csv")
    with p_out.open("w") as fo:
        csvwriter = csv.DictWriter(fo, fieldnames=asdict(Accuracy()).keys())
        csvwriter.writeheader()
        csvwriter.writerows(asdict(a) for a in accuracies)

    if args.woningtype == "eengezins":
        log.info(f"Selecting only {args.woningtype} woningtype")
        df_with_truth_subset = df_with_truth_all[~df_with_truth_all["woningtype"].str.contains(
            "appartement")]
    elif args.woningtype == "meergezins":
        log.info(f"Selecting only {args.woningtype} woningtype")
        df_with_truth_subset = df_with_truth_all[df_with_truth_all["woningtype"].str.contains(
            "appartement")]
    else:
        log.info("Selecting all woningtype")
        df_with_truth_subset = df_with_truth_all

    # Aggregate per buurt
    log.info("Aggregating the neigbourhoods")
    gen_dist_nl = aggregate_to_unit(df_with_truth_subset, "energylabel",
                                    AggregateUnit.NL)
    gen_dist_gem = aggregate_to_unit(df_with_truth_subset, "energylabel",
                                     AggregateUnit.GEMEENTE)
    gen_dist_wij = aggregate_to_unit(df_with_truth_subset, "energylabel",
                                     AggregateUnit.WIJK)
    gen_dist_buu = aggregate_to_unit(df_with_truth_subset, "energylabel",
                                     AggregateUnit.BUURT)
    df_distributions_units = pd.DataFrame.from_records(
        itertools.chain(gen_dist_nl, gen_dist_gem, gen_dist_wij, gen_dist_buu),
        index="unit_code")
    ## ep-online
    gen_dist_nl = aggregate_to_unit(df_with_truth_subset, "energylabel_ep_online",
                                    AggregateUnit.NL)
    gen_dist_gem = aggregate_to_unit(df_with_truth_subset, "energylabel_ep_online",
                                     AggregateUnit.GEMEENTE)
    gen_dist_wij = aggregate_to_unit(df_with_truth_subset, "energylabel_ep_online",
                                     AggregateUnit.WIJK)
    gen_dist_buu = aggregate_to_unit(df_with_truth_subset, "energylabel_ep_online",
                                     AggregateUnit.BUURT)
    df_distributions_units_ep_online = pd.DataFrame.from_records(
        itertools.chain(gen_dist_nl, gen_dist_gem, gen_dist_wij, gen_dist_buu),
        index="unit_code")

    log.info("Analysing estimated and EP-Online deviations (per address)")
    gen_nl = calculate_distance_stats_for_area(df_with_truth_subset, AggregateUnit.NL,
                                               distance_column)
    gen_gem = calculate_distance_stats_for_area(df_with_truth_subset,
                                                AggregateUnit.GEMEENTE, distance_column)
    gen_wij = calculate_distance_stats_for_area(df_with_truth_subset,
                                                AggregateUnit.WIJK, distance_column)
    gen_buu = calculate_distance_stats_for_area(df_with_truth_subset,
                                                AggregateUnit.BUURT, distance_column)
    df_distance_stats = pd.DataFrame.from_records(
        itertools.chain(gen_nl, gen_gem, gen_wij, gen_buu), index="unit_code")

    log.info("Analysing estimated and EP-Online deviations (aggregated)")
    df_dist_long_est = pd.melt(df_distributions_units.reset_index(),
                               id_vars=["unit_code"], value_vars=list(EnergyLabel),
                               var_name="energylabel",
                               value_name="probability").set_index(
        ["unit_code", "energylabel"])
    df_dist_long_ep = pd.melt(df_distributions_units_ep_online.reset_index(),
                              id_vars=["unit_code"], value_vars=list(EnergyLabel),
                              var_name="energylabel",
                              value_name="probability").set_index(
        ["unit_code", "energylabel"])
    df_dist_long = df_dist_long_est.join(df_dist_long_ep, rsuffix="_ep_online")
    df_dist_long["difference"] = df_dist_long["probability"] - df_dist_long[
        "probability_ep_online"]

    p_out = PATH_OUTPUT_DIR.joinpath("labels_neighbourhood_ep_online").with_suffix(
        ".csv")
    log.info(f"Writing output to {p_out}")
    df_distance_stats.join(df_distributions_units).to_csv(p_out)

    # Possible labels only
    log.info("Aggregating the neigbourhoods of possible labels")
    gen_dist_nl = aggregate_to_unit(possible_labels, "energylabel", AggregateUnit.NL)
    gen_dist_gem = aggregate_to_unit(possible_labels, "energylabel",
                                     AggregateUnit.GEMEENTE)
    gen_dist_wij = aggregate_to_unit(possible_labels, "energylabel", AggregateUnit.WIJK)
    gen_dist_buu = aggregate_to_unit(possible_labels, "energylabel",
                                     AggregateUnit.BUURT)
    df_distributions_units = pd.DataFrame.from_records(
        itertools.chain(gen_dist_nl, gen_dist_gem, gen_dist_wij, gen_dist_buu),
        index="unit_code")

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
        itertools.chain(gen_nl, gen_gem, gen_wij, gen_buu), index="unit_code")

    p_out = PATH_OUTPUT_DIR.joinpath(
        "labels_neighbourhood_ep_online_possible").with_suffix(".csv")
    log.info(f"Writing output to {p_out}")
    df_distance_stats.join(df_distributions_units).to_csv(p_out)

    if any([args.plot_nl, args.plot_gemeente, args.plot_wijk, args.plot_buurt]):
        p = PATH_OUTPUT_DIR.joinpath("plots")
        p.mkdir(parents=True, exist_ok=True)

        if args.plot_nl:
            log.info(f"Writing plot of the Netherlands to {p}")
            plot_comparison(df_with_truth_subset, p, aggregate_level=AggregateUnit.NL,
                            woningtype=args.woningtype)
        if args.plot_gemeente:
            log.info(f"Writing plots of municipalities to {p}")
            plot_comparison(df_with_truth_subset, p,
                            aggregate_level=AggregateUnit.GEMEENTE,
                            woningtype=args.woningtype)
        if args.plot_wijk:
            log.info(f"Writing plots of wijken to {p}")
            plot_comparison(df_with_truth_subset, p, aggregate_level=AggregateUnit.WIJK,
                            woningtype=args.woningtype)
        if args.plot_buurt:
            log.info(f"Writing plots of neighborhoods to {p}")
            plot_comparison(df_with_truth_subset, p,
                            aggregate_level=AggregateUnit.BUURT,
                            woningtype=args.woningtype)

        p = PATH_OUTPUT_DIR.joinpath("plots_possible")
        p.mkdir(parents=True, exist_ok=True)

        if args.plot_nl:
            log.info(f"Writing plot of the Netherlands to {p}")
            plot_comparison(possible_labels, p, aggregate_level=AggregateUnit.NL,
                            woningtype=args.woningtype)
        if args.plot_gemeente:
            log.info(f"Writing plots of municipalities to {p}")
            plot_comparison(possible_labels, p, aggregate_level=AggregateUnit.GEMEENTE,
                            woningtype=args.woningtype)
        if args.plot_wijk:
            log.info(f"Writing plots of wijken to {p}")
            plot_comparison(possible_labels, p, aggregate_level=AggregateUnit.WIJK,
                            woningtype=args.woningtype)
        if args.plot_buurt:
            log.info(f"Writing plots of neighborhoods to {p}")
            plot_comparison(possible_labels, p, aggregate_level=AggregateUnit.BUURT,
                            woningtype=args.woningtype)


if __name__ == "__main__":
    validate_cli()
