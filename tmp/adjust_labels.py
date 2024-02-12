import argparse
from pathlib import Path
import logging

import pandas as pd
import psycopg
import matplotlib.pyplot as plt

from wijklabels import AggregateUnit
from wijklabels.report import (aggregate_to_unit, plot_comparison, plot_aggregate,
                               calculate_distance_stats_for_area)
from wijklabels.load import EPLoader, ExcelLoader
from wijklabels.labels import parse_energylabel_ditributions, \
    reshape_for_classification, EnergyLabel
from wijklabels.vormfactor import VormfactorClass
from wijklabels.woningtype import WoningtypePreNTA8800, Bouwperiode

parser = argparse.ArgumentParser(prog='wijklabels-analyse-ep-online')
parser.add_argument('path_validated_labels_csv')
parser.add_argument('path_output_dir')

# Logger for data validation messages
log = logging.getLogger("PLOTTING")
log.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
ch.setFormatter(formatter)
log.addHandler(ch)

if __name__ == '__main__':
    args = parser.parse_args()
    # args = parser.parse_args([
    #     "/home/balazs/Development/wijklabels/tests/data/output/distribution/labels_individual_ep_online.csv",
    #     "/home/balazs/Development/wijklabels/tests/data/output/distribution",
    # ])

    p_el = Path(args.path_validated_labels_csv).resolve()
    p_out = Path(args.path_output_dir).resolve()
    p_out.mkdir(parents=True, exist_ok=True)

    columns_index = ["pand_identificatie", "vbo_identificatie"]

    log.info("Loading data")
    estimated_labels_df = pd.read_csv(
        p_el,
        converters={
            "energylabel": EnergyLabel.from_str,
        }).set_index(
        columns_index
    )

    label_adjustment = estimated_labels_df[["energylabel", "energylabel_dist_est_ep"]].groupby("energylabel").median() * -1.0
    log.info(f"Calculated label adjustment: {label_adjustment}")

    estimated_labels_df["energylabel_adjusted"] = estimated_labels_df["energylabel"].apply(
        lambda e: e.adjust_with(int(label_adjustment.loc[e].iloc[0]))
    )

    p = p_out.joinpath("plots_adjusted")
    p.mkdir(parents=True, exist_ok=True)
    log.info(f"Writing plot of the Netherlands to {p}")
    plot_aggregate(estimated_labels_df, p, aggregate_level=AggregateUnit.NL, column_energylabel="energylabel_adjusted")