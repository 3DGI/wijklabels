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
parser.add_argument('path_estimated_labels_csv')
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
    #     "/home/balazs/Development/wijklabels/tests/data/output/labels_individual.csv",
    #     "/home/balazs/Development/wijklabels/tests/data/output/distribution",
    # ])

    p_el = Path(args.path_estimated_labels_csv).resolve()
    p_out = Path(args.path_output_dir).resolve()
    p_out.mkdir(parents=True, exist_ok=True)

    plt.style.use('seaborn-v0_8-muted')

    columns_index = ["pand_identificatie", "vbo_identificatie"]

    log.info("Loading data")
    estimated_labels_df = pd.read_csv(
        p_el,
        converters={
            "energylabel": EnergyLabel.from_str,
            "bouwperiode": Bouwperiode.from_str,
            "vormfactorclass": VormfactorClass.from_str,
            "woningtype_pre_nta8800": WoningtypePreNTA8800.from_str
        }).set_index(
        columns_index
    )

    p = p_out.joinpath("plots")
    p.mkdir(parents=True, exist_ok=True)

    # Plot NL
    log.info(f"Writing plot of the Netherlands to {p}")
    plot_aggregate(estimated_labels_df, p, aggregate_level=AggregateUnit.NL)
    # log.info(f"Writing plots of neighborhoods to {p}")
    # plot_aggregate(estimated_labels_df, p, aggregate_level=AggregateUnit.BUURT)

