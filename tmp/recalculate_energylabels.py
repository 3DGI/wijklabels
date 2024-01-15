from pathlib import Path
import itertools
import random
import argparse
from concurrent.futures import ProcessPoolExecutor

import pandas as pd

from wijklabels.load import EnergyLabel, ExcelLoader
from wijklabels.woningtype import Woningtype, WoningtypePreNTA8800, Bouwperiode
from wijklabels.vormfactor import VormfactorClass
from wijklabels.labels import parse_energylabel_ditributions, \
    reshape_for_classification, estimate_label

parser = argparse.ArgumentParser("recalculate")
parser.add_argument("-i", "--input")
parser.add_argument("-d", "--distributions")
parser.add_argument("-o", "--output")
parser.add_argument("-j", "--jobs", type=int, default=4)

if __name__ == "__main__":
    # args = parser.parse_args()
    args = parser.parse_args([
        "-i", "/home/balazs/Development/wijklabels/tests/data/output/labels_individual.csv",
        "-d", "/home/balazs/Development/wijklabels/tests/data/input/Illustraties spreiding Energielabel in WoON2018 per Voorbeeldwoning 2022 - 2023 01 25.xlsx",
        "-o", "/home/balazs/Development/wijklabels/tests/data/output/labels_individual_newlabels.csv"
    ])
    random.seed(1, version=2)

    columns_index = ["pand_identificatie", "vbo_identificatie"]
    individual_labels_df = pd.read_csv(
        args.input,
        converters={
            "energylabel": EnergyLabel.from_str,
            "woningtype": Woningtype.from_str,
            "woningtype_pre_nta8800": WoningtypePreNTA8800.from_str,
            "bouwperiode": Bouwperiode.from_str,
            "vormfactorclass": VormfactorClass.from_str
        },
        index_col=columns_index
    )
    individual_labels_df.rename(
        columns={"energylabel": "energylabel_old"},
        inplace=True
    )

    excelloader = ExcelLoader(file=Path(args.distributions))
    _d = parse_energylabel_ditributions(excelloader)
    distributions = reshape_for_classification(_d)

    nr_pand = len(individual_labels_df)
    with ProcessPoolExecutor(max_workers=args.jobs) as executor:
        label_mapper = executor.map(
            estimate_label,
            itertools.repeat(distributions, nr_pand),
            individual_labels_df["woningtype_pre_nta8800"].to_list(),
            individual_labels_df["bouwperiode"].to_list(),
            individual_labels_df["vormfactorclass"].to_list(),
            itertools.repeat(None, nr_pand)
        )
        label_df = pd.DataFrame(data=label_mapper, index=individual_labels_df.index,
                                columns=["energylabel"])

    new_df = individual_labels_df.join(label_df, how="inner")
    new_df.to_csv(args.output)
    print(f"Saved to {args.output}")
