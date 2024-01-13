from pathlib import Path
import itertools
import random
import argparse

import pandas as pd

from wijklabels.load import EnergyLabel, ExcelLoader
from wijklabels.woningtype import Woningtype, WoningtypePreNTA8800, Bouwperiode
from wijklabels.vormfactor import VormfactorClass
from wijklabels.process import estimate_labels
from wijklabels.labels import parse_energylabel_ditributions, \
    reshape_for_classification

parser = argparse.ArgumentParser("recalculate")
parser.add_argument("-i", "--input")
parser.add_argument("-d", "--distributions")
parser.add_argument("-o", "--output")

if __name__ == "__main__":
    args = parser.parse_args()
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


    def estimate_labels_new(idf, dist, colidx):
        for _name, group in idf.groupby(colidx):
            estimate_labels(group, dist)
            yield group.reset_index().to_dict("records")


    records = itertools.chain.from_iterable(
        estimate_labels_new(individual_labels_df, distributions, columns_index)
    )
    new_df = pd.DataFrame.from_records(
        records,
        index=columns_index
    )
    new_df.to_csv(args.output)
    print(f"Saved to {args.output}")
