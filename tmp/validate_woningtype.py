import argparse
from pathlib import Path
import pandas as pd
from matplotlib import pyplot as plt

from wijklabels.load import EPLoader
from wijklabels.woningtype import Woningtype


def to_woningtype(gebouwtype: str):
    try:
        return Woningtype(gebouwtype)
    except ValueError:
        return pd.NA


# ep_df = pd.read_csv("/data/energylabel-ep-online/v20231101_v2_csv.csv",
#                     header=0, sep=";", low_memory=False,
#                     dtype={"Pand_bagpandid": str, "Pand_bagverblijfsobjectid": str})
# original_col_order = list(ep_df.columns)
# ep_df.set_index(["Pand_bagpandid", "Pand_bagverblijfsobjectid"], inplace=True)
#
# woningtypen_df = pd.read_csv(
#     "/home/balazs/Development/wijklabels/tests/data/output/labels_individual.csv",
#     index_col=["pand_identificatie", "vbo_identificatie"],
#     converters={"woningtype": to_woningtype})
# woningtypen_df.reset_index(inplace=True)
# woningtypen_df["Pand_bagpandid"] = woningtypen_df.apply(lambda row: row["pand_identificatie"].split("NL.IMBAG.Pand.")[1], axis=1)
# woningtypen_df["Pand_bagverblijfsobjectid"] = woningtypen_df.apply(lambda row: row["vbo_identificatie"].split("NL.IMBAG.Verblijfsobject.")[1], axis=1)
# woningtypen_df.set_index(["Pand_bagpandid", "Pand_bagverblijfsobjectid"], inplace=True)
#
# joined_df = woningtypen_df.join(ep_df, how="inner", rsuffix="_ep")
# subset = ep_df.loc[joined_df.index].reset_index()[original_col_order]
# subset.to_csv("/data/energylabel-ep-online/v20231101_v2_csv_subset.csv",
#               index=False, sep=";")


parser = argparse.ArgumentParser("validate-woningtype")
parser.add_argument("path_labels")
parser.add_argument("path_ep")
parser.add_argument("output_dir")

p = "/home/balazs/Development/wijklabels/tests/data/output/labels_individual.csv"
p2 = "/data/energylabel-ep-online/v20231101_v2_csv_subset.csv"

if __name__ == '__main__':
    args = parser.parse_args()
    woningtypen_df = pd.read_csv(
        Path(args.path_labels).resolve(),
        index_col=["pand_identificatie", "vbo_identificatie"],
        converters={"woningtype": to_woningtype})
    ep_df = EPLoader(Path(args.path_ep).resolve()).load()
    joined_df = woningtypen_df.join(ep_df, how="inner", rsuffix="_ep")

    # Eengezingswoningen
    eengezins = joined_df.loc[((joined_df["Pand_gebouwtype"].notna()) & (
        joined_df["Pand_gebouwsubtype"].isna()))]
    correct = eengezins.loc[eengezins["woningtype"] == eengezins["woningtype_ep"]]
    correct_pct = len(correct) / len(eengezins) * 100.0
    print(f"correct eengezinswoningen {round(correct_pct)}%")
    incorrect = eengezins.loc[eengezins["woningtype"] != eengezins["woningtype_ep"]]
    incorrect_pct = incorrect["woningtype"].value_counts() / len(incorrect)
    incorrect_pct.plot(kind="pie", autopct='%1.1f%%', ylabel="", xlabel="")
    plt.savefig(f"{args.output_dir}/woningtype_incorrect_eengezins.png")

    # Meergezinswoningen
    meergezins = joined_df.loc[joined_df["Pand_gebouwtype"] == "Appartement"]
    correct = meergezins.loc[meergezins["woningtype"] == meergezins["woningtype_ep"]]
    correct_pct = len(correct) / len(meergezins) * 100.0
    print(f"correct meergezinswoningen {round(correct_pct)}%")
    incorrect = meergezins.loc[meergezins["woningtype"] != meergezins["woningtype_ep"]]
    incorrect_pct = incorrect["woningtype"].value_counts() / len(incorrect)
    incorrect_pct.plot(kind="pie", autopct='%1.1f%%', ylabel="", xlabel="")
    plt.savefig(f"{args.output_dir}/woningtype_incorrect_meergezins.png")
