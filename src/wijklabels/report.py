from pathlib import Path

import numpy as np
import pandas as pd
from matplotlib import ticker as mtick, pyplot as plt

from wijklabels import AggregateUnit
from wijklabels.labels import EnergyLabel

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


def plot_comparison(validated: pd.DataFrame, dir_plots: Path,
                    aggregate_level: AggregateUnit):
    # Compare estimated to groundtruth in plots
    dir_plots.mkdir(exist_ok=True)
    if aggregate_level == AggregateUnit.BUURT:
        aggregate_id_column = "buurtcode"
    elif aggregate_level == AggregateUnit.WIJK:
        aggregate_id_column = "wijkcode"
    elif aggregate_level == AggregateUnit.GEMEENTE:
        aggregate_id_column = "gemeentecode"
    elif aggregate_level == AggregateUnit.NL:
        aggregate_id_column = "landcode"
    else:
        raise ValueError(f"Unknown aggregate level: {aggregate_level}")
    for aggregate_id in validated[aggregate_id_column].unique():
        b = validated.loc[
            validated[aggregate_id_column] == aggregate_id,
            ["energylabel", "energylabel_ep_online"]
        ]
        estimated = b["energylabel"].value_counts() / len(b) * 100
        truth = b["energylabel_ep_online"].value_counts() / len(b) * 100
        b_df = pd.DataFrame({"estimated": estimated, "ep-online": truth},
                            index=[EnergyLabel.APPPP, EnergyLabel.APPP, EnergyLabel.APP,
                                   EnergyLabel.AP, EnergyLabel.A, EnergyLabel.B,
                                   EnergyLabel.C, EnergyLabel.D,
                                   EnergyLabel.E, EnergyLabel.F, EnergyLabel.G])
        ax = b_df.plot(kind="bar",
                       rot=0,
                       xlabel="",
                       zorder=3)
        ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=100, decimals=0))
        ax.set_yticks([10, 20, 30, 40, 50, 60, 70, 80])
        plt.grid(visible=True, which="major", axis="y", zorder=0)
        plt.title(f"Nr. woningen: {len(b)}", fontsize=10)
        plt.suptitle(aggregate_id, fontsize=14)
        plt.tight_layout()
        filename = ''.join(e for e in aggregate_id if e.isalnum())
        plt.savefig(f"{dir_plots}/{aggregate_level}_{filename}.png")
        plt.close()
