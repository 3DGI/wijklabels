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


def calculate_distance_stats_for_area(validated: pd.DataFrame,
                                      aggregate_level: AggregateUnit,
                                      distance_column: str):
    aggregate_id_column = aggregate_column_name(aggregate_level)
    for aggregate_id in validated[aggregate_id_column].unique():
        yield {
            "unit": str(aggregate_level),
            "unit_code": aggregate_id,
            "woning_count": len(validated.loc[((validated[distance_column].notnull()) & (
                        validated[aggregate_id_column] == aggregate_id)), :]),
            "afwijking_median": validated.loc[
                validated[aggregate_id_column] == aggregate_id, [
                    distance_column]].median().values[0],
            "afwijking_mean": validated.loc[
                validated[aggregate_id_column] == aggregate_id, [
                    distance_column]].mean().values[0],
            "afwijking_std": validated.loc[
                validated[aggregate_id_column] == aggregate_id, [
                    distance_column]].std().values[0],
            "afwijking_min": validated.loc[
                validated[aggregate_id_column] == aggregate_id, [
                    distance_column]].min().values[0],
            "afwijking_max": validated.loc[
                validated[aggregate_id_column] == aggregate_id, [
                    distance_column]].max().values[0]
        }


def aggregate_to_unit(validated: pd.DataFrame, energylabel_col: str,
                      aggregate_level: AggregateUnit) -> pd.DataFrame:
    aggregate_id_column = aggregate_column_name(aggregate_level)
    for aggregate_id in validated[aggregate_id_column].unique():
        df_unit = validated.loc[((validated[energylabel_col].notnull()) & (validated[aggregate_id_column] == aggregate_id)), :]
        cnt = len(df_unit)
        dist = df_unit[energylabel_col].value_counts() / cnt
        for label in EnergyLabel:
            if label not in dist:
                dist.loc[label] = np.nan
        label_distribution = dist.sort_index(ascending=False).to_dict()
        label_distribution["unit_code"] = aggregate_id
        yield label_distribution


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


def plot_buurt(df: pd.DataFrame, buurt: str):
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
    return ax


def plot_aggregate(validated: pd.DataFrame, dir_plots: Path,
                    aggregate_level: AggregateUnit, column_energylabel="energylabel"):
    """Plot the aggregated labels on the provided level."""
    dir_plots.mkdir(exist_ok=True)
    aggregate_id_column = aggregate_column_name(aggregate_level)
    plt.style.use('seaborn-v0_8-muted')
    for aggregate_id in validated[aggregate_id_column].unique():
        # Plot both distributions side by side
        b = validated.loc[
            validated[aggregate_id_column] == aggregate_id,
            [column_energylabel, ]
        ]
        estimated = b[column_energylabel].value_counts() / len(b) * 100
        b_df = pd.DataFrame({"estimated": estimated},
                            index=[EnergyLabel.APPPP, EnergyLabel.APPP, EnergyLabel.APP,
                                   EnergyLabel.AP, EnergyLabel.A, EnergyLabel.B,
                                   EnergyLabel.C, EnergyLabel.D,
                                   EnergyLabel.E, EnergyLabel.F, EnergyLabel.G])
        ax = b_df.plot(kind="bar",
                       rot=0,
                       color={"estimated": COLORS},
                       xlabel="",
                       zorder=3)
        ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=100, decimals=0))
        ax.set_yticks([10, 20, 30, 40, 50, 60, 70, 80])
        ax.get_legend().remove()
        plt.style.use('seaborn-v0_8-muted')
        plt.grid(visible=True, which="major", axis="y", zorder=0)
        plt.title(f"{aggregate_id_column.title()}: {aggregate_id}\nNr. woningen: {len(b)}", fontsize=10)
        plt.suptitle("Spreiding van energielabels", fontsize=14)
        plt.tight_layout()
        filename = ''.join(e for e in aggregate_id if e.isalnum())
        plt.savefig(f"{dir_plots}/{aggregate_level}_{filename}_estimated.png")
        plt.close()


def plot_comparison(validated: pd.DataFrame, dir_plots: Path,
                    aggregate_level: AggregateUnit, woningtype=None):
    # Compare estimated to groundtruth in plots
    dir_plots.mkdir(exist_ok=True)
    aggregate_id_column = aggregate_column_name(aggregate_level)
    plt.style.use('seaborn-v0_8-muted')
    woningtype_subtitle = "Alle woningtypen" if woningtype is None else f"{woningtype.capitalize()} woningtypen"
    for aggregate_id in validated[aggregate_id_column].unique():
        # Plot both distributions side by side
        b = validated.loc[
            validated[aggregate_id_column] == aggregate_id,
            ["energylabel", "energylabel_ep_online"]
        ]
        estimated = b["energylabel"].value_counts() / len(b) * 100
        truth = b["energylabel_ep_online"].value_counts() / len(b) * 100
        b_df = pd.DataFrame({"ep-online": truth, "geschat": estimated},
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
        plt.style.use('seaborn-v0_8-muted')
        plt.grid(visible=True, which="major", axis="y", zorder=0)
        plt.title(f"{woningtype_subtitle}\n{aggregate_id_column.title()}: {aggregate_id}\nNr. woningen: {len(b)}", fontsize=10)
        plt.suptitle(f"Spreiding van energielabels", fontsize=14)
        plt.tight_layout()
        filename = ''.join(e for e in aggregate_id if e.isalnum())
        plt.savefig(f"{dir_plots}/{aggregate_level}_{filename}.png")
        plt.close()

        # Plot distances
        def _plot_dist(grouped, t):
            fig, ax = plt.subplots(figsize=(8, 6))
            boxplot = ax.boxplot(x=[group.values for name, group in grouped],
                                 labels=grouped.groups.keys(),
                                 patch_artist=True,
                                 medianprops={'color': 'black'},
                                 zorder=3)
            ax.set_xticks(ticks=range(11, 0, -1), labels=list(reversed(EnergyLabel)))
            ax.set_xlim(12, 0)
            if t == "_ep_est":
                ax.set_xlabel("Energielabel in EP-Online")
                ax.set_ylabel("Afwijking geschat energielabel")
            else:
                ax.set_xlabel("Geschat energielabel")
                ax.set_ylabel("Afwijking EP-Online")
            ax.set_yticks(range(-10, 11, 1))
            # Assign colors to each box in the boxplot
            for box, color in zip(boxplot['boxes'], reversed(COLORS)):
                box.set_facecolor(color)
            plt.axhline(y=0.0, color='#154273', linestyle='-')
            plt.grid(visible=True, which="major", axis="y", zorder=0)
            plt.title(f"{woningtype_subtitle}\n{aggregate_id_column.title()}: {aggregate_id}\nNr. woningen: {len(b)}", fontsize=10)
            plt.suptitle(f"Afwijking van de geschatte labels van de EP-Online labels", fontsize=14)
            plt.tight_layout()
            plt.savefig(f"{dir_plots}/{aggregate_level}_{filename}_dist{t}.png")
            plt.close()

        grouped_est_ep = validated.loc[
            validated[aggregate_id_column] == aggregate_id,
            ["energylabel", "energylabel_dist_est_ep"]
        ].groupby("energylabel")["energylabel_dist_est_ep"]
        _plot_dist(grouped_est_ep, "_est_ep")


def aggregate_column_name(aggregate_level):
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
    return aggregate_id_column