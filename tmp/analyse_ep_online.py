import argparse
from pathlib import Path

import pandas as pd
import psycopg
import matplotlib.pyplot as plt

from wijklabels.load import EPLoader
from wijklabels.woningtype import Bouwperiode, WoningtypePreNTA8800

parser = argparse.ArgumentParser(prog='wijklabels-analyse-ep-online')
parser.add_argument('path_ep_online_csv')
parser.add_argument("-d", '--dbname')
parser.add_argument('--host', default='localhost')
parser.add_argument("-p", '--port', type=int, default=5432)
parser.add_argument("-u", '--user')
parser.add_argument('--password')
parser.add_argument('--table', type=str, default='wijklabels.input')

if __name__ == '__main__':
    args = parser.parse_args()
    # args = parser.parse_args([
    #     "/data/energylabel-ep-online/v20231101_v2_csv_subset.csv",
    #     "-d", "postgres",
    #     "--host", "localhost",
    #     "-p", "8001",
    #     "-u", "postgres",
    #     "--password", "password"
    # ])
    p_ep = Path(args.path_ep_online_csv).resolve()
    connection_string = f"postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.dbname}"

    plt.style.use('seaborn-v0_8-muted')

    ep_online_df = EPLoader(file=p_ep).load()

    columns_index = ["pand_identificatie", "vbo_identificatie"]
    with psycopg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute(f"select pand_identificatie, vbo_identificatie, oorspronkelijkbouwjaar from {args.table}")
            bag_df = pd.DataFrame.from_records(
                cur.fetchall(),
                columns=["pand_identificatie", "vbo_identificatie", "oorspronkelijkbouwjaar"],
                index=columns_index
            )

    joined_df = ep_online_df.join(
        bag_df,
        how="inner"
    )
    joined_df["bouwperiode"] = joined_df.apply(
        lambda row: Bouwperiode.from_year_type_new(
            row["oorspronkelijkbouwjaar"], row["woningtype"]
        ) if pd.notnull(row["oorspronkelijkbouwjaar"]) and pd.notnull(row["woningtype"]) else pd.NA,
        axis=1
    )

    # Compare year distributions
    periods_sorted = [b.format_pretty() for b in Bouwperiode][2:-1]
    periods_sorted.insert(0, '< 1945')

    ep_online_bouwperiode = joined_df["oorspronkelijkbouwjaar"].map(
        lambda x: Bouwperiode.from_year(x).format_pretty(),
        na_action="ignore"
    )
    ep_online_bouwperiode.name = "bouwperiode"
    ep_online_bouwperiode_dist = pd.crosstab(
        ep_online_bouwperiode,
        columns="bouwperiode",
        normalize=True
    ).loc[
        periods_sorted
    ]
    bag_df_bouwperiode = bag_df["oorspronkelijkbouwjaar"].map(
        lambda x: Bouwperiode.from_year(x).format_pretty(),
        na_action="ignore"
    )
    bag_df_bouwperiode.name = "bouwperiode"
    bag_df_bouwperiode_dist = pd.crosstab(
        bag_df_bouwperiode,
        columns="bouwperiode",
        normalize=True
    ).loc[
        periods_sorted
    ]

    fig = plt.figure(figsize=(9, 7))
    plt.barh(
        y=bag_df_bouwperiode_dist.index,
        width=bag_df_bouwperiode_dist.bouwperiode.values * -100,
        label="BAG",
        zorder=3
    )
    plt.barh(
        y=ep_online_bouwperiode_dist.index,
        width=ep_online_bouwperiode_dist.bouwperiode.values * 100,
        label="EP-Online",
        zorder=3
    )
    plt.legend(loc="best")
    plt.grid(which="major", axis="x", zorder=0)
    plt.xticks(
        ticks=range(-50, 60, 10),
        labels=[str(i) for i in range(-50, 60, 10)]
    )
    plt.xlabel("Percentage (%) van het heele dataset")
    plt.ylabel("Bouwperiode")
    plt.suptitle("Spreiding van woningen per bouwperiode", fontsize=14)
    plt.savefig("bouwperiode_dist.png")

    # # Aggregate per year and type
    # total = joined_df.count().iloc[0]
    # pt_crosstab = pd.crosstab(
    #     joined_df["bouwperiode"],
    #     columns=joined_df["woningtype"],
    #     margins=True,
    #     margins_name="Totaal"
    # )
    # ct = pt_crosstab.apply(
    #     lambda col: list(map(lambda cnt: f"{cnt} ({round(cnt / total * 100)}%)", col))
    # ).replace(
    #     "0 (0%)", ""
    # ).reset_index(
    #     drop=False
    # )
    # ct.columns.name = "Woningtype"
    # ct["Bouwperiode"] = ct["bouwperiode"].apply(
    #     lambda bp: bp.format_pretty() if bp != "Totaal" else bp
    # )
    # ct.drop("bouwperiode", axis=1, inplace=True)
    # ct.set_index("Bouwperiode", inplace=True)
    # print(ct)