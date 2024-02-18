import argparse
from pathlib import Path
import logging

import pandas as pd
import psycopg
import matplotlib.pyplot as plt
import numpy as np

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

# Logger for data validation messages
log = logging.getLogger("VALIDATION")
log.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
ch.setFormatter(formatter)
log.addHandler(ch)

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
    bouwperiode = True
    woningtype = True
    coverage = True

    p_ep = Path(args.path_ep_online_csv).resolve()
    connection_string = f"postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.dbname}"

    plt.style.use('seaborn-v0_8-muted')

    log.info("Loading data")
    ep_online_df = EPLoader(file=p_ep).load()
    log.info(f"Loaded {len(ep_online_df)} records from EP-Online")

    columns_index = ["pand_identificatie", "vbo_identificatie"]
    with psycopg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT pand_identificatie, vbo_identificatie, oorspronkelijkbouwjaar, CASE WHEN vbo_count > 1 THEN 'appartement' ELSE woningtype END AS woningtype FROM {args.table};")
            bag_df = pd.DataFrame.from_records(
                cur.fetchall(),
                columns=["pand_identificatie", "vbo_identificatie", "oorspronkelijkbouwjaar", "woningtype"],
                index=columns_index
            )

    joined_df = ep_online_df.join(
        bag_df,
        how="inner",
        lsuffix="_ep_online"
    )
    joined_df["bouwperiode"] = joined_df.apply(
        lambda row: Bouwperiode.from_year_type_new(
            row["oorspronkelijkbouwjaar"], row["woningtype_ep_online"]
        ) if pd.notnull(row["oorspronkelijkbouwjaar"]) and pd.notnull(row["woningtype_ep_online"]) else pd.NA,
        axis=1
    )

    if bouwperiode:
        log.info("Comparing distributions per bouwperiode")
        periods_sorted = [b for b in Bouwperiode][2:-1]
        periods_sorted.insert(0, Bouwperiode.UNTIL_1945)
        periods_sorted_pretty = [b.format_pretty() for b in periods_sorted]

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
            periods_sorted_pretty
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
            periods_sorted_pretty
        ]



        fig = plt.figure(figsize=(9, 7))
        x = np.arange(len(bag_df_bouwperiode_dist.index))
        plt.bar(
            x=x,
            height=bag_df_bouwperiode_dist.bouwperiode.values * 100,
            label="BAG",
            zorder=3,
            width=0.25
        )
        plt.bar(
            x=x + 0.25,
            height=ep_online_bouwperiode_dist.bouwperiode.values * 100,
            label="EP-Online",
            zorder=3,
            width=0.25
        )
        plt.legend(loc="best")
        plt.grid(which="major", axis="x", zorder=0)
        plt.ylabel("Percentage (%) van het hele dataset")
        plt.xticks(x+0.125, bag_df_bouwperiode_dist.index)
        plt.xlabel("Bouwperiode")
        plt.suptitle("Spreiding van woningen per bouwperiode", fontsize=14)
        plt.savefig("bouwperiode_dist.png")
        plt.close()

    if woningtype:
        log.info("Comparing distributions per woningtype")
        ep_online_woningtype = joined_df["woningtype_ep_online"].map(
            lambda x: "appartement" if "appartement" in x else x.value,
            na_action="ignore"
        )
        ep_online_woningtype.name = "woningtype"
        ep_online_woningtype_dist = pd.crosstab(
            ep_online_woningtype,
            columns="woningtype",
            normalize=True
        )
        bag_df_woningtype_dist = pd.crosstab(
            joined_df["woningtype"],
            columns="woningtype",
            normalize=True
        )

        fig = plt.figure(figsize=(9, 7))
        x = np.arange(len(bag_df_woningtype_dist.index))
        plt.bar(
            x=x,
            height=bag_df_woningtype_dist.woningtype.values * 100,
            label="BAG",
            zorder=3,
            width=0.25
        )
        plt.bar(
            x=x+0.25,
            height=ep_online_woningtype_dist.woningtype.values * 100,
            label="EP-Online",
            zorder=3,
            width=0.25
        )
        plt.legend(loc="best")
        plt.grid(which="major", axis="x", zorder=0)
        plt.ylabel("Percentage (%) van het hele dataset")
        plt.xticks(x + 0.125, bag_df_woningtype_dist.index)
        plt.xlabel("woningtype")
        plt.suptitle("Spreiding van woningen per woningtype", fontsize=14)
        plt.savefig("woningtype_dist.png")
        plt.close()

    if coverage:
        log.info("Analysing the coverage of VBOs in neighborhoods")
        with psycopg.connect(connection_string) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT buurtcode, count(identificatie) vbo_cnt FROM wijklabels.vbo_in_buurt GROUP BY buurtcode ORDER BY buurtcode;")
                buurt_vbo_df = pd.DataFrame.from_records(
                    cur.fetchall(),
                    columns=["buurtcode", "vbo_cnt"],
                    index="buurtcode"
                )
                cur.execute("SELECT buurtcode, count(vbo_identificatie) vbo_with_label_cnt FROM wijklabels.ep_online_vbo GROUP BY buurtcode ORDER BY buurtcode;")
                buurt_vbo_label_df = pd.DataFrame.from_records(
                    cur.fetchall(),
                    columns=["buurtcode", "vbo_with_label_cnt"],
                    index="buurtcode"
                )
        buurt_coverage = pd.Series(
            index=buurt_vbo_df.index,
            data=buurt_vbo_label_df["vbo_with_label_cnt"] / buurt_vbo_df["vbo_cnt"] * 100,
        ).sort_index()
        buurt_coverage_stats = buurt_coverage.describe(
            percentiles=[.125, .25, .5, .75, .875]
        )
        ptiles = [buurt_coverage_stats.loc["12.5%"], buurt_coverage_stats.loc["50%"],
                  buurt_coverage_stats.loc["87.5%"]]

        ax = buurt_coverage.plot(
            kind="density",
            legend=False,
        )
        plt.xticks(
            ticks=range(30, 100, 10),
            labels=[str(i) for i in range(30, 100, 10)],
        )
        plt.xticks(
            ticks=ptiles,
            labels=[str(round(i)) for i in ptiles],
            minor=True
        )
        plt.xlim(-5, 100)
        plt.vlines(
            x=ptiles,
            ymin=0, ymax=0.1,
            colors="grey", linestyles="dashed"
        )
        # plt.grid(which="major", axis="x")
        plt.suptitle("Energielabeldekking van woningen in de buurten",
                     fontsize=14)
        plt.title("EP-Online v20231101_v2")
        plt.xlabel("Percentage woningen met een energielabel (%)")
        plt.savefig("/home/bdukai/software/wijklabels/coverage_vbo_dist_dense.png")
        plt.close()
