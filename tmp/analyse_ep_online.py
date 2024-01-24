import argparse
from pathlib import Path
import logging

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
    args = parser.parse_args([
        "/data/energylabel-ep-online/v20231101_v2_csv_subset.csv",
        "-d", "postgres",
        "--host", "localhost",
        "-p", "8001",
        "-u", "postgres",
        "--password", "password"
    ])
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
    plt.xticks(
        ticks=range(-55, 60, 10),
        minor=True,
    )
    plt.xlabel("Percentage (%) van het hele dataset")
    plt.ylabel("Bouwperiode")
    plt.suptitle("Spreiding van woningen per bouwperiode", fontsize=14)
    plt.savefig("bouwperiode_dist.png")
    plt.close()

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

    fig = plt.figure(figsize=(12, 7))
    plt.barh(
        y=bag_df_woningtype_dist.index,
        width=bag_df_woningtype_dist.woningtype.values * -100,
        label="BAG",
        zorder=3
    )
    plt.barh(
        y=ep_online_woningtype_dist.index,
        width=ep_online_woningtype_dist.woningtype.values * 100,
        label="EP-Online",
        zorder=3
    )
    plt.legend(loc="best")
    plt.grid(which="major", axis="x", zorder=0)
    plt.xticks(
        ticks=range(-90, 100, 10),
        labels=[str(i) for i in range(-90, 100, 10)],
    )
    plt.xticks(
        ticks=range(-95, 100, 5),
        minor=True,
    )
    plt.xlabel("Percentage (%) van het hele dataset")
    plt.ylabel("woningtype")
    plt.suptitle("Spreiding van woningen per woningtype", fontsize=14)
    plt.savefig("woningtype_dist.png")
    plt.close()

    log.info("Comparing distributions per woningtype")
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

    plt.boxplot(
        buurt_coverage,
        vert=False
    )
    plt.suptitle("Energielabeldekking van woningen in de buurten",
                 fontsize=14)
    plt.title("EP-Online v20231101_v2")
    plt.xlabel("Percentage woningen met een energielabel in de buurten (%)")
    plt.savefig("coverage_vbo_dist.png")
    plt.close()

    buurt_coverage.plot(
        kind="density",
        legend=False
    )
    plt.suptitle("Spreiding van de energielabeldekking van woningen in de buurten",
                 fontsize=14)
    plt.title("EP-Online v20231101_v2")
    plt.xlabel("Percentage woningen met een energielabel in de buurten (%)")
    plt.xlim(-5, 100)
    plt.show()
    plt.savefig("coverage_dist.png")
    plt.close()


    log.info("Analysing the coverage in neighborhoods")
    with psycopg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT buurtcode, count(identificatie) pand_cnt FROM wijklabels.pand_in_buurt GROUP BY buurtcode ORDER BY buurtcode;")
            buurt_pand_df = pd.DataFrame.from_records(
                cur.fetchall(),
                columns=["buurtcode", "pand_cnt"],
                index="buurtcode"
            )
            cur.execute("SELECT buurtcode, count(pand_identificatie) pand_with_label_cnt FROM wijklabels.ep_online_pand GROUP BY buurtcode ORDER BY buurtcode;")
            buurt_pand_label_df = pd.DataFrame.from_records(
                cur.fetchall(),
                columns=["buurtcode", "pand_with_label_cnt"],
                index="buurtcode"
            )
    buurt_coverage = pd.Series(
        index=buurt_pand_df.index,
        data=buurt_pand_label_df["pand_with_label_cnt"] / buurt_pand_df["pand_cnt"] * 100,
    ).sort_index()
    buurt_coverage.plot(
        kind="density",
        legend=False
    )
    plt.suptitle("Spreiding van de energielabeldekking van panden in de buurten",
                 fontsize=14)
    plt.title("EP-Online v20231101_v2")
    plt.xlabel("Percentage panden met een energielabel in de buurten (%)")
    plt.xlim(-5, 100)
    plt.savefig("coverage_dist.png")
    plt.close()

    log.info("Analysing the coverage and construction years in neighborhoods")
    with psycopg.connect(connection_string) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pand_identificatie, buurtcode, oorspronkelijkbouwjaar FROM wijklabels.input;")
            buurt_pand_df = pd.DataFrame.from_records(
                cur.fetchall(),
                columns=["pand_identificatie", "buurtcode", "oorspronkelijkbouwjaar"],
            )
    bouwperiode_median = buurt_pand_df.groupby(
        "buurtcode"
    )["oorspronkelijkbouwjaar"].median(
    )
    bp_df = pd.concat(
        [buurt_coverage, bouwperiode_median],
        axis=1
    ).rename(
        columns={
            0: "coverage",
            "oorspronkelijkbouwjaar": "bouwjaar_median"
        }
    )

    bp_df.plot(
        kind="scatter",
        x="bouwjaar_median",
        y="coverage",
        alpha=0.5
    )
    plt.xlim(1944, 2016)
    plt.xlabel("Median bouwjaar in de buurt")
    plt.ylabel("Energielabeldekking in de buurt (%)")
    plt.suptitle("Energielabeldekking per median bouwjaar in de buurten",
                 fontsize=14)
    plt.title("EP-Online v20231101_v2")
    plt.savefig("coverage_year_dist.png")
    plt.close()

    #v1
    plt.hist2d(
        x=bp_df["bouwjaar_median"],
        y=bp_df["coverage"],
        bins=[
            [i.value[0] for i in periods_sorted][1:] + [2020,],
            [float(i*10) for i in list(range(0, 6, 1))]
        ]
    )
    plt.xlabel("Median bouwjaar in de buurt")
    plt.ylabel("Energielabeldekking in de buurt (%)")
    plt.suptitle("Energielabeldekking per median bouwjaar in de buurten",
                 fontsize=14)
    plt.title("EP-Online v20231101_v2")
    plt.savefig("coverage_year_dist_hist.png")
    plt.close()
