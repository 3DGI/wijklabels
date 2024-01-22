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

    log.info("Loading data")
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
    plt.xlabel("Percentage (%) van het hele dataset")
    plt.ylabel("Bouwperiode")
    plt.suptitle("Spreiding van woningen per bouwperiode", fontsize=14)
    plt.savefig("bouwperiode_dist.png")
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

    #v1
    plt.hist2d(
        x=bp_df["bouwjaar_median"].astype('Int64', errors="ignore"),
        y=bp_df["coverage"],
        bins=[
            [i.value[0] for i in periods_sorted][1:] + [2020,],
            [i*10 for i in list(range(0, 6, 1))]
        ]
    )
    plt.xlabel("Median bouwjaar in de buurt")
    plt.ylabel("Energielabeldekking in de buurt (%)")
    plt.suptitle("Energielabeldekking per median bouwjaar in de buurten",
                 fontsize=14)
    plt.title("EP-Online v20231101_v2")
    plt.savefig("coverage_year_dist_hist.png")
    plt.close()

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