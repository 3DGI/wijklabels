import argparse
from pathlib import Path
import logging

import pandas as pd

from wijklabels.report import aggregate_to_buurt, plot_comparison

from wijklabels.load import EPLoader, to_energylabel

# Logger for data validation messages
log = logging.getLogger("VALIDATION")
log.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
ch.setFormatter(formatter)
log.addHandler(ch)


def join_with_ep_online(estimated_labels_csv_path: Path,
                        ep_online_csv_path: Path) -> pd.DataFrame:
    """Join the the dataframe with the estimated labels onto the EP-Online dataframe."""
    estimated_labels = pd.read_csv(estimated_labels_csv_path,
                                   converters={"energylabel": to_energylabel}).set_index(
        ["vbo_identificatie", "pand_identificatie"]
    )
    eploader = EPLoader(file=ep_online_csv_path)
    _g = eploader.load()

    groundtruth = _g.loc[_g["energylabel"].notna(), :]

    _v = estimated_labels.join(groundtruth["energylabel"], how="left",
                               rsuffix="_ep_online", validate="1:m")
    validated = _v.loc[(_v["energylabel_ep_online"].notna() & _v["energylabel"].notna())]
    return validated


parser_validate = argparse.ArgumentParser(prog='wijklabels-validate')
parser_validate.add_argument('path_estimated_labels_csv')
parser_validate.add_argument('path_ep_online_csv')
parser_validate.add_argument('path_output_dir')
parser_validate.add_argument('--plot', type=str,
                             help="Produce a diagram for each neighborhood, comparing the estimated labels to the EP-Online labels. The diagrams are placed into the provided directory.")


def validate_cli():
    args = parser_validate.parse_args()
    p_ep = Path(args.path_ep_online_csv).resolve()
    p_el = Path(args.path_estimated_labels_csv).resolve()
    PATH_OUTPUT_DIR = Path(args.path_output_dir).resolve()
    df_with_truth = join_with_ep_online(estimated_labels_csv_path=p_el,
                                        ep_online_csv_path=p_ep)

    p_out = PATH_OUTPUT_DIR.joinpath("labels_individual_ep_online").with_suffix(".csv")
    log.info(f"Writing output to {p_out}")
    df_with_truth.to_csv(p_out)

    # Aggregate per buurt
    log.info("Aggregating the neigbourhoods")
    buurten_labels_wide = aggregate_to_buurt(df_with_truth,
                                             col_labels="energylabel_ep_online")
    p_out = PATH_OUTPUT_DIR.joinpath("labels_neighbourhood_ep_online").with_suffix(".csv")
    log.info(f"Writing output to {p_out}")
    buurten_labels_wide.to_csv(p_out)

    if args.plot is not None:
        p = Path(args.plot).resolve()
        p.mkdir(parents=True, exist_ok=True)
        log.info(f"Writing plots to {p}")
        plot_comparison(df_with_truth, p)


if __name__ == "__main__":
    validate_cli()
