"""Energy label data parsing and statistics

Copyright 2023 3DGI
"""
import re
import logging

import pandas as pd
from numpy import nan

from wijklabels.load import ExcelLoader
from wijklabels.woningtype import Woningtype, Bouwperiode
from wijklabels.vormfactor import VormfactorClass

log = logging.getLogger()

# label_distributions_path = '/home/balazs/Development/wijklabels/resources/Illustraties spreiding Energielabel in WoON2018 per Voorbeeldwoning 2022 - 2023 01 25.xlsx'
# excelloader = load.ExcelLoader(file=label_distributions_path)

LabelDistributions = dict[tuple[Woningtype, Bouwperiode], pd.DataFrame]


def parse_energylabel_ditributions(excelloader: ExcelLoader) -> LabelDistributions:
    """Parse the energy label distributions from the excel file.
    The distribution tables are parsed into a DataFrame and they are indexed by
    (Woningtype, Bouwperiode).
    """
    label_distributions_excel = excelloader.load()
    # Assuming that we need the first sheet that has 'spreiding' in its name
    sheet_name = [sheet_name for sheet_name in label_distributions_excel.sheetnames if
                  "spreiding" in sheet_name][0]
    sheet = label_distributions_excel[sheet_name]
    # We know from inspecting the excel sheet that the dwelling types are in column B,
    # starting in row 5, in every 15th row
    expected_max_woningtype = 60
    re_year = re.compile(r"(\d{4})")
    label_distributions = {}
    for i in list(range(5, 15 * expected_max_woningtype, 15)):
        wt = sheet[f"B{i}"].value
        if wt is None or wt == "":
            break
        else:
            search_result = re_year.search(wt)
            construction_year_min, construction_year_max = None, None
            woningtype = None
            bouwperiode = None
            if search_result is None:
                log.error(f"Did not find any years in {wt}")
            else:
                startpos, endpos = search_result.span()
                if wt[startpos - 1] == " ":
                    construction_year_min = int(search_result.group())
                    construction_year_max = int(wt[endpos + 1:])
                elif wt[startpos - 1] == "<":
                    construction_year_max = int(search_result.group())
                    construction_year_min = 0
                elif wt[startpos - 1] == ">":
                    construction_year_min = int(search_result.group())
                    construction_year_max = 9999
                _wt = wt[:startpos - 1].strip().lower()
                woningtype = Woningtype(_wt)
                bouwperiode = Bouwperiode.from_year_type(
                    oorspronkelijkbouwjaar=construction_year_min + 1,
                    woningtype=woningtype)
            df = pd.read_excel(
                io=excelloader.file,
                usecols="B:O",
                skiprows=i, nrows=10,
                decimal=","
            )
            df.drop(columns=["Unnamed: 2"], inplace=True)
            vfc = list(VormfactorClass)
            vfc.append(nan)
            df["vormfactor"] = vfc
            label_distributions[(woningtype, bouwperiode)] = df
    sorted(label_distributions)
    return label_distributions


def normalize_distributions(distributions_df: pd.DataFrame) -> pd.DataFrame:
    """Normalize the percentages so that they total to 100% per vormfactor class, per
    woningtype. Because in the input excel tables, the percentages total across all
    vormfactors per woningtype."""
    pass
