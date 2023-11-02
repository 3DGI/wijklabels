import os
import re
import logging

import pandas as pd
from openpyxl import Workbook

log = logging.getLogger()

# label_distributions_path = '/home/balazs/Development/wijklabels/resources/Illustraties spreiding Energielabel in WoON2018 per Voorbeeldwoning 2022 - 2023 01 25.xlsx'
# excelloader = load.ExcelLoader(file=label_distributions_path)


def parse_energylabel_ditributions(label_distributions_excel: Workbook,
                                   label_distributions_path: os.PathLike) -> dict[
    tuple[int, int, str], pd.DataFrame]:
    """Parse the energy label distributions from the excel file.
    The distribution tables are parsed into a DataFrame and they are indexed by
    (min. construciton year, max. construction year, dwelling type).
    """
    # Assuming that we need the first sheet that has 'spreiding' in its name
    sheet_name = [sheet_name for sheet_name in label_distributions_excel.sheetnames if
                  "spreiding" in sheet_name][0]
    sheet = label_distributions_excel[sheet_name]
    # We know from inspecting the excel sheet that the dwelling types are in column B,
    # starting in row 5, in every 15th row
    expected_max_woningtype = 60
    re_year = re.compile(r"(\d{4})")
    woningtype_df = {}
    for i in list(range(5, 15 * expected_max_woningtype, 15)):
        wt = sheet[f"B{i}"].value
        if wt is None or wt == "":
            break
        else:
            search_result = re_year.search(wt)
            construction_year_min, construction_year_max = None, None
            woningtype = None
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
                woningtype = wt[:startpos - 1]
            df = pd.read_excel(
                io=label_distributions_path,
                usecols="B:O",
                skiprows=i, nrows=10,
                decimal=","
            )
            woningtype_df[
                (construction_year_min, construction_year_max, woningtype)] = df
    sorted(woningtype_df)
    return woningtype_df
