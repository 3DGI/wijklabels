"""Energy label data parsing and statistics

Copyright 2023 3DGI
"""
import re
import logging

import pandas as pd
from numpy import nan

from wijklabels import OrderedEnum
from wijklabels.woningtype import Woningtype, WoningtypePreNTA8800, Bouwperiode
from wijklabels.vormfactor import VormfactorClass

log = logging.getLogger()

LabelDistributions = dict[tuple[WoningtypePreNTA8800, Bouwperiode], pd.DataFrame]
LongLabels = pd.DataFrame


class EnergyLabel(OrderedEnum):
    G = "G"
    F = "F"
    E = "E"
    D = "D"
    C = "C"
    B = "B"
    A = "A"
    AP = "A+"
    APP = "A++"
    APPP = "A+++"
    APPPP = "A++++"

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.__str__()

    def within(self, other, within: int):
        """Test if the other label is within the provided range of labels.
        For example, the labels C, B, A are within the range of 1 of the
        label B. The labels D, C, B, A, A+ are within the range of 2 of the label B.
        The label B is in the range of 0 of the label B.
        The label-list is not recycled for the range, thus for the label G, the range of
        2 are G, F, E.
        """
        member_list = self.__class__._member_list()
        min_label_index = member_list.index(self) - within
        max_label_index = member_list.index(self) + within
        return min_label_index <= member_list.index(other) <= max_label_index

    @classmethod
    def from_str(cls, string: str):
        """Converts a string to an EnergyLabel

        :returns: a EnergyLabel object or pandas.NA if the string is invalid EnergyLabel
        """
        try:
            return cls(string)
        except ValueError:
            return pd.NA


def parse_energylabel_ditributions(excelloader) -> LabelDistributions:
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
                woningtype = WoningtypePreNTA8800(_wt)
                bouwperiode = Bouwperiode.from_year_type(
                    oorspronkelijkbouwjaar=construction_year_min + 1,
                    woningtype=woningtype)
            df = pd.read_excel(
                io=excelloader.file,
                usecols="B:O",
                skiprows=i, nrows=10,
                decimal=","
            )
            # Drop the second column that contains the end of the vormfactor range
            df.drop(columns=["Unnamed: 2"], inplace=True)
            df.rename(columns=dict((c, EnergyLabel(c)) for c in df.columns[1:-1]),
                      inplace=True)
            # Cast the vormfactor range to our enum
            vfc = list(VormfactorClass)
            vfc.append(nan)
            df["vormfactor"] = vfc
            label_distributions[(woningtype, bouwperiode)] = df
    sorted(label_distributions)
    return label_distributions


def reshape_for_classification(label_distributions: LabelDistributions) -> LongLabels:
    """Normalize the percentages so that they total to 100% per vormfactor class, per
    woningtype. Because in the input excel tables, the percentages total across all
    vormfactors per woningtype.

    Returns a Dataframe that has the following structure:
        index: ['woningtype', 'bouwperiode', 'vormfactor']
        columns: ['energylabel', 'probability', 'bin_min', 'bin_max']
    """
    dfs = []
    for (woningtype, bouwperiode), df in label_distributions.items():
        # Drop the last row that contains the totals per label
        df.drop(df.tail(1).index, inplace=True)
        df.set_index("vormfactor", inplace=True)
        totals = df["TOTAAL"]
        # Drop the last column that contains the total per vormfactor
        df.drop(columns=["TOTAAL"], inplace=True)
        normalized = df.div(totals, axis="index").replace(0.0, nan)
        dfs_bins = []
        for row in normalized.iterrows():
            # probabilities from A++++ to G
            probabilities = row[1]
            bin_max = probabilities.cumsum()
            bin_max_continuous = bin_max[bin_max.notna()].to_list()
            # shift the max values to get the lower range for each label
            bin_max_continuous.insert(0, 0.0)
            bin_max_continuous.pop()
            bin_min = pd.Series(bin_max_continuous,
                                index=bin_max[bin_max.notna()].index)
            df_bins = pd.DataFrame(
                data={"vormfactor": row[0], "energylabel": list(EnergyLabel),
                      "bin_min": bin_min, "bin_max": bin_max}
            )
            df_bins.set_index(["vormfactor", "energylabel"], inplace=True)
            if not df_bins.bin_max.isnull().all():
                dfs_bins.append(df_bins)
        # convert to long format
        normalized_long = normalized.melt(var_name="energylabel",
                                          value_name="probability", ignore_index=False)
        normalized_long.set_index(["energylabel"], append=True, inplace=True)
        df_long = normalized_long.join(pd.concat(dfs_bins), how="inner")
        df_long["woningtype_pre_nta8800"] = woningtype
        df_long["bouwperiode"] = bouwperiode
        dfs.append(df_long)
    df = pd.concat(dfs)
    df.reset_index(inplace=True)
    result_df = df[
        ["woningtype_pre_nta8800", "bouwperiode", "vormfactor", "energylabel",
         "probability",
         "bin_min", "bin_max"]]
    result_df.set_index(["woningtype_pre_nta8800", "bouwperiode", "vormfactor"],
                        inplace=True)
    return result_df.sort_index(inplace=False)


def estimate_label(df: LongLabels, woningtype: WoningtypePreNTA8800,
                   bouwperiode: Bouwperiode,
                   vormfactor: VormfactorClass,
                   random_number: float) -> EnergyLabel | None:
    """Assign an energy label to the provided properties (woningtype, bouwperiode,
    vormfactor) and the computed random number.
    For the given woningtype, bouwperiode and vormfactor, the energy labels have the
     distribution that is described in the Voorbeeldwoningen 2022 study. This
    distribution is described with bins, which are expressed as percentages in the
    original study, and here they are cumulative ranges on the range of (0,1).
    Then the input `random_number` is assigned the energy label which bin the
    `random_number` falls into.
    """
    try:
        label = df.loc[(woningtype, bouwperiode, vormfactor), :].query(
            f"bin_min <= {random_number} < bin_max").energylabel
        if len(label) > 1:
            log.error(
                f"multiple labels {label} found for {(woningtype, bouwperiode, vormfactor)} and {random_number}, returning None")
        return label.item() if len(label) == 1 else None
    except KeyError:
        # There is no data in the label distributions for this
        return None
