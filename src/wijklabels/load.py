"""Load CityJSON and BAG data

Copyright 2023 3DGI
"""
import inspect
from importlib import resources
from os import PathLike
from pathlib import Path
from copy import deepcopy

import numpy as np
from cjio.cityjson import CityJSON
import pandas as pd
from openpyxl import load_workbook, Workbook

from wijklabels import Bbox
from wijklabels.woningtype import Woningtype, to_woningtype
from wijklabels.labels import EnergyLabel


class SharedWallsLoader:
    """Load the shared walls data from a local CSV."""

    def __init__(self, file: PathLike = None):
        self.file = file

    def load(self) -> pd.DataFrame:
        df = pd.read_csv(self.file, header=0, index_col="identificatie")
        return df


class CityJSONLoader:
    """Load CityJSON data from local files or download them directly from 3dbag.nl."""

    def __init__(self, files: list[PathLike] = None, tiles: list[str] = None,
                 bbox: Bbox = None):
        self.files = files
        self.tiles = tiles
        self.bbox = bbox

    def load(self) -> CityJSON:
        if self.files:
            return self.__load_files()
        elif self.tiles:
            return self.__load_tiles()
        else:
            raise ValueError("Either files or tiles must be provided")

    def __load_files(self) -> CityJSON:
        """Load from CityJSON files on the filesystem"""
        files = deepcopy(self.files)
        path_base = Path(files.pop())
        if not path_base.exists():
            raise FileNotFoundError(path_base)
        with path_base.open("r") as fo:
            cm = CityJSON(file=fo)
        if len(files) > 0:
            cmls = []
            for f in files:
                p = Path(f)
                if not p.exists():
                    raise FileNotFoundError(p)
                with p.open("r") as fo:
                    cmls.append(CityJSON(file=fo))
            cm.merge(cmls)
        if self.bbox:
            cm = cm.get_subset_bbox(self.bbox)
        return cm

    def __load_tiles(self) -> CityJSON:
        """Download tile from 3dbag.nl"""
        raise NotImplementedError


class VBOLoader:
    def __init__(self, url: str = None, file: PathLike = None):
        self.url = url
        self.file = file
        self.__index_col = ["vbo_identificatie"]

    def load(self) -> pd.DataFrame:
        if self.url is not None:
            return self.__load_sql()
        elif self.file is not None:
            return self.__load_file()
        else:
            raise ValueError("Either url or file must be set")

    def __load_file(self) -> pd.DataFrame:
        return pd.read_csv(self.file, index_col=self.__index_col)

    def __load_sql(self) -> pd.DataFrame:
        sql_select_vbo = load_sql("select_verblijfsobject.sql")
        return pd.read_sql_query(sql_select_vbo, self.url,
                                 index_col=self.__index_col)


def load_sql(filename: str = None,
             query_params: dict = None):
    """Load SQL from a file and inject parameters if provided.

    If providing query parametes, they need to be in a dict, where the keys are the
    parameter names.

    The SQL script can contain parameters in the form of ``${...}``, which is
    understood by most database managers. This is handy for developing the SQL scripts
    in a database manager, without having to execute the pipeline.
    However, the python formatting only understands ``{...}`` placeholders, so the
    ``$`` are removed from ``${...}`` when the SQL is loaded from the file.

    Args:
        filename (str): SQL File to load (without the path) from the ``sql``
            sub-package. If None, it will load the ``.sql`` file with the name equal to
            the caller function's name.
        query_params (dict): If provided, the templated SQL is formatted with the
            parameters.
    """
    # Find the name of the main package. This should be bag3d.<package>, e.g. bag3d.core
    stk = inspect.stack()[1]
    mod = inspect.getmodule(stk[0])
    pkgs = mod.__package__.split(".")
    if pkgs[0] != "wijklabels" and len(pkgs) < 2:
        raise RuntimeError(
            "Trying to load SQL files from a namspace that is not wijklabels.<package>.")
    sqlfiles_module = ".".join([pkgs[0], pkgs[1], "sqlfiles"])
    # Get the name of the calling function
    _f = filename if filename is not None else f"{inspect.stack()[1].function}.sql"
    _sql = resources.files(sqlfiles_module).joinpath(_f).read_text()
    _pysql = _sql.replace("${", "{")
    # return inject_parameters(_pysql, query_params)
    return _pysql


class ExcelLoader:
    def __init__(self, file: PathLike = None):
        self.file = file

    def load(self) -> Workbook:
        return load_workbook(filename=self.file, read_only=True, data_only=True,
                             keep_vba=False, keep_links=True)


class WoningtypeLoader:
    def __init__(self, file: PathLike = None):
        self.file = file

    def load(self) -> pd.DataFrame:
        df = pd.read_csv(self.file, header=0,
                         converters={"woningtype": to_woningtype, "identificatie": str,
                                     "vbo_identificatie": str})
        return df


class EPLoader:
    """Loads the CSV file of the open energy labels from
    https://www.ep-online.nl/PublicData. The CSV file must be the one with all the
    labels, not the one with the mutations."""

    def __init__(self, file: PathLike = None):
        self.file = file

    def load(self) -> pd.DataFrame:
        """Columns in the csv, 0-indexed:
        5 - energy label
        11 - postcode
        12 - huisnummer
        13 - huisletter
        14 - huisnummertoevoeging
        16 - verblijfsobject ID
        19 - pand ID
        20 - gebouwtype (woningtype)
        21 - gebouwsubtype (woningsubtype)
        """

        def to_energylabel(energieklasse: str):
            try:
                return EnergyLabel(energieklasse)
            except ValueError:
                return pd.NA

        def to_woningtype(gebouwtype: str):
            if (gebouwtype == "Twee-onder-één-kap" or
                    gebouwtype == "Twee-onder-een-kap / rijwoning hoek"):
                return Woningtype.TWEE_ONDER_EEN_KAP
            else:
                try:
                    return Woningtype(gebouwtype.lower())
                except ValueError:
                    return pd.NA

        def to_huisnummer(hnr: str):
            try:
                return int(hnr)
            except ValueError:
                return np.nan

        def to_identificatie(id):
            if len(id) > 1:
                return f"NL.IMBAG.Pand.{id}"
            else:
                return pd.NA

        def to_vbo_identifiactie(id):
            if len(id) > 1:
                return f"NL.IMBAG.Verblijfsobject.{id}"
            else:
                return pd.NA

        def to_date(d):
            return pd.to_datetime(d, format="%Y%m%d")

        usecols = [0, 5, 11, 12, 13, 14, 16, 19, 20, 21]
        converters = {
            "Pand_opnamedatum": to_date,
            "Pand_energieklasse": to_energylabel,
            "Pand_gebouwtype": to_woningtype,
            "Pand_bagpandid": to_identificatie,
            "Pand_bagverblijfsobjectid": to_vbo_identifiactie,
            "Pand_postcode": str,
            "Pand_huisnummer": to_huisnummer,
            "Pand_huisletter": str,
            "Pand_huisnummertoevoeging": str
        }
        df = pd.read_csv(self.file, header=0, usecols=usecols, sep=";",
                         converters=converters)
        df.rename(columns={"Pand_energieklasse": "energylabel",
                           "Pand_gebouwtype": "woningtype",
                           "Pand_bagpandid": "identificatie",
                           "Pand_bagverblijfsobjectid": "vbo_identificatie"},
                  inplace=True)
        # The new NTA method was in place since 2021-01-01
        start_nta8800_method = pd.to_datetime("20210101", format="%Y%m%d")
        return df.loc[df["Pand_opnamedatum"] >= start_nta8800_method]
