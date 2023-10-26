"""Load CityJSON and BAG data

Copyright 2023 3DGI
"""
import inspect
from importlib import resources
from os import PathLike
from pathlib import Path
from copy import deepcopy

from cjio.cityjson import CityJSON
import pandas as pd

from . import Bbox


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
