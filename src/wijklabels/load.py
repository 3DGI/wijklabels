"""Load CityJSON data

Copyright 2023 3DGI
"""
import json
from os import PathLike
from pathlib import Path

from cjio.cityjson import CityJSON

from . import Bbox


class CityJSONLoader:
    """Load CityJSON data from local files or download them directly from 3dbag.nl."""

    def load(self, files: list[PathLike] = None, tiles: list[str] = None,
             bbox: Bbox = None) -> CityJSON:
        if files:
            return self.__load_files(files, bbox)
        elif tiles:
            return self.__load_tiles(tiles, bbox)

    @staticmethod
    def __load_files(files: list[PathLike], bbox: Bbox = None) -> CityJSON:
        """Load from CityJSON files on the filesystem"""
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
        if bbox:
            cm = cm.get_subset_bbox(bbox)
        return cm

    @staticmethod
    def __load_tiles(tiles: list[str], bbox: Bbox = None) -> CityJSON:
        """Download tile from 3dbag.nl"""
        raise NotImplementedError
