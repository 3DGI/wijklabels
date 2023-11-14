from wijklabels.load import CityJSONLoader, VBOLoader, EPLoader
from wijklabels.woningtype import Woningtype
from numpy import nan


def test_cityjsonloader_files(data_dir):
    path_1 = data_dir / "9-316-552.city.json"
    path_2 = data_dir / "9-316-556.city.json"
    cmloader = CityJSONLoader(files=[path_1, path_2])
    cm = cmloader.load()
    print(cm)


def test_cityjsonloader_files_bbox(data_dir):
    path_1 = data_dir / "9-316-552.city.json"
    path_2 = data_dir / "9-316-556.city.json"
    bbox = (92892, 445701, 93328, 446094)
    cmloader = CityJSONLoader(files=[path_1, path_2], bbox=bbox)
    cm = cmloader.load()
    print(cm)


def test_vbo_load_file(data_dir):
    path_csv = data_dir / "vbo.csv"
    vboloader = VBOLoader(file=path_csv)
    vbo_df = vboloader.load()
    print(vbo_df.head())
    assert len(vbo_df) > 0

def test_eploader():
    path_csv = "/data/energylabel-ep-online/subset.csv"
    eploader = EPLoader(file=path_csv)
    ep_df = eploader.load()
    assert all(ep_df.loc[ep_df["identificatie"] == "NL.IMBAG.Pand.0293100000228679", "woningtype"].isna())
    assert (ep_df.loc[ep_df["identificatie"] == "NL.IMBAG.Pand.0014100040028131", "woningtype"] == Woningtype.RIJWONING_TUSSEN).all()
    assert (ep_df.loc[ep_df["identificatie"] == "NL.IMBAG.Pand.1742100000100812", "woningtype"] == Woningtype.RIJWONING_HOEK).all()
    assert (ep_df.loc[ep_df["identificatie"] == "NL.IMBAG.Pand.1960100003011611", "woningtype"] == Woningtype.VRIJSTAAND).all()
    assert (ep_df.loc[ep_df["identificatie"] == "NL.IMBAG.Pand.0855100000817726", "woningtype"] == Woningtype.TWEE_ONDER_EEN_KAP).all()
