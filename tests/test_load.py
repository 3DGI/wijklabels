from wijklabels.load import CityJSONLoader, VBOLoader


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
