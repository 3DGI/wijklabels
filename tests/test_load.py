from wijklabels.load import CityJSONLoader


def test_cityjsonloader_files(data_dir):
    path_1 = data_dir / "9-316-552.city.json"
    path_2 = data_dir / "9-316-556.city.json"
    cmloader = CityJSONLoader()
    cm = cmloader.load(files=[path_1, path_2])
    print(cm)


def test_cityjsonloader_files_bbox(data_dir):
    path_1 = data_dir / "9-316-552.city.json"
    path_2 = data_dir / "9-316-556.city.json"
    bbox = (92892, 445701, 93328, 446094)
    cmloader = CityJSONLoader()
    cm = cmloader.load(files=[path_1, path_2], bbox=bbox)
    print(cm)
