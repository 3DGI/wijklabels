from wijklabels import woningtype, load
import pandas as pd


def test_distribute_vbo_on_floor():
    shared_walls_csv = "/home/balazs/Development/wijklabels/tests/data/input/rvo_shared_subset_den_haag.csv"
    shared_walls_loader = load.SharedWallsLoader(shared_walls_csv)
    shared_walls_df = shared_walls_loader.load().query("_betrouwbaar == True")
    # We select only those Pand that have a single VBO, which means that they are
    # houses, not appartaments
    vbo_csv = "/home/balazs/Development/wijklabels/tests/data/input/vbo_buurt.csv"
    vboloader = load.VBOLoader(file=vbo_csv)
    _v = vboloader.load()
    # Remove duplicate VBO, which happens when a Pand is split, so there are two
    # different Pand-ID, but the VBO is duplicated
    vbo_df = _v.loc[~_v.index.duplicated(keep="first"), :].copy()
    del _v
    woningtype_path = "/home/balazs/Development/wijklabels/tests/data/input/woningtypen_all_den_haag.csv"
    woningtypeloader = load.WoningtypeLoader(file=woningtype_path)
    _w = woningtypeloader.load()
    _w.set_index("vbo_identificatie", inplace=True)
    # Remove duplicate VBO, which happens when a Pand is split, so there are two
    # different Pand-ID, but the VBO is duplicated
    woningtype_df = _w.loc[~_w.index.duplicated(keep="first"), :].copy()
    del _w
    floors_df = pd.read_csv(
        "/home/balazs/Development/wijklabels/tests/data/input/floors.csv", header=0)
    floors_df.set_index("identificatie", inplace=True)

    panden = vbo_df.merge(woningtype_df, on="vbo_identificatie", how="inner",
                          validate="1:1")

    pand_ids = vbo_df["identificatie"].unique()
    for pid in pand_ids:
        try:
            vbo_ids = list(vbo_df.loc[vbo_df["identificatie"] == pid].index)
            nr_floors = floors_df.loc[pid, "nr_floors"]
            vbo_count = floors_df.loc[pid, "vbo_count"]
            wtype_pand = woningtype_df.loc[woningtype_df["identificatie"] == pid, "woningtype"]
            vbo_positions = woningtype.distribute_vbo_on_floor(vbo_ids=vbo_ids,
                                                               nr_floors=nr_floors,
                                                               vbo_count=vbo_count)
            apartment_typen = woningtype.classify_apartments(woningtype=wtype_pand,
                                                                vbo_positions=vbo_positions)
            for vbo_id, wtype_vbo in apartment_typen:
                panden.loc[vbo_id, "woningtype"] = wtype_vbo
        except KeyError:
            continue
    print("\n")

