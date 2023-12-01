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

    vbo_ids = list(vbo_df.loc[
                       vbo_df["identificatie"] == "NL.IMBAG.Pand.0518100000333865"
                       ].index)
    nr_floors = floors_df.loc["NL.IMBAG.Pand.0518100000333865", "nr_floors"]
    vbo_count = floors_df.loc["NL.IMBAG.Pand.0518100000333865", "vbo_count"]
    woningt = woningtype_df.loc[woningtype_df[
                                    "identificatie"] == "NL.IMBAG.Pand.0518100000333865", "woningtype"]
    vbo_positions = woningtype.distribute_vbo_on_floor(vbo_ids=vbo_ids,
                                                       nr_floors=nr_floors,
                                                       vbo_count=vbo_count)
    appartament_typen = woningtype.classify_apartements(woningtype=woningt,
                                                        vbo_positions=vbo_positions)
    print("\n")
    for a in appartament_typen:
        print(a[1])
