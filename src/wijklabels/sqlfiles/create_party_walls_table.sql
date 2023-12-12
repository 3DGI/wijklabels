CREATE TABLE public.party_walls (
  identificatie text,
  b3_bag_bag_overlap double precision,
  b3_opp_grond double precision,
  b3_opp_dak_plat double precision,
  b3_opp_dak_schuin double precision,
  b3_opp_scheidingsmuur double precision,
  b3_opp_buitenmuur double precision,
  b3_pw_datum integer,
  b3_volume_lod22 double precision,
  oorspronkelijkbouwjaar integer,
  status text,
  b3_val3dity_lod22 text,
  _opp_bag_polygoon double precision,
  _opp_grond_verlies double precision,
  _ratio_grond_tot_volume double precision,
  _ratio_dak_tot_volume double precision,
  _ratio_buitenmuur_tot_volume double precision,
  _betrouwbaar bool
);

COMMENT ON TABLE public.party_walls IS 'Party walls calculated for RVO, imported from CSV.';