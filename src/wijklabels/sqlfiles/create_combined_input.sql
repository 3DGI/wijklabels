/* Before running this script:

   The buurten data is downloaded from https://service.pdok.nl/cbs/wijkenbuurten/2022/atom/downloads/wijkenbuurten_2022_v1.gpkg
   and the 'buurten' layer is loaded into the 'public.buurten' table with ogr2ogr.
   ogr2ogr PG:"dbname=<db> host=localhost port=5432 user=<user>" -f PostgreSQL wijkenbuurten_2022_v1.gpkg buurten
   psql -U <user> -p 5432 -h localhost -d <db> -c "COMMENT ON TABLE public.buurten IS 'CBS buurten 2022 imported from https://service.pdok.nl/cbs/wijkenbuurten/2022/atom/downloads/wijkenbuurten_2022_v1.gpkg';"

   RVO shared walls project output.
   It comes in a large CSV which is loaded into the 'public.party_walls' table
   http://godzilla.bk.tudelft.nl/tmp/3dbag_v20231008_rvo_export.zip
   psql -U <user> -p <port> -h localhost -d <db> -f create_party_walls_table.sql
   psql -U <user> -p <port> -h localhost -d <db> -c "\copy public.party_walls FROM '3dbag_v20231008_rvo_export.csv' DELIMITER ',' CSV HEADER;"
   psql -U <user> -p <port> -h localhost -d <db> -c "ALTER TABLE public.party_walls ADD PRIMARY KEY (identificatie);"
   psql -U <user> -p <port> -h localhost -d <db> -c "COMMENT ON TABLE public.party_walls IS '3D BAG party walls data generated for RVO, imported from the delivered CSV, from 3DBAG v20231008';"

   EP-Online data.
   The EP-Online CSV export https://www.ep-online.nl/PublicData is read into the 'public.ep_online' table.
   Any import method works, I used ogr2ogr, which requires that the CSV driver is enabled.
   ogr2ogr -f PostgreSQL PG:"dbname=..." -nln "public.ep_online" v20231101_v2_csv.csv
   */

CREATE SCHEMA IF NOT EXISTS wijklabels;

/* BAG VBOs with 'woonfunctie'
   */
CREATE OR REPLACE VIEW wijklabels.pand_vbo_woonfunctie AS
SELECT p.identificatie AS pand_identificatie
     , vbo.vbo_identificatie
     , p.oorspronkelijkbouwjaar
     , vbo.oppervlakte
     , p.geometrie
FROM lvbag.pandactueelbestaand AS p
         INNER JOIN (SELECT unnest(pandref) AS pandref
                          , gebruiksdoel
                          , oppervlakte
                          , identificatie   AS vbo_identificatie
                     FROM lvbag.verblijfsobjectactueelbestaand
                     WHERE 'woonfunctie' = ANY (gebruiksdoel)
                       AND status IS NOT NULL) AS vbo
                    ON vbo.pandref = p.identificatie;

COMMENT ON VIEW wijklabels.pand_vbo_woonfunctie IS 'The lvbag.pandactueelbestaand objects joined with the VBO where the gebruiksdoel contains woonfunctie.';

/* BAG Pand with 'woonfunctie'
   */
CREATE TABLE wijklabels.pand_woonfunctie AS
SELECT DISTINCT pand_identificatie, geometrie
FROM (SELECT p.identificatie AS pand_identificatie
           , p.geometrie
      FROM lvbag.pandactueelbestaand AS p
               RIGHT JOIN (SELECT unnest(pandref) AS pandref
                           FROM lvbag.verblijfsobjectactueelbestaand
                           WHERE 'woonfunctie' = ANY (gebruiksdoel)
                             AND status IS NOT NULL) AS vbo
                          ON vbo.pandref = p.identificatie) AS sub
WHERE pand_identificatie IS NOT NULL;

COMMENT ON TABLE wijklabels.pand_woonfunctie IS 'The lvbag.pandactueelbestaand objects where the gebruiksdoel of at least one of their VBOs contains woonfunctie.';

CREATE INDEX pand_woonfunctie_geometrie_idx ON wijklabels.pand_woonfunctie USING gist (geometrie);

/* Woningtype
   */
CREATE TABLE wijklabels.woningtypen AS
WITH clusters AS (SELECT pand_identificatie
                       , geometrie
                       , st_clusterintersectingwin(geometrie) OVER () AS cluster
                  FROM wijklabels.pand_woonfunctie)
   , counts AS (SELECT *, count(*) OVER (PARTITION BY cluster) AS count_in_cluster
                FROM clusters)
   , woningtype_single AS (SELECT pand_identificatie
                                , geometrie
                                , cluster
                                , CASE
                                      WHEN count_in_cluster = 1
                                          THEN 'vrijstaande woning'
                                      WHEN count_in_cluster = 2 THEN '2 onder 1 kap'
                                      WHEN count_in_cluster > 2
                                          THEN 'rijwoning' END AS wt
                           FROM counts)
   , isects AS (SELECT id1 AS pand_identificatie, count(*) AS isect_count
                FROM (SELECT pd1.pand_identificatie AS id1
                           , pd2.pand_identificatie AS id2
                      FROM wijklabels.pand_woonfunctie AS pd1
                               LEFT JOIN wijklabels.pand_woonfunctie AS pd2
                                         ON st_intersects(pd1.geometrie, pd2.geometrie)
                      WHERE pd1.pand_identificatie != pd2.pand_identificatie) AS sub
                GROUP BY id1)
   , wtype_isect AS (SELECT *
                     FROM woningtype_single
                              LEFT JOIN isects USING (pand_identificatie))
SELECT pv.vbo_identificatie
     , i.pand_identificatie
     , CASE
           WHEN i.wt = 'rijwoning' AND i.isect_count = 1 THEN 'rijwoning hoek'
           WHEN i.wt = 'rijwoning' AND i.isect_count > 1 THEN 'rijwoning tussen'
           ELSE i.wt END AS woningtype
FROM wijklabels.pand_vbo_woonfunctie AS pv
         LEFT JOIN wtype_isect AS i USING (pand_identificatie);

COMMENT ON TABLE wijklabels.woningtypen IS 'lvbag.pandactueelbestaand objects where the VBO gebruiksdoel contains woonfunctie are classified into vrijstaande woning, 2 onder 1 kap, rijwoning hoek, rijwoning tussen. This is not strictly correct classification, because other types, such as appartements are also included in the four categories.';

CREATE INDEX vbo_identificatie_idx ON wijklabels.woningtypen (vbo_identificatie);

/* Buurten
   */
CREATE OR REPLACE VIEW wijklabels.pand_in_buurt AS
SELECT p.identificatie, 'NL' AS landcode, b.gemeentecode, b.wijkcode, b.buurtcode, b.buurtnaam
FROM lvbag.pandactueelbestaand AS p
         INNER JOIN public.buurten AS b
                    ON st_intersects(st_centroid(p.geometrie), b.geom);

COMMENT ON VIEW wijklabels.pand_in_buurt IS 'lvbag.pandactueelbestaand objects assigned to the buurt that intersect their centroid.';

CREATE OR REPLACE VIEW wijklabels.vbo_in_buurt AS
SELECT v.identificatie, 'NL' AS landcode, b.gemeentecode, b.wijkcode, b.buurtcode, b.buurtnaam
FROM lvbag.verblijfsobjectactueelbestaand AS v
         INNER JOIN public.buurten AS b
                    ON st_intersects(v.geometrie, b.geom);

COMMENT ON VIEW wijklabels.vbo_in_buurt IS 'lvbag.verblijfsobjectactueelbestaand objects assigned to the buurt that intersect it.';

/* Number of floors estimation.
   */
CREATE TABLE wijklabels.floors AS
WITH gb AS (SELECT pand_identificatie
                 , sum(oppervlakte) AS gebruiksoppervlakte
                 , count(*)         AS vbo_count
            FROM wijklabels.pand_vbo_woonfunctie
            GROUP BY pand_identificatie)
SELECT gb.pand_identificatie
     , ceil(gb.gebruiksoppervlakte / pw.b3_opp_grond)::int4 AS nr_floors
     , gb.vbo_count
FROM public.party_walls AS pw
         JOIN gb ON pw.identificatie = gb.pand_identificatie
WHERE pw.b3_opp_grond > 0.0;

COMMENT ON TABLE wijklabels.floors IS 'The estimated number of floors per pand.';

/* Combined table with all necessary attributes
   */
CREATE TABLE wijklabels.input AS
SELECT p.pand_identificatie
     , p.vbo_identificatie
     , p.oorspronkelijkbouwjaar
     , p.oppervlakte
     , p.geometrie
     , w.woningtype
     , b.landcode
     , b.gemeentecode
     , b.wijkcode
     , b.buurtcode
     , f.nr_floors
     , f.vbo_count
     , pw.b3_opp_buitenmuur
     , pw.b3_opp_dak_plat
     , pw.b3_opp_dak_schuin
     , pw.b3_opp_grond
     , pw.b3_opp_scheidingsmuur
FROM wijklabels.pand_vbo_woonfunctie p
         INNER JOIN wijklabels.woningtypen w
                    ON p.pand_identificatie = w.pand_identificatie AND
                       p.vbo_identificatie = w.vbo_identificatie
         INNER JOIN wijklabels.pand_in_buurt b ON p.pand_identificatie = b.identificatie
         INNER JOIN wijklabels.floors f ON p.pand_identificatie = f.pand_identificatie
         INNER JOIN public.party_walls pw ON p.pand_identificatie = pw.identificatie
WHERE pw._betrouwbaar IS TRUE;

COMMENT ON TABLE wijklabels.input IS 'The input data with all the attributes needed for the energy label estimation.';

CREATE INDEX input_pand_identificatie_idx ON wijklabels.input (pand_identificatie);

/* Join the EP-Online data with geometry and the neighborhoods.
   */
CREATE TABLE wijklabels.ep_online_vbo AS
WITH nta_only AS (SELECT 'NL.IMBAG.Pand.' || pand_bagpandid AS pand_identificatie
                       , 'NL.IMBAG.Verblijfsobject.' || pand_bagverblijfsobjectid AS vbo_identificatie
                       , pand_energieklasse AS energylabel
                  FROM public.ep_online
                  WHERE pand_berekeningstype LIKE 'NTA 8800%')
SELECT pand_identificatie
     , vbo_identificatie
     , n.energylabel
     , b.buurtcode
     , p.geometrie
FROM nta_only AS n
         INNER JOIN lvbag.verblijfsobjectactueelbestaand AS p
                    ON n.vbo_identificatie = p.identificatie
         INNER JOIN public.buurten AS b ON st_intersects( p.geometrie, b.geom);

COMMENT ON TABLE wijklabels.ep_online_vbo IS 'The EP-Online data joined with the Verblijfsobject geometries and neighborhoods.';

CREATE INDEX ep_online_vbo_geometrie_idx ON wijklabels.ep_online_vbo USING gist (geometrie);


CREATE TABLE wijklabels.ep_online_pand AS
WITH nta_only AS (SELECT 'NL.IMBAG.Pand.' || pand_bagpandid AS pand_identificatie
                       , array_agg('NL.IMBAG.Verblijfsobject.' || pand_bagverblijfsobjectid) AS vbo_identificatie
                       , array_agg(pand_energieklasse) AS energylabel
                  FROM public.ep_online
                  WHERE pand_berekeningstype LIKE 'NTA 8800%'
                  GROUP BY pand_bagpandid)
SELECT pand_identificatie
     , n.energylabel
     , p.oorspronkelijkbouwjaar
     , b.buurtcode
     , p.geometrie
FROM nta_only AS n
         INNER JOIN lvbag.pandactueelbestaand AS p
                    ON n.pand_identificatie = p.identificatie
         INNER JOIN public.buurten AS b ON st_intersects(st_centroid(p.geometrie), b.geom);

COMMENT ON TABLE wijklabels.ep_online_pand IS 'The EP-Online data joined with the Pand geometries and neighborhoods.';

ALTER TABLE wijklabels.ep_online_pand ADD PRIMARY KEY (pand_identificatie);

CREATE INDEX ep_online_pand_geometrie_idx ON wijklabels.ep_online_pand USING gist (geometrie);
