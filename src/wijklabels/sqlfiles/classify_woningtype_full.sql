CREATE TABLE lvbag.woningtypen AS
WITH clusters AS (SELECT identificatie
                       , geometrie
                       , st_clusterintersectingwin(geometrie) OVER () AS cluster
                  FROM lvbag.pandactueelbestaand)
   , counts AS (SELECT *, count(*) OVER (PARTITION BY cluster) AS count_in_cluster
                FROM clusters)
   , woningtype_single AS (SELECT identificatie
                                , geometrie
                                , cluster
                                , CASE
                                      WHEN count_in_cluster = 1 THEN 'vrijstaande woning'
                                      WHEN count_in_cluster = 2 THEN '2 onder 1 kap'
                                      WHEN count_in_cluster > 2 THEN 'rijwoning' END AS wt
                           FROM counts)
   , isects AS (SELECT id1 AS identificatie, count(*) AS isect_count
                FROM (SELECT pd1.identificatie AS id1, pd2.identificatie AS id2
                      FROM lvbag.pandactueelbestaand AS pd1
                               LEFT JOIN lvbag.pandactueelbestaand AS pd2
                                         ON st_intersects(pd1.geometrie, pd2.geometrie)
                      WHERE pd1.identificatie != pd2.identificatie) AS sub
                GROUP BY id1)
   , wtype_isect AS (SELECT *
                     FROM woningtype_single
                              LEFT JOIN isects USING (identificatie))
SELECT i.identificatie
     , CASE
           WHEN i.wt = 'rijwoning' AND i.isect_count = 1 THEN 'rijwoning hoek'
           WHEN i.wt = 'rijwoning' AND i.isect_count > 1 THEN 'rijwoning tussen'
           ELSE i.wt END AS woningtype
FROM wtype_isect AS i
         INNER JOIN lvbag.pand_vbo_single AS pv USING (identificatie);

COMMENT ON TABLE lvbag.woningtypen IS 'lvbag.pandactueelbestaand objects that have a single verblijfsobject in them and the VBO gebruiksdoel contains woonfunctie are classified into vrijstaande woning, 2 onder 1 kap, rijwoning hoek, rijwoning tussen.';

ALTER TABLE lvbag.woningtypen
    ADD PRIMARY KEY (identificatie);
