WITH sub AS (SELECT *
             FROM lvbag.pandactueelbestaand
             WHERE geometrie &&
                   st_makeenvelope(92593.338, 444890.404, 93593.338, 446890.404, 28992))
   , clusters AS (SELECT identificatie
                       , geometrie
                       , st_clusterintersectingwin(geometrie) OVER () AS cluster
                  FROM sub)
   , counts AS (SELECT *, count(*) OVER (PARTITION BY cluster) AS count
                FROM clusters)
   , woningtype_single AS (SELECT identificatie
                                , geometrie
                                , cluster
                                , CASE
                                      WHEN count = 1 THEN 'vrijstaand'
                                      WHEN count = 2 THEN '2 onder 1 kap'
                                      WHEN count > 2 THEN 'rijwoning' END AS wt
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
SELECT identificatie
     , st_astext(geometrie) AS wkt
     , cluster
     , CASE
           WHEN wt = 'rijwoning' AND isect_count = 1 THEN 'rijwoning hoek'
           WHEN wt = 'rijwoning' AND isect_count > 1 THEN 'rijwoning tussen'
           ELSE wt END      AS woningtype
FROM wtype_isect;
