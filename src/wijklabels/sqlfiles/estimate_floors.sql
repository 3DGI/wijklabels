CREATE SCHEMA IF NOT EXISTS bdukai_work;

CREATE TABLE bdukai_work.floor_per_ground AS
WITH gb AS (SELECT p.identificatie, sum(oppervlakte) AS gebruiksoppervlakte
            FROM lvbag.pandactueelbestaand AS p
                     RIGHT JOIN (SELECT unnest(pandref) AS pandref
                                      , gebruiksdoel
                                      , oppervlakte
                                      , identificatie   AS vbo_identificatie
                                 FROM lvbag.verblijfsobjectactueelbestaand
                                 WHERE 'woonfunctie' = ANY (gebruiksdoel)
                                   AND status IS NOT NULL) AS vbo
                                ON vbo.pandref = p.identificatie
            GROUP BY p.identificatie)
SELECT p.identificatie
     , gb.gebruiksoppervlakte / st_area(p.geometrie) AS floor_per_ground
     , p.geometrie
FROM lvbag.pandactueelbestaand AS p
         JOIN gb USING (identificatie);