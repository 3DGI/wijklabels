CREATE SCHEMA IF NOT EXISTS bdukai_work;

CREATE TABLE bdukai_work.floors AS
WITH gb AS (SELECT identificatie, sum(oppervlakte) AS gebruiksoppervlakte
            FROM lvbag.pand_vbo_multi
            GROUP BY identificatie)
SELECT p.identificatie
     , ceil(gb.gebruiksoppervlakte / st_area(p.geometrie))::int4 AS nr_floors
FROM lvbag.pandactueelbestaand AS p
         JOIN gb USING (identificatie);

COMMENT ON TABLE bdukai_work.floors IS 'The estimated number of floors the panden that have multiple VBO-s in them.';