CREATE OR REPLACE VIEW lvbag.multi_pand_single_vbo AS
WITH joined AS (SELECT p.identificatie
                     , p.oorspronkelijkbouwjaar
                     , p.status
                     , vbo.gebruiksdoel
                     , vbo.oppervlakte
                     , vbo.vbo_identificatie
                     , p.geometrie
                FROM lvbag.pandactueelbestaand AS p
                         RIGHT JOIN (SELECT unnest(pandref) AS pandref
                                          , gebruiksdoel
                                          , oppervlakte
                                          , identificatie   AS vbo_identificatie
                                     FROM lvbag.verblijfsobjectactueelbestaand
                                     WHERE 'woonfunctie' = ANY (gebruiksdoel)
                                       AND status IS NOT NULL) AS vbo
                                    ON vbo.pandref = p.identificatie)
   , counts AS (SELECT vbo_identificatie, count(*) AS cnt
                FROM joined
                WHERE identificatie IS NOT NULL
                GROUP BY vbo_identificatie)
SELECT *
FROM counts
WHERE cnt > 1;

COMMENT ON VIEW lvbag.multi_pand_single_vbo IS 'The verblijfsactueelbestaand objects where a single VBO belongs to multiple pand.';