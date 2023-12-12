CREATE OR REPLACE VIEW lvbag.pand_vbo_single AS
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
SELECT *
FROM joined
         RIGHT JOIN LATERAL (
    SELECT identificatie
    FROM joined
    GROUP BY identificatie
    HAVING count(*) = 1) AS sub USING (identificatie);

COMMENT ON VIEW lvbag.pand_vbo_single IS 'The lvbag.pandactueelbestaand objects that have a single verblijfsobject in them and the VBO gebruiksdoel contains woonfunctie.';

CREATE OR REPLACE VIEW lvbag.pand_vbo_multi AS
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
SELECT *
FROM joined
         RIGHT JOIN LATERAL (
    SELECT identificatie
    FROM joined
    GROUP BY identificatie
    HAVING count(*) > 1) AS sub USING (identificatie);

COMMENT ON VIEW lvbag.pand_vbo_multi IS 'The lvbag.pandactueelbestaand objects that have multiple verblijfsobject in them and the VBO gebruiksdoel contains woonfunctie.';

CREATE OR REPLACE VIEW lvbag.pand_vbo_woonfunctie AS
SELECT p.identificatie AS pand_identificatie
     , p.oorspronkelijkbouwjaar
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
                    ON vbo.pandref = p.identificatie;

COMMENT ON VIEW lvbag.pand_vbo_woonfunctie IS 'The lvbag.pandactueelbestaand objects joined with the VBO where the gebruiksdoel contains woonfunctie.';
