CREATE OR REPLACE VIEW lvbag.pand_vbo AS
SELECT p.identificatie
     , p.oorspronkelijkbouwjaar
     , p.status
     , vbo.gebruiksdoel
     , vbo.oppervlakte
     , vbo.vbo_identificatie
     , st_area(p.geometrie) AS bag_area
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

COMMENT ON VIEW lvbag.pand_vbo IS 'The lvbag.pandactueelbestaand objects where the VBO gebruiksdoel contains woonfunctie.';