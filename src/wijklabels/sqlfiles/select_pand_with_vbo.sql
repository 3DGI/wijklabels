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
