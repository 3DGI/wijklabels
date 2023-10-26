WITH vbo AS (SELECT identificatie AS vbo_identificatie
                  , gebruiksdoel
                  , oppervlakte
                  , pandref
                  , hoofdadresnummeraanduidingref
                  , nevenadresnummeraanduidingref
             FROM lvbag.verblijfsobjectactueelbestaand
             WHERE 'woonfunctie' = ANY (gebruiksdoel))
   , nma AS (SELECT identificatie AS nma_identificatie
                  , huisnummer
                  , huisletter
                  , huisnummertoevoeging
                  , postcode
                  , typeadresseerbaarobject
             FROM lvbag.nummeraanduidingactueelbestaand)
   , vbo_nma AS (SELECT *
                 FROM vbo
                          LEFT JOIN nma ON vbo.hoofdadresnummeraanduidingref =
                                           nma.nma_identificatie)
   , vbo_nma_pd AS (SELECT vbo_nma.vbo_identificatie
                         , pd.identificatie AS pd_identificatie
                         , huisnummer
                         , huisletter
                         , huisnummertoevoeging
                         , postcode
                         , gebruiksdoel
                         , oppervlakte
                    FROM vbo_nma
                             LEFT JOIN lvbag.pandactueelbestaand AS pd
                                       ON vbo_nma.pandref[1] = pd.identificatie
                    WHERE vbo_nma.hoofdadresnummeraanduidingref IS NOT NULL
                      AND pd.geometrie && st_makeenvelope(92593.338, 444890.404, 93593.338, 446890.404, 28992))
SELECT *
FROM vbo_nma_pd
WHERE pd_identificatie IS NOT NULL
  AND postcode IS NOT NULL
ORDER BY pd_identificatie;
