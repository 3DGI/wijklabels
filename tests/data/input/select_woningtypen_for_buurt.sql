WITH buurt AS (SELECT p.identificatie
               FROM lvbag.pand_vbo_single AS p
                        LEFT JOIN lvbag.pand_in_buurt AS b USING (identificatie)
               WHERE buurtcode = ANY
                     (ARRAY ['BU16212254','BU16212231','BU16212241','BU16212211','BU16211231','BU16212321','BU16211211','BU16212121','BU16212221','BU16211221','BU16212111','BU16212131','BU16212144','BU16212311','BU16211552','BU16211311','BU16211331']))
SELECT woningtypen.*
FROM buurt
         JOIN lvbag.woningtypen USING (identificatie);