CREATE TABLE bdukai_work.lod13_fixed AS
SELECT id
     , fid
     , identificatie
     , b3_dd_id
     , b3_pand_deel_id
     , b3_h_50p
     , b3_h_70p
     , b3_h_min
     , b3_h_max
     , b3_h_maaiveld
     , CASE
           WHEN do_fix IS TRUE THEN 'horizontal'
           ELSE b3_dak_type
    END AS b3_dak_type
FROM bdukai_work.lod13
         LEFT JOIN LATERAL (
    SELECT identificatie, TRUE AS do_fix
    FROM bdukai_work.lod13
    WHERE b3_dak_type = 'multiple horizontal'
    GROUP BY identificatie
    HAVING count(*) = 1
    ) AS sub USING (identificatie);