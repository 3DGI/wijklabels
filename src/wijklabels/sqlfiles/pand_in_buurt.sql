CREATE OR REPLACE VIEW lvbag.pand_in_buurt AS
SELECT p.identificatie, b.buurtcode, b.buurtnaam
FROM lvbag.pandactueelbestaand AS p
         INNER JOIN bdukai_work.buurten AS b
                    ON st_intersects(st_centroid(p.geometrie), b.geom);

COMMENT ON VIEW lvbag.pand_in_buurt IS 'lvbag.pandactueelbestaand objects assigned to the buurt that intersect their centroid.';