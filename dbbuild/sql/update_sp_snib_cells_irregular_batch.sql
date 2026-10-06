-- Fase 2 del precalculo de celdas por especie: mallas IRREGULARES
-- (ageb/cue/mun/state). Corre despues de update_sp_snib_cells_regular_batch.sql
-- (fase 1, mallas regulares) y es la que cierra cells_dirty=false.
--
-- Toma especies cells_dirty con cells_8km IS NOT NULL (fase 1 ya hecha).
-- Mismo patron de plan que la fase 1 (batch/pts MATERIALIZED): ver el
-- comentario ahi sobre el join invertido contra el GiST de snib.

WITH batch AS MATERIALIZED (
  SELECT spid
  FROM sp_snib
  WHERE cells_dirty = true AND cells_8km IS NOT NULL
  ORDER BY spid
  LIMIT %s
  FOR UPDATE SKIP LOCKED
),
pts AS MATERIALIZED (
  SELECT s.spid, s.the_geom
  FROM batch b
  JOIN snib s ON s.spid = b.spid AND s.the_geom IS NOT NULL
),
cageb AS (
  SELECT p.spid, array_agg(DISTINCT g.gridid_ageb ORDER BY g.gridid_ageb) AS cells
  FROM pts p JOIN grid_ageb_aoi_local g ON ST_Intersects(p.the_geom, g.the_geom)
  GROUP BY p.spid
),
ccue AS (
  SELECT p.spid, array_agg(DISTINCT g.gridid_cue ORDER BY g.gridid_cue) AS cells
  FROM pts p JOIN grid_cue_aoi_local g ON ST_Intersects(p.the_geom, g.the_geom)
  GROUP BY p.spid
),
cmun AS (
  SELECT p.spid, array_agg(DISTINCT g.gridid_mun ORDER BY g.gridid_mun) AS cells
  FROM pts p JOIN grid_mun_aoi_local g ON ST_Intersects(p.the_geom, g.the_geom)
  GROUP BY p.spid
),
cstate AS (
  SELECT p.spid, array_agg(DISTINCT g.gridid_state ORDER BY g.gridid_state) AS cells
  FROM pts p JOIN grid_state_aoi_local g ON ST_Intersects(p.the_geom, g.the_geom)
  GROUP BY p.spid
)
UPDATE sp_snib sp
SET
  cells_ageb  = COALESCE(cageb.cells,  '{}'::integer[]),
  cells_cue   = COALESCE(ccue.cells,   '{}'::integer[]),
  cells_mun   = COALESCE(cmun.cells,   '{}'::integer[]),
  cells_state = COALESCE(cstate.cells, '{}'::integer[]),
  cells_dirty = false
FROM batch b
LEFT JOIN cageb  ON cageb.spid  = b.spid
LEFT JOIN ccue   ON ccue.spid   = b.spid
LEFT JOIN cmun   ON cmun.spid   = b.spid
LEFT JOIN cstate ON cstate.spid = b.spid
WHERE sp.spid = b.spid;
