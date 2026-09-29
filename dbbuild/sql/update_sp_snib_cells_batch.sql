-- Calcula/recalcula, para un lote de especies marcadas cells_dirty=true,
-- en que celda de cada resolucion de malla cae al menos una de sus
-- ocurrencias (snib.the_geom) -- el mismo ST_Intersects que hoy hace
-- buildBatchQuery en spv3_controller.js contra pool_mallas, pero calculado
-- una sola vez aqui y guardado, en vez de en cada consulta.
--
-- Requiere las copias locales grid_<resolucion>_aoi_local (ver
-- sync_mesh_grids_local.sql) -- NO usa mesh_fdw.grid_*_aoi directamente:
-- postgres_fdw no empuja ST_Intersects al servidor remoto, asi que un join
-- espacial contra la tabla foranea cae en un Nested Loop sobre la tabla
-- completa (grid_8km_aoi tiene 1.2M filas) -- confirmado con EXPLAIN.
--
-- FOR UPDATE SKIP LOCKED permite correr esto en paralelo (varios workers)
-- sin que dos procesos tomen la misma especie a la vez, igual que ya hace
-- update_snib_spid_batch.sql para la asignacion de spid.

WITH batch AS (
  SELECT spid
  FROM sp_snib
  WHERE cells_dirty = true
  ORDER BY spid
  LIMIT %s
  FOR UPDATE SKIP LOCKED
),
c64km AS (
  SELECT s.spid, array_agg(DISTINCT g.gridid_64km ORDER BY g.gridid_64km) AS cells
  FROM batch b
  JOIN snib s ON s.spid = b.spid AND s.the_geom IS NOT NULL
  JOIN grid_64km_aoi_local g ON ST_Intersects(s.the_geom, g.the_geom)
  GROUP BY s.spid
),
c32km AS (
  SELECT s.spid, array_agg(DISTINCT g.gridid_32km ORDER BY g.gridid_32km) AS cells
  FROM batch b
  JOIN snib s ON s.spid = b.spid AND s.the_geom IS NOT NULL
  JOIN grid_32km_aoi_local g ON ST_Intersects(s.the_geom, g.the_geom)
  GROUP BY s.spid
),
c16km AS (
  SELECT s.spid, array_agg(DISTINCT g.gridid_16km ORDER BY g.gridid_16km) AS cells
  FROM batch b
  JOIN snib s ON s.spid = b.spid AND s.the_geom IS NOT NULL
  JOIN grid_16km_aoi_local g ON ST_Intersects(s.the_geom, g.the_geom)
  GROUP BY s.spid
),
c8km AS (
  SELECT s.spid, array_agg(DISTINCT g.gridid_8km ORDER BY g.gridid_8km) AS cells
  FROM batch b
  JOIN snib s ON s.spid = b.spid AND s.the_geom IS NOT NULL
  JOIN grid_8km_aoi_local g ON ST_Intersects(s.the_geom, g.the_geom)
  GROUP BY s.spid
),
cageb AS (
  SELECT s.spid, array_agg(DISTINCT g.gridid_ageb ORDER BY g.gridid_ageb) AS cells
  FROM batch b
  JOIN snib s ON s.spid = b.spid AND s.the_geom IS NOT NULL
  JOIN grid_ageb_aoi_local g ON ST_Intersects(s.the_geom, g.the_geom)
  GROUP BY s.spid
),
ccue AS (
  SELECT s.spid, array_agg(DISTINCT g.gridid_cue ORDER BY g.gridid_cue) AS cells
  FROM batch b
  JOIN snib s ON s.spid = b.spid AND s.the_geom IS NOT NULL
  JOIN grid_cue_aoi_local g ON ST_Intersects(s.the_geom, g.the_geom)
  GROUP BY s.spid
),
cmun AS (
  SELECT s.spid, array_agg(DISTINCT g.gridid_mun ORDER BY g.gridid_mun) AS cells
  FROM batch b
  JOIN snib s ON s.spid = b.spid AND s.the_geom IS NOT NULL
  JOIN grid_mun_aoi_local g ON ST_Intersects(s.the_geom, g.the_geom)
  GROUP BY s.spid
),
cstate AS (
  SELECT s.spid, array_agg(DISTINCT g.gridid_state ORDER BY g.gridid_state) AS cells
  FROM batch b
  JOIN snib s ON s.spid = b.spid AND s.the_geom IS NOT NULL
  JOIN grid_state_aoi_local g ON ST_Intersects(s.the_geom, g.the_geom)
  GROUP BY s.spid
)
UPDATE sp_snib sp
SET
  cells_64km  = COALESCE(c64km.cells,  '{}'::integer[]),
  cells_32km  = COALESCE(c32km.cells,  '{}'::integer[]),
  cells_16km  = COALESCE(c16km.cells,  '{}'::integer[]),
  cells_8km   = COALESCE(c8km.cells,   '{}'::integer[]),
  cells_ageb  = COALESCE(cageb.cells,  '{}'::integer[]),
  cells_cue   = COALESCE(ccue.cells,   '{}'::integer[]),
  cells_mun   = COALESCE(cmun.cells,   '{}'::integer[]),
  cells_state = COALESCE(cstate.cells, '{}'::integer[]),
  cells_dirty = false
FROM batch b
LEFT JOIN c64km  ON c64km.spid  = b.spid
LEFT JOIN c32km  ON c32km.spid  = b.spid
LEFT JOIN c16km  ON c16km.spid  = b.spid
LEFT JOIN c8km   ON c8km.spid   = b.spid
LEFT JOIN cageb  ON cageb.spid  = b.spid
LEFT JOIN ccue   ON ccue.spid   = b.spid
LEFT JOIN cmun   ON cmun.spid   = b.spid
LEFT JOIN cstate ON cstate.spid = b.spid
WHERE sp.spid = b.spid;
