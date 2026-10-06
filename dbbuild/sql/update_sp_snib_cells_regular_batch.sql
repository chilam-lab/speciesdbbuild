-- Fase 1 del precalculo de celdas por especie: solo mallas REGULARES
-- (64km/32km/16km/8km). Las irregulares (ageb/cue/mun/state) las calcula
-- despues update_sp_snib_cells_irregular_batch.sql. Se separa para que las
-- mallas mas usadas queden listas primero para todo el catalogo: el
-- middleware (get_data_byid) usa cada columna cells_<res> en cuanto deja de
-- ser NULL, sin mirar cells_dirty.
--
-- Toma especies cells_dirty con cells_8km IS NULL (pendientes de fase 1) y
-- deja cells_dirty=true para que la fase 2 las recoja.
--
-- Plan: batch y pts van MATERIALIZED y cada malla se une contra pts (los
-- puntos del lote, leidos de snib una sola vez). Sin esto el planner llegaba
-- a invertir el join para mallas con pocas filas (grid_state: 32 filas):
-- recorria cada poligono contra el GiST de snib (42M puntos) y filtraba por
-- spid al final -- cada lote tardaba 2-4 dias sin importar su tamano.
--
-- Usa las copias locales grid_<res>_aoi_local (ver sync_mesh_grids_local.sql)
-- y FOR UPDATE SKIP LOCKED para poder correr varios workers en paralelo.

WITH batch AS MATERIALIZED (
  SELECT spid
  FROM sp_snib
  WHERE cells_dirty = true AND cells_8km IS NULL
  ORDER BY spid
  LIMIT %s
  FOR UPDATE SKIP LOCKED
),
pts AS MATERIALIZED (
  SELECT s.spid, s.the_geom
  FROM batch b
  JOIN snib s ON s.spid = b.spid AND s.the_geom IS NOT NULL
),
c64km AS (
  SELECT p.spid, array_agg(DISTINCT g.gridid_64km ORDER BY g.gridid_64km) AS cells
  FROM pts p JOIN grid_64km_aoi_local g ON ST_Intersects(p.the_geom, g.the_geom)
  GROUP BY p.spid
),
c32km AS (
  SELECT p.spid, array_agg(DISTINCT g.gridid_32km ORDER BY g.gridid_32km) AS cells
  FROM pts p JOIN grid_32km_aoi_local g ON ST_Intersects(p.the_geom, g.the_geom)
  GROUP BY p.spid
),
c16km AS (
  SELECT p.spid, array_agg(DISTINCT g.gridid_16km ORDER BY g.gridid_16km) AS cells
  FROM pts p JOIN grid_16km_aoi_local g ON ST_Intersects(p.the_geom, g.the_geom)
  GROUP BY p.spid
),
c8km AS (
  SELECT p.spid, array_agg(DISTINCT g.gridid_8km ORDER BY g.gridid_8km) AS cells
  FROM pts p JOIN grid_8km_aoi_local g ON ST_Intersects(p.the_geom, g.the_geom)
  GROUP BY p.spid
)
UPDATE sp_snib sp
SET
  cells_64km = COALESCE(c64km.cells, '{}'::integer[]),
  cells_32km = COALESCE(c32km.cells, '{}'::integer[]),
  cells_16km = COALESCE(c16km.cells, '{}'::integer[]),
  cells_8km  = COALESCE(c8km.cells,  '{}'::integer[])
FROM batch b
LEFT JOIN c64km ON c64km.spid = b.spid
LEFT JOIN c32km ON c32km.spid = b.spid
LEFT JOIN c16km ON c16km.spid = b.spid
LEFT JOIN c8km  ON c8km.spid  = b.spid
WHERE sp.spid = b.spid;
