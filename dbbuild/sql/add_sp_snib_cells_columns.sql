-- Arreglos precalculados de celdas por especie, por resolucion de malla.
-- Sustituyen el ST_Intersects en vivo que hoy hace spv3_controller.js
-- (buildBatchQuery) contra pool_mallas en cada consulta de get_data_byid.
--
-- cells_dirty marca que faltan (re)calcular los arreglos de esa especie.
-- Nace en true (default) para que toda especie existente se calcule la
-- primera vez que corra update_sp_snib_cells_batch.sql, y se vuelve true
-- de nuevo cada vez que update_snib_spid_batch.sql le asigna una ocurrencia
-- nueva (ver ese archivo) -- asi la actualizacion incremental de datos
-- nuevos solo recalcula las especies que realmente cambiaron.

ALTER TABLE sp_snib
  ADD COLUMN IF NOT EXISTS cells_64km   INTEGER[],
  ADD COLUMN IF NOT EXISTS cells_32km   INTEGER[],
  ADD COLUMN IF NOT EXISTS cells_16km   INTEGER[],
  ADD COLUMN IF NOT EXISTS cells_8km    INTEGER[],
  ADD COLUMN IF NOT EXISTS cells_ageb   INTEGER[],
  ADD COLUMN IF NOT EXISTS cells_cue    INTEGER[],
  ADD COLUMN IF NOT EXISTS cells_mun    INTEGER[],
  ADD COLUMN IF NOT EXISTS cells_state  INTEGER[],
  ADD COLUMN IF NOT EXISTS cells_dirty  boolean NOT NULL DEFAULT true;

-- Solo acelera el WHERE cells_dirty = true del batch de recalculo; no es
-- para busquedas por especie (esas ya usan idx_sp_snib_spid).
CREATE INDEX IF NOT EXISTS idx_sp_snib_cells_dirty ON sp_snib(spid) WHERE cells_dirty = true;
