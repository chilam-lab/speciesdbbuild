CREATE EXTENSION IF NOT EXISTS postgres_fdw;

DROP SERVER IF EXISTS mesh_server CASCADE;

CREATE SERVER mesh_server
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (
  host '__MESHDB_HOST__',
  port '__MESHDB_PORT__',
  dbname '__MESHDB_NAME__'
);

CREATE SCHEMA IF NOT EXISTS mesh_fdw;

DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER mesh_server;

CREATE USER MAPPING FOR CURRENT_USER
SERVER mesh_server
OPTIONS (
  user '__MESHDB_USER__',
  password '__MESHDB_PASS__'
);

IMPORT FOREIGN SCHEMA public
LIMIT TO (
  cat_grid,
  grid_geojson_64km_aoi,
  grid_geojson_32km_aoi,
  grid_geojson_16km_aoi,
  grid_geojson_8km_aoi,
  grid_geojson_state_aoi,
  grid_geojson_mun_aoi,
  grid_geojson_ageb_aoi,
  grid_geojson_cue_aoi,
  -- Tablas finas de celda (no las vistas de region grid_geojson_*): se
  -- necesitan para calcular presencia exacta por especie/celda en
  -- update_sp_snib_cells_batch.sql, a diferencia de cat_taxon.available_grids
  -- que solo necesita la cobertura burda por region.
  grid_64km_aoi,
  grid_32km_aoi,
  grid_16km_aoi,
  grid_8km_aoi,
  grid_state_aoi,
  grid_mun_aoi,
  grid_ageb_aoi,
  grid_cue_aoi
)
FROM SERVER mesh_server
INTO mesh_fdw;
