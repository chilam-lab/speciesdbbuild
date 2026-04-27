DROP TABLE IF EXISTS cat_taxon;

CREATE TABLE cat_taxon (
    id serial4 PRIMARY KEY,
    taxon varchar(20),
    description varchar(250),
    column_taxon varchar(100),
    level_size int4,
    available_grids int4[],
    filter_fields jsonb
);

INSERT INTO cat_taxon (taxon, description, column_taxon, filter_fields)
VALUES
  ('reino','reinos guardados en SNIB','reinovalido','{"min_occ":"integer","in_fosil":"boolean","in_sin_fecha":"boolean"}'),
  ('phylum','phylums guardados en SNIB','phylumdivisionvalido','{"min_occ":"integer","in_fosil":"boolean","in_sin_fecha":"boolean"}'),
  ('clase','clases guardados en SNIB','clasevalida','{"min_occ":"integer","in_fosil":"boolean","in_sin_fecha":"boolean"}'),
  ('orden','ordenes guardados en SNIB','ordenvalido','{"min_occ":"integer","in_fosil":"boolean","in_sin_fecha":"boolean"}'),
  ('familia','familias guardados en SNIB','familiavalida','{"min_occ":"integer","in_fosil":"boolean","in_sin_fecha":"boolean"}'),
  ('genero','generos guardados en SNIB','generovalido','{"min_occ":"integer","in_fosil":"boolean","in_sin_fecha":"boolean"}'),
  ('especie','especies guardados en SNIB','especievalidabusqueda','{"min_occ":"integer","in_fosil":"boolean","in_sin_fecha":"boolean"}');

-- CREATE INDEX IF NOT EXISTS idx_snib_geom_gist ON snib USING GIST(the_geom);

DO $$
DECLARE
  r record;
  total_taxa int;
  i int := 0;
  v_cnt int;
  v_sql text;
  t0 timestamptz;
BEGIN
  t0 := clock_timestamp();
  SELECT count(*) INTO total_taxa FROM cat_taxon;
  RAISE NOTICE '[level_size] Inicio. taxones=%', total_taxa;

  FOR r IN SELECT id, taxon, column_taxon FROM cat_taxon LOOP
    i := i + 1;
    RAISE NOTICE '[level_size] %/% -> taxon=% col=%', i, total_taxa, r.taxon, r.column_taxon;

    v_sql := format(
      'SELECT count(DISTINCT nullif(trim(%I), '''')) FROM sp_snib WHERE nullif(trim(%I), '''') IS NOT NULL',
      r.column_taxon, r.column_taxon
    );
    EXECUTE v_sql INTO v_cnt;
    UPDATE cat_taxon SET level_size = v_cnt WHERE id = r.id;
    RAISE NOTICE '[level_size] calculado taxon=% valor=%', r.taxon, v_cnt;
  END LOOP;

  RAISE NOTICE '[level_size] Fin. tiempo=%', (clock_timestamp() - t0);
END$$;

DO $$
DECLARE
  t record;
  total_targets int;
  i int := 0;
  total_ok int := 0;
  v_sql text;
  has_presence boolean;
  has_region_col boolean;
  has_border_col boolean;
  t0 timestamptz;
BEGIN
  t0 := clock_timestamp();
  CREATE TEMP TABLE tmp_mesh_presence (
    table_view_name text,
    region_id int4,
    has_presence boolean,
    PRIMARY KEY (table_view_name, region_id)
  ) ON COMMIT DROP;

  SELECT count(*)
  INTO total_targets
  FROM (
    SELECT DISTINCT table_view_name, region_id
    FROM mesh_fdw.cat_grid
    WHERE table_view_name IS NOT NULL
      AND region_id IS NOT NULL
  ) q;

  RAISE NOTICE '[available_grids] Inicio. combinaciones (vista,region)=%', total_targets;

  FOR t IN
    SELECT DISTINCT table_view_name, region_id
    FROM mesh_fdw.cat_grid
    WHERE table_view_name IS NOT NULL
      AND region_id IS NOT NULL
    ORDER BY table_view_name, region_id
  LOOP
    i := i + 1;
    RAISE NOTICE '[available_grids] %/% -> vista=% region=%', i, total_targets, t.table_view_name, t.region_id;

    IF to_regclass(format('mesh_fdw.%I', t.table_view_name)) IS NULL THEN
      RAISE NOTICE '[available_grids] omite: tabla no existe mesh_fdw.%', t.table_view_name;
      INSERT INTO tmp_mesh_presence VALUES (t.table_view_name, t.region_id, false)
      ON CONFLICT DO NOTHING;
      CONTINUE;
    END IF;

    SELECT EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = 'mesh_fdw'
        AND table_name = t.table_view_name
        AND column_name = 'region_id'
    ) INTO has_region_col;

    SELECT EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = 'mesh_fdw'
        AND table_name = t.table_view_name
        AND column_name = 'border'
    ) INTO has_border_col;

    IF NOT has_region_col OR NOT has_border_col THEN
      RAISE NOTICE '[available_grids] omite mesh_fdw.%: falta region_id o border', t.table_view_name;
      INSERT INTO tmp_mesh_presence VALUES (t.table_view_name, t.region_id, false)
      ON CONFLICT DO NOTHING;
      CONTINUE;
    END IF;

    v_sql := format($f$
      SELECT EXISTS (
        SELECT 1
        FROM snib s
        JOIN mesh_fdw.%I g
          ON g.region_id = %s
         AND s.the_geom IS NOT NULL
         AND s.the_geom && g.border
         AND ST_Covers(g.border, s.the_geom)
        LIMIT 1
      )
    $f$, t.table_view_name, t.region_id);

    BEGIN
      EXECUTE v_sql INTO has_presence;
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE '[available_grids] error evaluando %.%: %', t.table_view_name, t.region_id, SQLERRM;
      has_presence := false;
    END;

    IF has_presence THEN
      total_ok := total_ok + 1;
      RAISE NOTICE '[available_grids] presencia=true para vista=% region=%', t.table_view_name, t.region_id;
    ELSE
      RAISE NOTICE '[available_grids] presencia=false para vista=% region=%', t.table_view_name, t.region_id;
    END IF;

    INSERT INTO tmp_mesh_presence (table_view_name, region_id, has_presence)
    VALUES (t.table_view_name, t.region_id, has_presence)
    ON CONFLICT (table_view_name, region_id)
    DO UPDATE SET has_presence = EXCLUDED.has_presence;
  END LOOP;

  UPDATE cat_taxon ct
  SET available_grids = COALESCE((
    SELECT array_agg(cg.grid_id ORDER BY cg.grid_id)
    FROM mesh_fdw.cat_grid cg
    JOIN tmp_mesh_presence p
      ON p.table_view_name = cg.table_view_name
     AND p.region_id = cg.region_id
    WHERE p.has_presence
  ), '{}'::int4[]);

  RAISE NOTICE '[available_grids] Fin. combinaciones_con_presencia=% de %', total_ok, total_targets;
  RAISE NOTICE '[available_grids] Fin. tiempo=%', (clock_timestamp() - t0);
END$$;
