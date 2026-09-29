WITH batch AS (
  SELECT
    ctid,
    COALESCE(reinovalido,'') AS reinovalido,
    COALESCE(phylumdivisionvalido,'') AS phylumdivisionvalido,
    COALESCE(clasevalida,'') AS clasevalida,
    COALESCE(ordenvalido,'') AS ordenvalido,
    COALESCE(familiavalida,'') AS familiavalida,
    COALESCE(generovalido,'') AS generovalido,
    COALESCE(especievalidabusqueda,'') AS especievalidabusqueda
  FROM snib
  WHERE spid IS NULL
  ORDER BY idejemplar
  LIMIT %s
  FOR UPDATE SKIP LOCKED
),
to_update AS (
  SELECT b.ctid, t.spid
  FROM batch b
  JOIN sp_snib t
    ON t.reinovalido = b.reinovalido
   AND t.phylumdivisionvalido = b.phylumdivisionvalido
   AND t.clasevalida = b.clasevalida
   AND t.ordenvalido = b.ordenvalido
   AND t.familiavalida = b.familiavalida
   AND t.generovalido = b.generovalido
   AND t.especievalidabusqueda = b.especievalidabusqueda
),
-- Ocurrencia nueva asignada a una especie (nueva o ya existente): sus
-- arreglos de celdas precalculados (cells_64km, etc.) quedan obsoletos,
-- asi que update_sp_snib_cells_batch.sql la vuelve a tomar en su proximo
-- batch. Reutiliza este mismo mecanismo incremental, no uno nuevo.
--
-- Este UPDATE es un CTE de solo efecto secundario: no lo referencia el
-- UPDATE final, pero Postgres lo ejecuta igual (a diferencia de un SELECT,
-- un CTE que modifica datos SIEMPRE corre, aunque nada lo referencie
-- despues -- ver "Data-Modifying Statements in WITH" en la doc de Postgres).
-- Se deja asi a proposito para que cur.rowcount en build_speciesdb.py siga
-- reflejando filas de snib actualizadas (no de sp_snib), sin tocar la
-- condicion de corte del loop de asignacion de spid.
mark_dirty AS (
  UPDATE sp_snib
  SET cells_dirty = true
  WHERE spid IN (SELECT DISTINCT spid FROM to_update)
  RETURNING spid
)
UPDATE snib s
SET spid = u.spid
FROM to_update u
WHERE s.ctid = u.ctid
  AND s.spid IS NULL;
