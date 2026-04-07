WITH batch AS (
  SELECT
    idejemplar,
    reinovalido,
    phylumdivisionvalido,
    clasevalida,
    ordenvalido,
    familiavalida,
    generovalido,
    especievalidabusqueda
  FROM snib
  WHERE spid IS NULL
  ORDER BY idejemplar
  LIMIT %s
),
to_update AS (
  SELECT b.idejemplar, t1.spid
  FROM batch b
  JOIN sp_snib t1
    ON b.reinovalido IS NOT DISTINCT FROM t1.reinovalido
   AND b.phylumdivisionvalido IS NOT DISTINCT FROM t1.phylumdivisionvalido
   AND b.clasevalida IS NOT DISTINCT FROM t1.clasevalida
   AND b.ordenvalido IS NOT DISTINCT FROM t1.ordenvalido
   AND b.familiavalida IS NOT DISTINCT FROM t1.familiavalida
   AND b.generovalido IS NOT DISTINCT FROM t1.generovalido
   AND b.especievalidabusqueda IS NOT DISTINCT FROM t1.especievalidabusqueda
)
UPDATE snib s
SET spid = u.spid
FROM to_update u
WHERE s.idejemplar = u.idejemplar
  AND s.spid IS NULL;
