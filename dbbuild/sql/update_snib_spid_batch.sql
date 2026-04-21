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
)
UPDATE snib s
SET spid = u.spid
FROM to_update u
WHERE s.ctid = u.ctid
  AND s.spid IS NULL;
