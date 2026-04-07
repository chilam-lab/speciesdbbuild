UPDATE snib
SET
  reinovalido = COALESCE(reinovalido, ''),
  phylumdivisionvalido = COALESCE(phylumdivisionvalido, ''),
  clasevalida = COALESCE(clasevalida, ''),
  ordenvalido = COALESCE(ordenvalido, ''),
  familiavalida = COALESCE(familiavalida, ''),
  generovalido = COALESCE(generovalido, ''),
  especievalidabusqueda = COALESCE(especievalidabusqueda, '')
WHERE reinovalido IS NULL
   OR phylumdivisionvalido IS NULL
   OR clasevalida IS NULL
   OR ordenvalido IS NULL
   OR familiavalida IS NULL
   OR generovalido IS NULL
   OR especievalidabusqueda IS NULL;

UPDATE snib
SET the_geom = ST_SetSRID(ST_MakePoint(longitud,latitud),4326)
WHERE the_geom IS NULL AND longitud IS NOT NULL AND latitud IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_snib_geom ON snib USING GIST(the_geom);
CREATE INDEX IF NOT EXISTS idx_snib_clasevalida ON snib(clasevalida);
CREATE INDEX IF NOT EXISTS idx_snib_ordenvalido ON snib(ordenvalido);
CREATE INDEX IF NOT EXISTS idx_snib_familiavalida ON snib(familiavalida);
CREATE INDEX IF NOT EXISTS idx_snib_generovalido ON snib(generovalido);
CREATE INDEX IF NOT EXISTS idx_snib_especievalidabusqueda ON snib(especievalidabusqueda);
CREATE INDEX IF NOT EXISTS idx_snib_categoriainfraespecievalida ON snib(categoriainfraespecievalida);
CREATE INDEX IF NOT EXISTS idx_snib_latitud ON snib(latitud);
CREATE INDEX IF NOT EXISTS idx_snib_longitud ON snib(longitud);
CREATE INDEX IF NOT EXISTS idx_snib_fechacolecta ON snib(fechacolecta);
CREATE INDEX IF NOT EXISTS idx_snib_phylumdivisionvalido ON snib(phylumdivisionvalido);
CREATE INDEX IF NOT EXISTS idx_snib_idnombrecatvalido ON snib(idnombrecatvalido);
CREATE INDEX IF NOT EXISTS idx_snib_idnombrecat ON snib(idnombrecat);
CREATE INDEX IF NOT EXISTS idx_snib_ejemplarfosil ON snib(ejemplarfosil);
CREATE INDEX IF NOT EXISTS idx_snib_aniocolecta ON snib(aniocolecta);
