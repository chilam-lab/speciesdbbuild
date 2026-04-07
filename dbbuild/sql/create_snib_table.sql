CREATE TABLE IF NOT EXISTS snib AS
SELECT
  familiavalida, generovalido, especievalida as especievalidabusqueda,
  longitud, latitud, estadomapa, municipiomapa, localidad, fechacolecta, anp,
  formadecrecimiento, fuente, urlejemplar, idejemplar, ultimafechaactualizacion,
  idnombrecatvalido, idnombrecat, reino, phylumdivision, clase, orden, familia, genero, especie,
  autor, estatustax, reftax, taxonvalidado, reinovalido, phylumdivisionvalido, clasevalida, ordenvalido,
  autorvalido, nombrecomun, ambiente, region, datum, geovalidacion, paismapa, claveestadomapa,
  clavemunicipiomapa, "incertidumbreXY", altitudmapa, coleccion, institucion, paiscoleccion, numcatalogo,
  numcolecta, procedenciaejemplar, colector, diacolecta, mescolecta, aniocolecta, tipo, ejemplarfosil,
  proyecto, formadecitar, licenciauso, urlproyecto, urlorigen, obsusoinfo, "version", idestadomapa,
  idmunicipiomapa, comentarioscat, comentarioscatvalido, homonimosgenero, homonimosespecie,
  homonimosinfraespecie, homonimosgenerocatvalido, homonimosespeciecatvalido, homonimosinfraespeciecatvalido,
  categoriataxonomica, categoriainfraespecievalida
FROM informaciongeoportal
WHERE 1=0;

ALTER TABLE snib ADD COLUMN IF NOT EXISTS the_geom geometry(POINT,4326);
ALTER TABLE snib ADD COLUMN IF NOT EXISTS spid integer;
ALTER TABLE snib ADD COLUMN IF NOT EXISTS gid integer;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'snib_idejemplar_pk') THEN
    ALTER TABLE snib ADD CONSTRAINT snib_idejemplar_pk PRIMARY KEY (idejemplar);
  END IF;
END$$;

ALTER TABLE snib ALTER COLUMN localidad TYPE text;
ALTER TABLE snib ALTER COLUMN reftax TYPE text;
ALTER TABLE snib ALTER COLUMN institucion TYPE text;
ALTER TABLE snib ALTER COLUMN colector TYPE text;
ALTER TABLE snib ALTER COLUMN urlproyecto TYPE text;
ALTER TABLE snib ALTER COLUMN obsusoinfo TYPE text;
ALTER TABLE snib ALTER COLUMN comentarioscat TYPE text;
ALTER TABLE snib ALTER COLUMN comentarioscatvalido TYPE text;

ALTER TABLE snib ALTER COLUMN familiavalida TYPE text;
ALTER TABLE snib ALTER COLUMN generovalido TYPE text;
ALTER TABLE snib ALTER COLUMN estadomapa TYPE text;
ALTER TABLE snib ALTER COLUMN fuente TYPE text;
ALTER TABLE snib ALTER COLUMN reino TYPE text;
ALTER TABLE snib ALTER COLUMN phylumdivision TYPE text;
ALTER TABLE snib ALTER COLUMN clase TYPE text;
ALTER TABLE snib ALTER COLUMN orden TYPE text;
ALTER TABLE snib ALTER COLUMN familia TYPE text;
ALTER TABLE snib ALTER COLUMN genero TYPE text;
ALTER TABLE snib ALTER COLUMN reinovalido TYPE text;
ALTER TABLE snib ALTER COLUMN phylumdivisionvalido TYPE text;
ALTER TABLE snib ALTER COLUMN clasevalida TYPE text;
ALTER TABLE snib ALTER COLUMN ordenvalido TYPE text;
ALTER TABLE snib ALTER COLUMN region TYPE text;
ALTER TABLE snib ALTER COLUMN datum TYPE text;
ALTER TABLE snib ALTER COLUMN paismapa TYPE text;
ALTER TABLE snib ALTER COLUMN coleccion TYPE text;
ALTER TABLE snib ALTER COLUMN paiscoleccion TYPE text;
ALTER TABLE snib ALTER COLUMN tipo TYPE text;
ALTER TABLE snib ALTER COLUMN proyecto TYPE text;
ALTER TABLE snib ALTER COLUMN categoriataxonomica TYPE text;
