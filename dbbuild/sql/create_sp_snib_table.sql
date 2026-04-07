CREATE TABLE IF NOT EXISTS sp_snib (
    spid bigserial PRIMARY KEY,
    reinovalido text,
    phylumdivisionvalido text,
    clasevalida text,
    ordenvalido text,
    familiavalida text,
    generovalido text,
    especievalidabusqueda text,
    validadoterceros smallint,
    especieepiteto text,
    subgenero text,
    nombreinfra text,
    idcat text,
    idbacktax integer,
    idnombrecatvalido text
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'uq_sp_snib_tax'
    ) THEN
        ALTER TABLE sp_snib
        ADD CONSTRAINT uq_sp_snib_tax UNIQUE (
            reinovalido, phylumdivisionvalido, clasevalida, ordenvalido,
            familiavalida, generovalido, especievalidabusqueda
        );
    END IF;
END$$;

INSERT INTO sp_snib (
    reinovalido, phylumdivisionvalido, clasevalida, ordenvalido,
    familiavalida, generovalido, especievalidabusqueda
)
SELECT DISTINCT
    COALESCE(reinovalido,''),
    COALESCE(phylumdivisionvalido,''),
    COALESCE(clasevalida,''),
    COALESCE(ordenvalido,''),
    COALESCE(familiavalida,''),
    COALESCE(generovalido,''),
    COALESCE(especievalidabusqueda,'')
FROM snib
ON CONFLICT ON CONSTRAINT uq_sp_snib_tax DO NOTHING;

UPDATE sp_snib SET validadoterceros = 1 WHERE validadoterceros IS NULL;

UPDATE sp_snib
SET
  especieepiteto = CASE WHEN left(split_part(especievalidabusqueda,' ',2),1)='('
      THEN split_part(especievalidabusqueda,' ',3) ELSE split_part(especievalidabusqueda,' ',2) END,
  subgenero = CASE WHEN left(split_part(especievalidabusqueda,' ',2),1)='('
      THEN rtrim(ltrim(split_part(especievalidabusqueda,' ',2),'('),')') ELSE '' END,
  nombreinfra = CASE
      WHEN split_part(especievalidabusqueda,' ',3)<>'' AND left(split_part(especievalidabusqueda,' ',2),1)<>'('
          THEN split_part(especievalidabusqueda,' ',3)
      WHEN split_part(especievalidabusqueda,' ',4)<>'' AND left(split_part(especievalidabusqueda,' ',2),1)='('
          THEN split_part(especievalidabusqueda,' ',4)
      ELSE '' END
WHERE especieepiteto IS NULL OR subgenero IS NULL OR nombreinfra IS NULL;

CREATE INDEX IF NOT EXISTS idx_sp_snib_spid ON sp_snib(spid);
CREATE INDEX IF NOT EXISTS idx_sp_snib_idcat ON sp_snib(idcat);
