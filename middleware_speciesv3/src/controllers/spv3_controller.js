var debug = require('debug')('verbs:controllers')
var verb_utils = require('./verb_utils')
var pgp = require('pg-promise')()
var config = require('../../config')

var pool = verb_utils.pool 
var pool_mallas = verb_utils.pool_mallas 

let dic_taxon_data = new Map();
dic_taxon_data.set('especievalidabusqueda','{"reino":"\'||reinovalido||\'","phylum":"\'||phylumdivisionvalido||\'","clase":"\'||clasevalida||\'","orden":"\'||ordenvalido||\'", "familia":"\'||familiavalida||\'", "genero":"\'||generovalido||\'", "especie":"\'||especievalidabusqueda||\'"}')
dic_taxon_data.set('generovalido','{"reino":"\'||reinovalido||\'","phylum":"\'||phylumdivisionvalido||\'","clase":"\'||clasevalida||\'","orden":"\'||ordenvalido||\'", "familia":"\'||familiavalida||\'", "genero":"\'||generovalido||\'"}')
dic_taxon_data.set('familiavalida','{"reino":"\'||reinovalido||\'","phylum":"\'||phylumdivisionvalido||\'","clase":"\'||clasevalida||\'","orden":"\'||ordenvalido||\'", "familia":"\'||familiavalida||\'"}')
dic_taxon_data.set('ordenvalido','{"reino":"\'||reinovalido||\'","phylum":"\'||phylumdivisionvalido||\'","clase":"\'||clasevalida||\'","orden":"\'||ordenvalido||\'"}')
dic_taxon_data.set('clasevalida','{"reino":"\'||reinovalido||\'","phylum":"\'||phylumdivisionvalido||\'","clase":"\'||clasevalida||\'"}')
dic_taxon_data.set('phylumdivisionvalido','{"reino":"\'||reinovalido||\'","phylum":"\'||phylumdivisionvalido||\'"}')
dic_taxon_data.set('reinovalido','{"reino":"\'||reinovalido||\'"}')


let dic_taxon_group = new Map();
dic_taxon_group.set('especievalidabusqueda','especievalidabusqueda, reinovalido, phylumdivisionvalido, clasevalida, ordenvalido, familiavalida, generovalido')
dic_taxon_group.set('generovalido','generovalido, reinovalido, phylumdivisionvalido, clasevalida, ordenvalido, familiavalida')
dic_taxon_group.set('familiavalida','familiavalida, reinovalido, phylumdivisionvalido, clasevalida, ordenvalido')
dic_taxon_group.set('ordenvalido','ordenvalido, reinovalido, phylumdivisionvalido, clasevalida')
dic_taxon_group.set('clasevalida','clasevalida, reinovalido, phylumdivisionvalido')
dic_taxon_group.set('phylumdivisionvalido','phylumdivisionvalido, reinovalido')
dic_taxon_group.set('reinovalido','reinovalido')

let valid_filters = ["levels_id","reino","phylum","clase","orden","familia","genero","especie"]

let dic_taxon_db = new Map();
dic_taxon_db.set('levels_id','spid')
dic_taxon_db.set('especie','especievalidabusqueda')
dic_taxon_db.set('genero','generovalido')
dic_taxon_db.set("familia",'familiavalida')
dic_taxon_db.set('orden','ordenvalido')
dic_taxon_db.set('clase','clasevalida')
dic_taxon_db.set('phylum','phylumdivisionvalido')
dic_taxon_db.set('reino','reinovalido')

exports.variables = async function (req, res) {
  try {
    const data = await pool.any(
      `SELECT id, taxon AS variable, level_size, filter_fields, available_grids
       FROM cat_taxon
       ORDER BY id;`,
      {}
    );

    return res.status(200).json({ data });
  } catch (error) {
    debug(error);
    return res.status(500).json({
      message: "Error interno al obtener el catálogo de variables"
    });
  }
};


exports.secuencia = async function(req, res) {
  try {

    const { variableLevel, variableValue, nextVariableLevel } = req.body || {};

    // 1) Validación de entrada
    if (!variableLevel || !nextVariableLevel || variableValue === undefined || variableValue === null || variableValue === "") {
      return res.status(400).json({
        message: "Parámetros requeridos: variableLevel, variableValue, nextVariableLevel"
      });
    }

    // console.log("variableLevel: " + dic_taxon_db.get(variableLevel))
    // console.log("variableValue: " + variableValue)
    // console.log("nextVariableLevel: " + dic_taxon_db.get(nextVariableLevel))

    const currentColumn = dic_taxon_db.get(variableLevel);
    const nextColumn = dic_taxon_db.get(nextVariableLevel);

    if (!currentColumn || !nextColumn) {
      return res.status(400).json({
        message: "variableLevel o nextVariableLevel no son válidos"
      });
    }

    // 2) Query segura para nombres de columna dinámicos
    const currentColSql = pgp.as.name(currentColumn);
    const nextColSql = pgp.as.name(nextColumn);

    const query = `
      SELECT DISTINCT
        ${nextColSql} AS value,
        ${nextColSql} AS label
      FROM sp_snib ss
      WHERE ${nextColSql} <> ''
        AND ${currentColSql} = $1
      ORDER BY ${nextColSql};
    `;

    const data = await pool.any(query, [variableValue]);
    return res.status(200).json({ data });

  } catch (error) {
    debug(error);
    return res.status(500).json({
      message: "Error interno al obtener secuencia"
    });
  }

}

exports.get_sourceinfo = async function (req, res) {
  try {
    const data = await pool.oneOrNone(
      `SELECT
         name,
         description,
         source_url,
         download_url,
         dict_url
       FROM data_source_info
       ORDER BY updated_at DESC, id DESC
       LIMIT 1;`,
      {}
    );

    if (!data) {
      return res.status(404).json({
        message: "No se encontró información de la fuente de datos"
      });
    }

    return res.status(200).json({ data });
  } catch (error) {
    debug(error);
    return res.status(500).json({
      message: "Error interno al obtener información de la fuente de datos"
    });
  }
};




exports.get_variable_byid = async function (req, res) {
  try {
    const variable_id = Number(req.params.id);
    const q = verb_utils.getParam(req, 'q', '');
    const offset = Number(verb_utils.getParam(req, 'offset', 0));
    const limit = Number(verb_utils.getParam(req, 'limit', 10));

    if (!Number.isInteger(variable_id) || variable_id <= 0) {
      return res.status(400).json({ message: "El parámetro id es inválido" });
    }

    if (!Number.isInteger(offset) || offset < 0 || !Number.isInteger(limit) || limit <= 0) {
      return res.status(400).json({ message: "Parámetros offset/limit inválidos" });
    }

    const query_array = [];
    const filter_separator = ";";
    const pair_separator = "=";
    const group_separator = ",";

    if (q !== "") {
      const array_queries = q.split(filter_separator).map(x => x.trim()).filter(Boolean);

      for (const filter of array_queries) {
        const filter_pair = filter.split(pair_separator);

        if (filter_pair.length !== 2) {
          return res.status(400).json({ message: `Filtro inválido por composición: ${filter}` });
        }

        const filter_param = filter_pair[0].trim();

        if (valid_filters.indexOf(filter_param) === -1) {
          return res.status(400).json({ message: `Filtro inválido: ${filter_param}` });
        }

        const filter_values = filter_pair[1].trim().split(group_separator).map(v => v.trim()).filter(Boolean);

        if (filter_values.length === 0) {
          return res.status(400).json({ message: `Filtro sin valores: ${filter_param}` });
        }

        let query_temp = "( ";
        filter_values.forEach((value, index) => {
          const col = dic_taxon_db.get(filter_param);
          const op = index === 0 ? "" : "or ";

          if (filter_param === "levels_id") {
            const num = Number(value);
            if (!Number.isInteger(num) || num <= 0) return; // ignora valores inválidos
            query_temp += `${op}${col} = ${num} `;
          } else {
            const safeValue = String(value).replace(/'/g, "''");

            if (filter_param === "especie") {
              // exacto para no incluir subespecies
              query_temp += `${op}lower(${col}) = lower('${safeValue}') `;
            } else {
              // mantiene comportamiento actual por prefijo para otros taxones
              query_temp += `${op}lower(${col}) like lower('${safeValue}%') `;
            }
          }
        });
        query_temp += " )";


        query_array.push(query_temp);
      }
    }

    const taxonRow = await pool.oneOrNone(
      "SELECT id, column_taxon FROM cat_taxon WHERE id = $1",
      [variable_id]
    );

    if (!taxonRow) {
      return res.status(404).json({ message: "No existe la variable solicitada" });
    }

    const { id, column_taxon } = taxonRow;

    let query = `select $<id:raw> as id, array_agg(spid) as level_id, ('$<dic_taxon_data:raw>')::jsonb as datos
      from sp_snib
      where $<column_taxon:raw> <> '' {queries}
      group by $<dic_taxon_group:raw>
      order by $<column_taxon:raw>
      offset $<offset:raw>
      limit $<limit:raw>`;

    query_array.forEach((query_temp) => {
      query = query.replace("{queries}", ` and ${query_temp} {queries} `);
    });

    query = query.replace("{queries}", "");
    query = query.replace(/levels_id/g, "spid");

    const data = await pool.any(query, {
      id,
      column_taxon,
      dic_taxon_data: dic_taxon_data.get(column_taxon),
      dic_taxon_group: dic_taxon_group.get(column_taxon),
      offset,
      limit
    });

    return res.status(200).json({ data });
  } catch (error) {
    debug(error);
    return res.status(500).json({
      message: "Error interno al obtener datos"
    });
  }
};





exports.get_data_byid = async function (req, res) {
  try {
    const variable_id = Number(req.params.id);
    const grid_id = Number(verb_utils.getParam(req, "grid_id", 1));
    const levels_id_raw = verb_utils.getParam(req, "levels_id", []);
    const filter_names = verb_utils.getParam(req, "filter_names", []);
    const filter_values = verb_utils.getParam(req, "filter_values", []);

    if (!Number.isInteger(variable_id) || variable_id <= 0) {
      return res.status(400).json({ message: "El parámetro id es inválido" });
    }

    if (!Number.isInteger(grid_id) || grid_id <= 0) {
      return res.status(400).json({ message: "El parámetro grid_id es inválido" });
    }

    const levels_id = Array.isArray(levels_id_raw)
      ? levels_id_raw.map(Number).filter((n) => Number.isInteger(n) && n > 0)
      : [];

    if (levels_id.length === 0) {
      return res.status(400).json({ message: "levels_id debe ser un arreglo con al menos un valor válido" });
    }

    if (!Array.isArray(filter_names) || !Array.isArray(filter_values) || filter_names.length !== filter_values.length) {
      return res.status(400).json({ message: "filter_names y filter_values deben ser arreglos del mismo tamaño" });
    }

    const taxonRow = await pool.oneOrNone(
      "SELECT id, column_taxon FROM cat_taxon WHERE id = $1",
      [variable_id]
    );

    if (!taxonRow) {
      return res.status(404).json({ message: "No existe la variable solicitada" });
    }

    const column_taxon = taxonRow.column_taxon;
    if (!dic_taxon_data.get(column_taxon) || !dic_taxon_group.get(column_taxon)) {
      return res.status(500).json({ message: "Configuración taxonómica inválida para la variable solicitada" });
    }

    const gridInfo = await pool_mallas.oneOrNone(
      "SELECT resolution, table_cell_name FROM cat_grid WHERE grid_id = $1",
      [grid_id]
    );

    if (!gridInfo) {
      return res.status(404).json({ message: "No existe la malla solicitada (grid_id)" });
    }

    const res_column = "g.gridid_" + gridInfo.resolution;
    const table_cell_name = gridInfo.table_cell_name;

    // Cache bajo demanda: sp_snib.cells_<resolucion> lo llena el batch de
    // dbbuild/sql/update_sp_snib_cells_batch.sql (precalculo de fondo), pero
    // no hay que esperarlo -- si una especie ya se calculo (por el batch o
    // por una consulta previa que la cacheo aqui mismo), se usa directo sin
    // tocar snib/pool_mallas. Solo es seguro sin filtros activos: min_occ/
    // in_fosil/in_sin_fecha cambian que ocurrencias cuentan, y el precalculo
    // asume la vista sin filtrar.
    const VALID_RESOLUTIONS = new Set(["64km", "32km", "16km", "8km", "ageb", "cue", "mun", "state"]);
    const canUseCache = filter_names.length === 0 && VALID_RESOLUTIONS.has(gridInfo.resolution);
    const cellsColumn = "cells_" + gridInfo.resolution;

    let cachedRows = [];
    let pendingLevelsId = levels_id;

    if (canUseCache) {
      cachedRows = await pool.any(
        `SELECT spid, ${cellsColumn} AS cells, ('$<dic_taxon_data:raw>')::jsonb AS datos
         FROM sp_snib
         WHERE spid IN ($<spids:csv>) AND ${cellsColumn} IS NOT NULL`,
        { spids: levels_id, dic_taxon_data: dic_taxon_data.get(column_taxon) }
      ).catch((err) => { debug('cache lookup:', err.message); return []; });

      const cachedSpids = new Set(cachedRows.map((r) => r.spid));
      pendingLevelsId = levels_id.filter((id) => !cachedSpids.has(id));
    }

    // Per-species query: always returns one row per spid with its occurrence points.
    // Large IN-lists (e.g. 2268 species in Mammalia) trigger a seq-scan on the
    // 42M-row snib table and time out.  We avoid this by batching: 10 spids per
    // query keeps PostgreSQL on an index scan (~1.3 s).  Waves of 10 concurrent
    // batches prevent pool-connection timeouts (connectionTimeoutMillis = 5 s).
    //
    const SPID_BATCH        = 10;
    const WAVE_SIZE         = 10;
    const MAX_PTS_PER_SPID  = 10000;

    let queryPts = `
      SELECT DISTINCT
        spid,
        array_agg(st_astext(the_geom)) AS points,
        ('$<dic_taxon_data:raw>')::jsonb AS datos
      FROM snib s
      WHERE spid IN ($<spids:csv>)
        AND the_geom IS NOT NULL
        {in_fosil} {in_sin_fecha}
      GROUP BY spid, $<dic_taxon_group:raw>
      {min_occ}
    `;

    for (let i = 0; i < filter_names.length; i++) {
      const filter_param = filter_names[i];
      const filter_value = filter_values[i];

      if (filter_param === "min_occ") {
        const min = Number(filter_value);
        if (!Number.isFinite(min) || min < 0) {
          return res.status(400).json({ message: "min_occ debe ser un número >= 0" });
        }
        queryPts = queryPts.replace(
          "{min_occ}",
          ` HAVING array_length(array_agg(st_astext(the_geom)),1) > ${Math.floor(min)} `
        );
      } else if (filter_param === "in_fosil") {
        queryPts = queryPts.replace("{in_fosil}", filter_value ? " " : " AND ejemplarfosil = 'NO' ");
      } else if (filter_param === "in_sin_fecha") {
        queryPts = queryPts.replace("{in_sin_fecha}", filter_value ? " " : " AND fechacolecta IS NOT NULL ");
      } else {
        return res.status(400).json({ message: `Filtro no válido: ${filter_param}` });
      }
    }

    queryPts = queryPts
      .replace("{min_occ}", "")
      .replace("{in_fosil}", "")
      .replace("{in_sin_fecha}", "");

    const snibParams = {
      dic_taxon_data: dic_taxon_data.get(column_taxon),
      dic_taxon_group: dic_taxon_group.get(column_taxon),
    };

    // Split spids into chunks and fetch per-species rows in waves
    const spidChunks = [];
    for (let i = 0; i < pendingLevelsId.length; i += SPID_BATCH) {
      spidChunks.push(pendingLevelsId.slice(i, i + SPID_BATCH));
    }

    const datapoints = [];
    for (let i = 0; i < spidChunks.length; i += WAVE_SIZE) {
      const wave = spidChunks.slice(i, i + WAVE_SIZE);
      const waveRows = await Promise.all(
        wave.map(batch =>
          pool.any(queryPts, { spids: batch, ...snibParams })
            .catch(err => { debug('snib batch:', err.message); return []; })
        )
      );
      datapoints.push(...waveRows.flat());
    }

    const speciesPoints = [];
    for (const points_byspid of datapoints) {
      if (!points_byspid.points || points_byspid.points.length === 0) continue;

      const uniquePts = [...new Set(points_byspid.points)];
      const pts = uniquePts.length > MAX_PTS_PER_SPID
        ? uniquePts.sort(() => Math.random() - 0.5).slice(0, MAX_PTS_PER_SPID)
        : uniquePts;

      speciesPoints.push({ spid: points_byspid.spid, datos: points_byspid.datos, pts });
    }

    // Agrupa varias especies por consulta para reducir el número de
    // round-trips a la BD (antes: 1 query por especie, ~2268 para una clase
    // como Mammalia). El límite es por TOTAL de puntos embebidos en la
    // consulta, no por número de especies, para que una especie con muchos
    // registros no genere un batch desproporcionadamente grande.
    const MAX_PTS_PER_QUERY = 20000;

    const speciesBatches = [];
    let currentBatch = [];
    let currentBatchPts = 0;
    for (const sp of speciesPoints) {
      if (currentBatch.length > 0 && currentBatchPts + sp.pts.length > MAX_PTS_PER_QUERY) {
        speciesBatches.push(currentBatch);
        currentBatch = [];
        currentBatchPts = 0;
      }
      currentBatch.push(sp);
      currentBatchPts += sp.pts.length;
    }
    if (currentBatch.length > 0) speciesBatches.push(currentBatch);

    const buildBatchQuery = (batch) => {
      const valuesRows = [];
      for (const sp of batch) {
        for (const wkt of sp.pts) {
          valuesRows.push(`(${sp.spid}, ST_SetSRID(ST_GeomFromText('${wkt}'), 4326))`);
        }
      }
      return `
        WITH puntos(spid, geom) AS (
          VALUES ${valuesRows.join(", ")}
        )
        SELECT DISTINCT spid, ${res_column} AS cell
        FROM puntos p
        JOIN ${table_cell_name} g
          ON ST_Intersects(g.the_geom, p.geom)
        ORDER BY spid, cell;
      `;
    };

    const GRID_WAVE = 25; // debe ser <= db_mallas.poolSize (config.js)
    const cellsBySpid = new Map();
    if (speciesBatches.length > 0) {
      for (let i = 0; i < speciesBatches.length; i += GRID_WAVE) {
        const wave = speciesBatches.slice(i, i + GRID_WAVE);
        const waveRows = await Promise.all(
          wave.map((batch) =>
            pool_mallas.any(buildBatchQuery(batch), {}).catch((err) => { debug(err); return []; })
          )
        );
        for (const rows of waveRows) {
          for (const { spid, cell } of rows) {
            if (!cellsBySpid.has(spid)) cellsBySpid.set(spid, []);
            cellsBySpid.get(spid).push(cell);
          }
        }
      }

      // Cache write-back: lo que se acaba de calcular en vivo se guarda en
      // sp_snib para que la proxima consulta de esta misma especie/resolucion
      // ya no necesite tocar snib/pool_mallas. No bloquea la respuesta al
      // cliente (fire-and-forget) -- si falla, simplemente se vuelve a
      // calcular en vivo la proxima vez, sin romper nada.
      if (canUseCache && cellsBySpid.size > 0) {
        const writes = [];
        for (const [spid, cells] of cellsBySpid.entries()) {
          writes.push(
            pool.none(
              `UPDATE sp_snib SET ${cellsColumn} = $<cells> WHERE spid = $<spid>`,
              { cells, spid }
            ).catch((err) => debug('cache write-back:', err.message))
          );
        }
        Promise.all(writes).catch(() => {});
      }
    }

    // Combina lo cacheado (sp_snib) con lo recien calculado en vivo,
    // preservando el orden de entrada de levels_id.
    const cachedBySpid = new Map(cachedRows.map((r) => [r.spid, r]));
    const speciesPointsBySpid = new Map(speciesPoints.map((sp) => [sp.spid, sp]));

    const response_array = [];
    for (const spid of levels_id) {
      if (cachedBySpid.has(spid)) {
        const row = cachedBySpid.get(spid);
        const cells = row.cells || [];
        response_array.push({ id: variable_id, grid_id, level_id: spid, metadata: row.datos, cells, n: cells.length });
      } else if (speciesPointsBySpid.has(spid)) {
        const sp = speciesPointsBySpid.get(spid);
        const cells = cellsBySpid.get(spid) || [];
        response_array.push({ id: variable_id, grid_id, level_id: spid, metadata: sp.datos, cells, n: cells.length });
      }
    }

    return res.status(200).json(response_array);
  } catch (error) {
    debug(error);
    return res.status(500).json({
      message: "Error interno al obtener datos"
    });
  }
};

