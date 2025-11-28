var debug = require('debug')('verbs:auth')
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

exports.variables = function(req, res) {

	let { } = req.body;

	// Se recomienda agregar la columna available_grids a este catalogo con ayuda de los servicios disponibles del proyecto regionmiddleware
	pool.any(`SELECT id, taxon as variable, level_size, filter_fields, available_grids
			FROM cat_taxon order by id;`, {}).then( 
		function(data) {
			// debug(data);
		res.status(200).json({
			data: data
		})
  	})
  	.catch(error => {
      debug(error)
      res.status(403).json({
      	message: "error al obtener catalogo", 
      	error: error
      })
   	});
}


exports.secuencia = function(req, res) {

	let { 
		variableLevel,
		variableValue,
		nextVariableLevel
	} = req.body;

	console.log("variableLevel: " + dic_taxon_db.get(variableLevel))
	console.log("variableValue: " + variableValue)
	console.log("nextVariableLevel: " + dic_taxon_db.get(nextVariableLevel))

	pool.any(`select distinct '$<nextVariableLevel:raw>' as value,  $<nextVariableLevel:raw> as label  from sp_snib ss 
			where $<nextVariableLevel:raw> <> '' and $<variableLevel:raw> = '$<variableValue:raw>';`, {
				variableLevel: dic_taxon_db.get(variableLevel),
				variableValue: variableValue,
				nextVariableLevel: dic_taxon_db.get(nextVariableLevel)
			}).then( 
		function(data) {
			debug(data);


		res.status(200).json({
			data: data
		})
  	})
  	.catch(error => {
      debug(error)
      res.status(403).json({
      	message: "error al obtener catalogo", 
      	error: error
      })
   	});
}




exports.get_variable_byid = function(req, res) {

	let variable_id = req.params.id
	debug("variable_id: " + variable_id)

	let q = verb_utils.getParam(req, 'q', '')
	let offset = verb_utils.getParam(req, 'offset', 0)
	let limit = verb_utils.getParam(req, 'limit', 10)

	debug("q: " + q)
	debug("offset: " + offset)
	debug("limit: " + limit)

	let query_array = []
	
	let filter_separator = ";"
	let pair_separator = "="
	let group_separator = ","

	// q: "levels_id = 310245,265492; familia = Acanthaceae; especie = Lynx rufus"
	
	if(q != ""){

		let array_queries = q.split(filter_separator)
		debug(array_queries)

		if(array_queries.length == 0){
			debug("Sin filtros definidos")
		}
		else{
			array_queries.forEach((filter, index) => {

				let filter_pair = filter.split(pair_separator)
				debug(filter_pair)

				if(filter_pair.length == 0){
					debug("************ Filtro indefinido")
				}
				else{
					
					let filter_param = filter_pair[0].trim()
					debug("filter_param: " + filter_param)

					// TODO:Revisar por que no jala en enalce del mapa con su llave valor
					// debug(dic_taxon_db.keys())
					// debug("dic_taxon_db: " + dic_taxon_db.get(filter_param))
					

					if(valid_filters.indexOf(filter_param) == -1){
						debug("************  Filtro invalido")
					}
					else{
						
						if(filter_pair.length != 2){
							debug("************ Filtro invalido por composición")
						}
						else{
							let filter_value = filter_pair[1].trim().split(group_separator)	
							
							let query_temp = "( "
							
							// TODO: Esta logica no esta funcionando con el or, al final solo se aplica un and 

							filter_value.forEach((value, index) => {

								value = value.trim();

								debug("value: " + value + " - index: " + index)

								if(filter_param !== "levels_id"){
									value = "'" + value + "%'"
								}
								
								if(index == 0){
									query_temp = query_temp + "lower(" + dic_taxon_db.get(filter_param) + ") like lower(" + value + ") "
								}
								else{
									query_temp = query_temp + " or " + "lower(" + dic_taxon_db.get(filter_param) + ") like lower(" + value + ") "
								}

							})

							query_temp = query_temp + " )"

							debug("query_temp: " + query_temp)

							query_array.push(query_temp)

						}

					}

				}

			})	

			debug(query_array)

		}

	}


	pool.task(t => {

		return t.one(
			"select id, column_taxon from cat_taxon where id = $<variable_id:raw>", {
				variable_id: variable_id
			}	
		).then(resp => {

			let column_taxon = resp.column_taxon
			debug("column_taxon: " + column_taxon)

			let id = resp.id
			debug("id variable: " + id)

			let query = `select $<id:raw> as id, array_agg(spid) as level_id, ('$<dic_taxon_data:raw>')::jsonb as datos
				from sp_snib
				where $<column_taxon:raw> <> '' {queries}
				group by $<dic_taxon_group:raw>
				order by $<column_taxon:raw>
				offset $<offset:raw>
				limit $<limit:raw>`

			query_array.forEach((query_temp, index) => {
				
				query = query.replace("{queries}", " and " + query_temp + " {queries} ")

			})

			query = query.replace("{queries}", "")
			query = query.replace(/levels_id/g, "spid")
			
			debug(query)
				
			return t.any(query, {
					id: id,
					column_taxon:column_taxon,
					dic_taxon_data:dic_taxon_data.get(column_taxon),
					dic_taxon_group: dic_taxon_group.get(column_taxon),
					offset: offset,
					limit: limit
				}	

			).then(resp => {

				res.status(200).json({
					data: resp
				})

			}).catch(error => {
		      debug(error)
		      res.status(403).json({
		      	error: "Error al obtener la malla solicitada", 
		      	message: "error al obtener datos"
		      })
		   	})

		}).catch(error => {
	      debug(error)

	      res.status(403).json({	      	
	      	error: "Error al obtener la malla solicitada", 
	      	message: "error al obtener datos"
	      })
	   	})

	}).catch(error => {
      debug(error)
      res.status(403).json({
      	message: "error general", 
      	error: error
      })
   	});
  	
}


exports.get_data_byid = async function (req, res) {
  try {
    const variable_id = req.params.id;
    debug("variable_id: " + variable_id);

    const grid_id = verb_utils.getParam(req, 'grid_id', 1);
    debug("grid_id: " + grid_id);

    const levels_id = verb_utils.getParam(req, 'levels_id', []); // array de spids
    const filter_names = verb_utils.getParam(req, 'filter_names', []);
    const filter_values = verb_utils.getParam(req, 'filter_values', []);

    debug(filter_names);
    debug(filter_values);

    // Construcción de filtros
    const filter_array = [];
    if (filter_names.length > 0) {
      filter_names.forEach((filter_name, index) => {
        filter_array.push({ filter_param: filter_name, filter_value: filter_values[index] });
      });
    }

    // 1) Obtener column_taxon
    const taxonRow = await pool.task(async (t) => {
      const row = await t.one(
        "SELECT id, column_taxon FROM cat_taxon WHERE id = $<variable_id>",
        { variable_id }
      );
      return row;
    });

    const column_taxon = taxonRow.column_taxon;
    debug("column_taxon: " + column_taxon);
    debug("json: " + dic_taxon_data.get(column_taxon));
    debug("json: " + dic_taxon_group.get(column_taxon));

    // 2) Armar query base de puntos por spid
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

    // Aplicar filtros en la plantilla
    for (const filter_item of filter_array) {
      let filter_query = "";
      switch (filter_item.filter_param) {
        case "min_occ":
          debug("min_occ");
          filter_query = ` HAVING array_length(array_agg(st_astext(the_geom)),1) > ${Number(filter_item.filter_value) || 0} `;
          queryPts = queryPts.replace("{min_occ}", filter_query);
          break;

        case "in_fosil":
          debug("Incluir registros fosil");
          filter_query = filter_item.filter_value ? " " : " AND ejemplarfosil = 'NO' ";
          queryPts = queryPts.replace("{in_fosil}", filter_query);
          break;

        case "in_sin_fecha":
          debug("Incluir registros sin fecha");
          // si es true no filtramos; si es false exigimos fecha
          filter_query = filter_item.filter_value ? " " : " AND fechacolecta IS NOT NULL ";
          queryPts = queryPts.replace("{in_sin_fecha}", filter_query);
          break;

        default:
          console.log("Filtro no válido: " + filter_item.filter_param);
      }
    }

    // Limpieza por si quedaron tokens
    queryPts = queryPts
      .replace("{min_occ}", "")
      .replace("{in_fosil}", "")
      .replace("{in_sin_fecha}", "");

    // 3) Ejecutar query de puntos por spid
    const datapoints = await pool.task(async (t) => {
      const rows = await t.any(queryPts, {
        spids: levels_id, // :csv espera array
        dic_taxon_data: dic_taxon_data.get(column_taxon),
        dic_taxon_group: dic_taxon_group.get(column_taxon),
      });
      return rows;
    });

    if (!datapoints || datapoints.length === 0) {
      const response_array = [{
        id: variable_id,
        grid_id: grid_id,
        cells: [],
        n: 0,
        message: "No hay datos para esta solicitud",
      }];
      return res.status(404).json(response_array);
    }

    // 4) Obtener malla (resolution y table_cell_name) según grid_id
    const gridInfo = await pool_mallas.task(async (t) => {
      const row = await t.one(
        `SELECT * FROM cat_grid cg WHERE grid_id = $<grid_id>`,
        { grid_id }
      );
      return {
        resolution: row.resolution,
        table_cell_name: row.table_cell_name,
      };
    });

    const res_column = "g.gridid_" + gridInfo.resolution;
    const table_cell_name = gridInfo.table_cell_name;

    console.log("res_column: " + res_column);
    console.log("table_cell_name: " + table_cell_name);

    // 5) Preparar queries por spid para obtener celdas intersectadas
    const query_array = [];
    for (const points_byspid of datapoints) {
      if (!points_byspid.points || points_byspid.points.length === 0) {
        debug("el " + points_byspid.spid + " no tiene ocurrencias registradas");
        continue;
      }

      const query_points = points_byspid.points
        .map((wkt) => `ST_SetSRID(ST_GeomFromText('${wkt}'), 4326)`)
        .join(", ");

      let query_temp = `
        WITH puntos AS (
          SELECT ARRAY[{query_points}] AS geom_array
        ),
        point_geom AS (
          SELECT unnest(geom_array) AS geom FROM puntos
        )
        SELECT DISTINCT {res_column} AS cell
        FROM point_geom p
        JOIN {table_cell_name} g
          ON ST_Intersects(g.the_geom, p.geom)
        ORDER BY cell;
      `;

      query_temp = query_temp
        .replace("{query_points}", query_points)
        .replace("{res_column}", res_column)
        .replace("{table_cell_name}", table_cell_name);

      query_array.push({
        query_temp,
        spid: points_byspid.spid,
        datos: points_byspid.datos,
      });
    }

    // 6) Ejecutar todas las queries en paralelo y construir respuesta
    const results = await Promise.all(
      query_array.map(({ query_temp }) =>
        pool_mallas.task((t) => t.any(query_temp, {}))
          .catch((err) => {
            debug(err);
            // Devolvemos arreglo vacío para que ese spid no trabe todo
            return [];
          })
      )
    );

    const response_array = query_array.map((q, idx) => {
      const rows = results[idx] || [];
      const cells = rows.map(r => r.cell);
      return {
        id: variable_id,
        grid_id: grid_id,
        level_id: q.spid,
        metadata: q.datos,
        cells,
        n: cells.length,
      };
    });

    return res.status(200).json(response_array);

  } catch (error) {
    debug(error);
    return res.status(403).json({
      message: "error general",
      error,
    });
  }
};
