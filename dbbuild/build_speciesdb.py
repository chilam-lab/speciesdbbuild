#!/usr/bin/env python3
import os
import time
import sys
import csv
import psycopg2
import psycopg2.extras as extras
import subprocess
import argparse
import glob
from shutil import copyfile
from aux_functions import *
from pathlib import Path
import pandas as pd
# from osgeo import gdal

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv, dotenv_values

create_extensions       = './sql/create_extensions.sql'
create_aoi_table    = './sql/create_aoi_table.sql'
geom_aoi_data       = './sql/geom_aoi.sql'
get_aoi        = './sql/get_aoi.sql'

root_folder                = './'
data_folder                = './data'
stored_procedures_folder   = '../sql/stored_procedures'
stored_validation_folder   = '../stored_validation'
create_snib_table          = './sql/create_snib_table.sql'
create_sp_snib_table       = './sql/create_sp_snib_table.sql'

# columns_file               = './data/columns.txt'
columns_file               = './data/columns_marzo26.txt'

# ruta_archivo               = '/mnt/fastdata/SNIBEjemplares_20241217_103551.csv' 
ruta_archivo               = '../data/SNIBEjemplares_20260310_103629.csv' 

# ruta_archivo               = '../data/SNIBEjemplares_20241217_103551.csv' 
# ruta_archivo               = '../data/test_data.csv' 

create_geoportal_table     = './sql/create_geoportal_table.sql'

logger = setup_logger()
load_dotenv() 

DBNICHENAME=os.getenv("DBNICHENAME")
DBNICHEHOST=os.getenv("DBNICHEHOST")
DBNICHEPORT=os.getenv("DBNICHEPORT")
DBNICHEUSER=os.getenv("DBNICHEUSER")
DBNICHEPASSWD=os.getenv("DBNICHEPASSWD")

# Obteniendo variables de ambiente
try:
    
    logger.info('lectura de USUARIO: {0} en el HOST: {1}, BASE: {2} y PUERTO: {3}'.format(DBNICHEUSER, DBNICHEHOST, DBNICHENAME, DBNICHEPORT))
except Exception as e:
    logger.error('No se pudieron obtener las variables de entorno requeridas : {0}'.format(str(e)))
    sys.exit()


# Creando tabla aoi (area de interes) contempla todos los países con la columna de continente por continente
try:

    logger.info('Instalando extensiones y Creando tabla aoi a nivel mundial')
    create_extensions_sql = get_sql(create_extensions) 
    create_aoi_table_sql = get_sql(create_aoi_table) 
    geom_aoi_data_sql = get_sql(geom_aoi_data) 

    conn = psycopg2.connect('dbname={0} host={1} port={2} user={3} password={4}'.format(DBNICHENAME, DBNICHEHOST, DBNICHEPORT, DBNICHEUSER, DBNICHEPASSWD))
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    cur.execute(create_extensions_sql)
    logger.info('create_extensions_sql')

    # cur.execute(create_aoi_table_sql)
    # logger.info('create_aoi_table_sql')

    # cur.execute(geom_aoi_data_sql)
    # logger.info('geom_aoi_data_sql')

    # # Creando tabla geoportal
    # create_geoportal_table_sql = get_sql(create_geoportal_table)
    # cur.execute(create_geoportal_table_sql)
    # logger.info('Tabla geoportal creada')

    cur.close()
    conn.close()

except Exception as e:
    logger.error('No se pudo instalar las extensiones necesarias o crear: {0}'.format(str(e)))
    sys.exit()


# ******************* INICIO DE EJECUCIÓN DE SCRIPTS PARA AGREGAR MAS OCURRENCIAS ******************* 
# Insertando ocurrencias, si ya existen agrega sobre las existentes
chunk_size = 5000
engine = None
cursor = None

try:
  
  # columns = get_sql(columns_file).splitlines()
  columns = [c.strip().strip('"') for c in get_sql(columns_file).splitlines() if c.strip()]

  os.chdir(data_folder)

  progress_file = 'load_progress.txt'  # se guarda dentro de ./data porque haces os.chdir(data_folder)
  start_chunk = int(os.getenv("START_CHUNK", "0"))

  if os.path.exists(progress_file):
        with open(progress_file, 'r') as pf:
            saved = pf.read().strip()
            if saved.isdigit():
                start_chunk = max(start_chunk, int(saved))
  
  engine = psycopg2.connect( database=DBNICHENAME, user=DBNICHEUSER, password=DBNICHEPASSWD, host=DBNICHEHOST, port=DBNICHEPORT)
  cursor = engine.cursor()
    
  logger.info('Cargando datos de ocurrencias')
  logger.info('       --> {0}'.format(ruta_archivo))
  
  # descarta columnas nuevas de versiones recientes de archivos de descarga que no existen en la tabla
  omit_cols = {"cuencahidrograficamapa", "ecorregionterrestremapa"}
  columns = [c for c in columns if c not in omit_cols]
  
  # columns_to_cast = ['\"latitud\"', '\"longitud\"', '\"altitudmapa\"', '\"incertidumbreXY\"'] 
  columns_to_cast = ['latitud', 'longitud', 'altitudmapa', 'incertidumbreXY']
  
  for i, chunk in enumerate(pd.read_csv(
    ruta_archivo,
    chunksize=chunk_size,
    sep=',',
    header=0,
    usecols=columns,
    dtype=str,
    low_memory=False
)):
    if i < start_chunk:
        continue

    for column in columns_to_cast:
        if column in chunk.columns:
            chunk[column] = pd.to_numeric(chunk[column], errors='coerce')

    # filtra ids ya existentes para no reintentar inserts innecesarios
    if "idejemplar" in chunk.columns:
        ids = chunk["idejemplar"].dropna().tolist()
        if ids:
            cursor.execute(
                'SELECT "idejemplar" FROM informaciongeoportal WHERE "idejemplar" = ANY(%s)',
                (ids,)
            )
            existing_ids = {r[0] for r in cursor.fetchall()}
            if existing_ids:
                chunk = chunk[~chunk["idejemplar"].isin(existing_ids)]

    if chunk.empty:
        logger.info(f'Chunk {i}: 0 filas nuevas, se omite.')
        with open(progress_file, 'w') as pf:
            pf.write(str(i + 1))
        continue

    chunk = chunk.astype(object).where(pd.notnull(chunk), None)
    data_tuples = [tuple(row) for row in chunk.to_records(index=False)]

    cols = ','.join(f'"{c}"' for c in columns)
    insert_query = (
        "INSERT INTO informaciongeoportal (" + cols + ") VALUES %s "
        "ON CONFLICT ON CONSTRAINT informaciongeoportal_pkey DO NOTHING"
    )

    logger.info(f'Chunk {i}: {len(chunk)} filas a insertar')
    try:
        extras.execute_values(cursor, insert_query, data_tuples)
        engine.commit()
        print(f'Chunk {i}: {len(chunk)} filas insertadas en la base de datos.')
        with open(progress_file, 'w') as pf:
            pf.write(str(i + 1))
    except Exception as err:
        engine.rollback()
        err_type, err_obj, traceback = sys.exc_info()
        line_num = traceback.tb_lineno
        print("\npsycopg2 ERROR:", err, "on line number:", line_num)
        print("psycopg2 traceback:", traceback, "-- type:", err_type)
        print(err_obj)
        raise

except Exception as e:
  logger.error('No se pudieron agregar todas las ocurrencias: {0}'.format(str(e)))
  sys.exit()
finally:
    if cursor:
        cursor.close()
    if engine:
        engine.close()
  


# # Insertando procedimientos almacenados
# try:
#   conn = psycopg2.connect('dbname={0} host={1} port={2} user={3} password={4}'.format(DBNICHENAME, DBNICHEHOST, DBNICHEPORT, DBNICHEUSER, DBNICHEPASSWD))
#   conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

#   cur = conn.cursor()

#   # os.chdir(root_folder)
#   # if os.path.exists("../sql/stored_procedures"):
#   #   print("existe")
#   # else:
#   #   print("no existe")

#   os.chdir(stored_procedures_folder)
#   for file in glob.glob('*.sql'):
#       print("file: {}".format(file))
#       with open(file, 'r') as f:
#           cur.execute(f.read())
    
#   os.chdir(stored_validation_folder)

#   for file in glob.glob('*.sql'):
#       print("file: {}".format(file))
#       with open(file, 'r') as f:
#           cur.execute(f.read())
    
#   os.chdir('../..')
#   cur.close()
#   conn.close()

#   logger.info('Procedimientos almacenados insertados')
# except Exception as e:
#   logger.error('No se insertaron todos los procedimientos almacenados: {0}'.format(str(e)))
#   sys.exit()



# # Construyendo variables bioticas
# try:
#     conn = psycopg2.connect('dbname={0} host={1} port={2} user={3} password={4}'.format(DBNICHENAME, DBNICHEHOST, DBNICHEPORT, DBNICHEUSER, DBNICHEPASSWD))
#     conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

#     cur = conn.cursor()
#     logger.info('Creación de tablas')

#     create_snib_table_sql = get_sql(create_snib_table)
#     create_sp_snib_table_sql = get_sql(create_sp_snib_table)
#     # create_geo_snib_table_sql = get_sql(create_geo_snib_table)

#     logger.info('Creando tabla snib')
#     cur.execute(create_snib_table_sql)

#     logger.info('Creando tabla sp_snib')
#     cur.execute(create_sp_snib_table_sql)

#     # logger.info('Creando tabla geo_snib')
#     # cur.execute(create_geo_snib_table_sql)

#     cur.close()
#     conn.close()
#     logger.info('Se crearon las variables bioticas correctamente')
            
# except Exception as err:
    
#     logger.error('No se crearon correctamente las variables bioticas: {0}'.format(str(err)))
#     err_type, err_obj, traceback = sys.exc_info()
#     line_num = traceback.tb_lineno
#     print ("\nERROR:", err, "on line number:", line_num)
#     print ("traceback:", traceback, "-- type:", err_type)
#     sys.exit()


# # Construyendo tabla catalogo cat_taxon
# try:
#     conn = psycopg2.connect('dbname={0} host={1} port={2} user={3} password={4}'.format(DBNICHENAME, DBNICHEHOST, DBNICHEPORT, DBNICHEUSER, DBNICHEPASSWD))
#     conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

#     cur = conn.cursor()
#     logger.info('Creación de tabla catalogo')

    

#     create_snib_table_sql = get_sql(create_snib_table)
#     create_sp_snib_table_sql = get_sql(create_sp_snib_table)
#     # create_geo_snib_table_sql = get_sql(create_geo_snib_table)

#     logger.info('Creando tabla snib')
#     cur.execute(create_snib_table_sql)

#     logger.info('Creando tabla sp_snib')
#     cur.execute(create_sp_snib_table_sql)

#     # logger.info('Creando tabla geo_snib')
#     # cur.execute(create_geo_snib_table_sql)

#     cur.close()
#     conn.close()
#     logger.info('Se crearon las variables bioticas correctamente')
            
# except Exception as err:
    
#     logger.error('No se crearon correctamente las variables bioticas: {0}'.format(str(err)))
#     err_type, err_obj, traceback = sys.exc_info()
#     line_num = traceback.tb_lineno
#     print ("\nERROR:", err, "on line number:", line_num)
#     print ("traceback:", traceback, "-- type:", err_type)
#     sys.exit()



# # Creando tabla de metadatos de fuente de datos (SNIB)
# try:
#     conn = psycopg2.connect(
#         'dbname={0} host={1} port={2} user={3} password={4}'.format(
#             DBNICHENAME, DBNICHEHOST, DBNICHEPORT, DBNICHEUSER, DBNICHEPASSWD
#         )
#     )
#     conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
#     cur = conn.cursor()

#     create_data_source_table_sql = """
#     CREATE TABLE IF NOT EXISTS data_source_info (
#         id serial PRIMARY KEY,
#         name varchar(200) NOT NULL UNIQUE,
#         description text,
#         source_url text,
#         download_url text,
#         dict_url text,
#         created_at timestamp without time zone DEFAULT now(),
#         updated_at timestamp without time zone DEFAULT now()
#     );
#     """

#     upsert_data_source_sql = """
#     INSERT INTO data_source_info (name, description, source_url, download_url, dict_url)
#     VALUES (%s, %s, %s, %s, %s)
#     ON CONFLICT (name) DO UPDATE SET
#         description = EXCLUDED.description,
#         source_url = EXCLUDED.source_url,
#         download_url = EXCLUDED.download_url,
#         dict_url = EXCLUDED.dict_url,
#         updated_at = now();
#     """

#     cur.execute(create_data_source_table_sql)
#     cur.execute(
#         upsert_data_source_sql,
#         (
#             "SNIB Data Source",
#             "Esta fuente de datos contiene la información de los datos del Sistema Nacional de Información sobre Biodiversidad de México",
#             "https://www.snib.mx/",
#             "https://www.snib.mx/ejemplares/descarga/",
#             "https://www.snib.mx/ejemplares/docs/CONABIO-SNIB-DiccionarioDatosEstandar202412.pdf",
#         )
#     )

#     logger.info('Tabla data_source_info creada/actualizada correctamente')

#     cur.close()
#     conn.close()

# except Exception as e:
#     logger.error('No se pudo crear/actualizar data_source_info: {0}'.format(str(e)))
#     sys.exit()



