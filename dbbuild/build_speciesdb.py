#!/usr/bin/env python
import os
import time
import sys
import csv
import psycopg2
import psycopg2.extras as extras
import subprocess
import argparse
import glob
from osgeo import gdal
from shutil import copyfile
from aux_functions import *
from pathlib import Path
import pandas as pd

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
columns_file               = './data/columns.txt'
ruta_archivo               = '/mnt/fastdata/SNIBEjemplares_20241217_103551.csv' 
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

# os.chdir(data_folder)


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
    # create_aoi_table_sql = get_sql(create_aoi_table) 
    # geom_aoi_data_sql = get_sql(geom_aoi_data) 

    conn = psycopg2.connect('dbname={0} host={1} port={2} user={3} password={4}'.format(DBNICHENAME, DBNICHEHOST, DBNICHEPORT, DBNICHEUSER, DBNICHEPASSWD))
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    cur.execute(create_extensions_sql)
    logger.info('create_extensions_sql')

    # Creando tabla geoportal
    create_geoportal_table_sql = get_sql(create_geoportal_table)
    cur.execute(create_geoportal_table_sql)
    logger.info('Tabla geoportal creada')

    cur.close()
    conn.close()

except Exception as e:
    logger.error('No se pudo instalar las extensiones necesarias o crear: {0}'.format(str(e)))
    sys.exit()


# ******************* INICIO DE EJECUCIÓN DE SCRIPTS PARA AGREGAR MAS OCURRENCIAS ******************* 
# Insertando ocurrencias, si ya existen agrega sobre las existentes
chunk_size = 5000
try:
  columns = get_sql(columns_file).splitlines()

  os.chdir(data_folder)
  # engine = create_engine('postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(DBNICHEUSER, DBNICHEPASSWD, DBNICHEHOST, DBNICHEPORT, DBNICHENAME))
  engine = psycopg2.connect( database=DBNICHENAME, user=DBNICHEUSER, password=DBNICHEPASSWD, host=DBNICHEHOST, port=DBNICHEPORT)
  cursor = engine.cursor()
    
  logger.info('Cargando datos de ocurrencias')

  # for file in glob.glob('*.csv'):
  # with open(ruta_archivo, mode='r', newline='', encoding='utf-8') as archivo:
  #   file = csv.reader(archivo)
  logger.info('       --> {0}'.format(ruta_archivo))

  columns_to_cast = ['\"latitud\"', '\"longitud\"', '\"altitudmapa\"', '\"incertidumbreXY\"'] 

  for chunk in pd.read_csv(ruta_archivo, chunksize=chunk_size, sep=',', names=columns):

    for column in columns_to_cast:
        chunk[column] = pd.to_numeric(chunk[column], errors='coerce')  # 'coerce' convierte los errores a NaN

    chunk = chunk.astype(object).where(pd.notnull(chunk), None)

    data_tuples = [tuple(row) for row in chunk.to_records(index=False)]
    cols = ','.join(columns)


    # Consulta SQL de inserción para la tabla (ajustar según la estructura de tu tabla)
    insert_query = "INSERT INTO informaciongeoportal ( " + cols + ") VALUES %s ON CONFLICT ON CONSTRAINT informaciongeoportal_pkey DO NOTHING"

    # logger.info('{0}'.format(insert_query))
    logger.info('{0}'.format(data_tuples[0]))

    # Insertar los datos en la base de datos usando executemany para múltiples registros
    # cursor.executemany(insert_query, data_tuples)

    try:
        extras.execute_values(cursor, insert_query, data_tuples)
        engine.commit()
        # print("the dataframe is inserted")
        print(f'{len(chunk)} filas insertadas en la base de datos.')

    # except (Exception, psycopg2.DatabaseError) as error:
    except Exception as err:
        # print_psycopg2_exception(err)
        err_type, err_obj, traceback = sys.exc_info()
        line_num = traceback.tb_lineno
        print ("\npsycopg2 ERROR:", err, "on line number:", line_num)
        print ("psycopg2 traceback:", traceback, "-- type:", err_type)
        print (err_obj)
        # print ("pgcode:", err.pgcode, "\n")
        engine.rollback()
        # cursor.close()
  
  cursor.close()
  conn.close()

except Exception as e:
  logger.error('No se pudieron agregar todas las ocurrencias: {0}'.format(str(e)))
  sys.exit()
  cursor.close()



# Insertando procedimientos almacenados
try:
  conn = psycopg2.connect('dbname={0} host={1} port={2} user={3} password={4}'.format(DBNICHENAME, DBNICHEHOST, DBNICHEPORT, DBNICHEUSER, DBNICHEPASSWD))
  conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

  cur = conn.cursor()

  # os.chdir(root_folder)
  # if os.path.exists("../sql/stored_procedures"):
  #   print("existe")
  # else:
  #   print("no existe")

  os.chdir(stored_procedures_folder)
  for file in glob.glob('*.sql'):
      print("file: {}".format(file))
      with open(file, 'r') as f:
          cur.execute(f.read())
    
  os.chdir(stored_validation_folder)

  for file in glob.glob('*.sql'):
      print("file: {}".format(file))
      with open(file, 'r') as f:
          cur.execute(f.read())
    
  os.chdir('../..')
  cur.close()
  conn.close()

  logger.info('Procedimientos almacenados insertados')
except Exception as e:
  logger.error('No se insertaron todos los procedimientos almacenados: {0}'.format(str(e)))
  sys.exit()



# Construyendo variables bioticas
try:
    conn = psycopg2.connect('dbname={0} host={1} port={2} user={3} password={4}'.format(DBNICHENAME, DBNICHEHOST, DBNICHEPORT, DBNICHEUSER, DBNICHEPASSWD))
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    cur = conn.cursor()
    logger.info('Creación de tablas')

    create_snib_table_sql = get_sql(create_snib_table)
    create_sp_snib_table_sql = get_sql(create_sp_snib_table)
    # create_geo_snib_table_sql = get_sql(create_geo_snib_table)

    logger.info('Creando tabla snib')
    cur.execute(create_snib_table_sql)

    logger.info('Creando tabla sp_snib')
    cur.execute(create_sp_snib_table_sql)

    # logger.info('Creando tabla geo_snib')
    # cur.execute(create_geo_snib_table_sql)

    cur.close()
    conn.close()
    logger.info('Se crearon las variables bioticas correctamente')
            
except Exception as err:
    
    logger.error('No se crearon correctamente las variables bioticas: {0}'.format(str(err)))
    err_type, err_obj, traceback = sys.exc_info()
    line_num = traceback.tb_lineno
    print ("\nERROR:", err, "on line number:", line_num)
    print ("traceback:", traceback, "-- type:", err_type)
    sys.exit()


# Construyendo tabla catalogo cat_taxon
try:
    conn = psycopg2.connect('dbname={0} host={1} port={2} user={3} password={4}'.format(DBNICHENAME, DBNICHEHOST, DBNICHEPORT, DBNICHEUSER, DBNICHEPASSWD))
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    cur = conn.cursor()
    logger.info('Creación de tabla catalogo')

    

    create_snib_table_sql = get_sql(create_snib_table)
    create_sp_snib_table_sql = get_sql(create_sp_snib_table)
    # create_geo_snib_table_sql = get_sql(create_geo_snib_table)

    logger.info('Creando tabla snib')
    cur.execute(create_snib_table_sql)

    logger.info('Creando tabla sp_snib')
    cur.execute(create_sp_snib_table_sql)

    # logger.info('Creando tabla geo_snib')
    # cur.execute(create_geo_snib_table_sql)

    cur.close()
    conn.close()
    logger.info('Se crearon las variables bioticas correctamente')
            
except Exception as err:
    
    logger.error('No se crearon correctamente las variables bioticas: {0}'.format(str(err)))
    err_type, err_obj, traceback = sys.exc_info()
    line_num = traceback.tb_lineno
    print ("\nERROR:", err, "on line number:", line_num)
    print ("traceback:", traceback, "-- type:", err_type)
    sys.exit()



