# ******************* INICIO DE EJECUCIÓN DE SCRIPTS PARA AGREGAR MAS OCURRENCIAS ******************* 
# Insertando ocurrencias, si ya existen agrega sobre las existentes
chunk_size = 5000
try:
  columns = get_sql(columns_file).splitlines()
  # os.chdir(data_folder)

  # engine = create_engine('postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(DBNICHEUSER, DBNICHEPASSWD, DBNICHEHOST, DBNICHEPORT, DBNICHENAME))
  engine = psycopg2.connect( database=DBNICHENAME, user=DBNICHEUSER, password=DBNICHEPASSWD, host=DBNICHEHOST, port=DBNICHEPORT)
  cursor = engine.cursor()
    
  logger.info('Cargando datos de ocurrencias')

  # Configurar conexión SSH con el servidor remoto
  ssh_client = paramiko.SSHClient()
  ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

  remote_host = SERVERJUANHOST
  remote_user = SERVERJUANUSER
  remote_password = SERVERJUANPASS
  remote_file_path = ruta_archivo

  logger.info('       --> {0}'.format(ruta_archivo))

  ssh_client.connect(hostname=remote_host, username=remote_user, password=remote_password)

  # Abrir el archivo remoto como un flujo de datos
  sftp = ssh_client.open_sftp()
  remote_file = sftp.file(remote_file_path, mode='r')

  columns_to_cast = ['\"latitud\"', '\"longitud\"', '\"altitudmapa\"', '\"incertidumbreXY\"'] 

  # for chunk in pd.read_csv(ruta_archivo, chunksize=chunk_size, sep=',', names=columns):
  for chunk in pd.read_csv(remote_file, chunksize=chunk_size, sep=',', names=columns, iterator=True, low_memory=False):

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
    try:
        extras.execute_values(cursor, insert_query, data_tuples)
        engine.commit()
        print(f'{len(chunk)} filas insertadas en la base de datos.')

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
  ssh_client.close()

except Exception as e:
  logger.error('No se pudieron agregar todas las ocurrencias: {0}'.format(str(e)))
  sys.exit()
  cursor.close()