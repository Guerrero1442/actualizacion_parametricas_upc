# Coneccion base de datos
from datetime import datetime
import re
from dateutil.relativedelta import relativedelta
import oracledb
import configparser
import pandas as pd
import logging
import sys
from pathlib import Path
from sqlalchemy import create_engine, exc, inspect, text
import yaml
from scripts.limpieza_archivo import sacar_longitudes_max_columnas

# Constantes
CONFIG_PATH = 'config/database.yaml'

def leer_configuracion_oracle() -> dict:
    with open(CONFIG_PATH, 'r') as file:
        config = yaml.safe_load(file)
    return config


def conectar_base_oracle():
    config = leer_configuracion_oracle()
    
    host: str = config['oracle']['host']
    port: int = int(config['oracle']['port'])
    sid: str = config['oracle']['service_name']
    user: str = config['oracle']['user']
    password: str = config['oracle']['password']

    dsn: str = oracledb.makedsn(host, port, sid)
    
    try:
        engine = create_engine(f'oracle+oracledb://{user}:{password}@{dsn}', pool_pre_ping=True)
        logging.info('Conexión exitosa con la base de datos')
        return engine
    except Exception as e:
        logging.critical(f'error al conectar con la base de datos {e}')
        sys.exit(1)
        
        
def actualizar_datos_oracle(engine, df: pd.DataFrame, tabla: str) -> None:
    with engine.connect() as connection:
        try:
            with connection.begin(): # Iniciar una transaccion ()
                logging.info(f'Truncando tabla {tabla}')
                connection.execute(text(f"TRUNCATE TABLE {tabla}"))
                
                logging.info(f'Insertando {df.shape[0]} registros en la tabla {tabla}')
                df.to_sql(tabla, 
                          con=engine, 
                          if_exists='append',
                           index=False)
        except Exception as e:
            logging.warning(f'error al truncar la tabla {tabla} {e}')
    logging.info(f'Se actualizaron {df.shape[0]} registros en la tabla {tabla}')
    
def insertar_datos_oracle(engine, tabla: str, df: pd.DataFrame) -> None:
    columnas = df.columns.tolist()
    columnas_sql = ",".join([f'"{col}"' for col in columnas])
    placeholders = ", ".join([f':{i+1}' for i in range(len(columnas))])
    
    sql = f"INSERT INTO {tabla} ({columnas_sql}) VALUES ({placeholders})"
    
    df.fillna('', inplace=True)
    
    tuples_values = [tuple(row) for row in df.values]
    
    with engine.connect() as conn:
        cur = conn.connection.cursor()
        
        try:
            chunksize = 100000
            for i in range(0, len(tuples_values), chunksize):
                cur.executemany(sql, tuples_values[i:i+chunksize])
                conn.connection.commit()
            logging.info(f'Se insertaron {len(tuples_values)} registros en la tabla {tabla}')  
        except Exception as e:
            logging.warning(f'Error al insertar los datos en la tabla {tabla} {e}')       
            
def es_nombre_tabla_valido(nombre_tabla: str, max_longitud: int = 128) -> bool:
    """
    Verifica si el nombre de la tabla es válido según las reglas de Oracle.
    :param nombre_tabla: Nombre de la tabla a verificar.
    :param max_longitud: Longitud máxima permitida para el nombre de la tabla.
    :return: True si el nombre es válido, False en caso contrario.
    """
    if not nombre_tabla:
        logging.warning("El nombre de la tabla no puede estar vacío.")
        return False
    if len(nombre_tabla) > max_longitud:
        logging.warning(f"El nombre de la tabla no puede exceder {max_longitud} caracteres.")
        return False

    # Patrón: Empieza con letra, seguido de letras, números o guion bajo.
    # ^[a-zA-Z] asegura que empieza con una letra.
    # [a-zA-Z0-9_]*$ permite cero o más letras, números o guiones bajos hasta el final.
    if not re.fullmatch(r"^[a-zA-Z][a-zA-Z0-9_]*$", nombre_tabla):
        logging.warning(f"Nombre de tabla '{nombre_tabla}' contiene caracteres inválidos o formato incorrecto.")
        return False
    
    # Lista (parcial) de palabras reservadas comunes de SQL y Oracle.
    # Deberías expandir esta lista o usar una más exhaustiva.
    PALABRAS_RESERVADAS = {
        "SELECT", "UPDATE", "DELETE", "INSERT", "CREATE", "TABLE", "VIEW", "INDEX",
        "DROP", "ALTER", "FROM", "WHERE", "GROUP", "ORDER", "BY", "DATABASE",
        "DEFAULT", "USER", "PASSWORD", "GRANT", "ACCESS", "AUDIT", "CLUSTER",
        "COLUMN", "COMMENT", "COMPRESS", "CONNECT", "DATE", "EXCLUSIVE", "FILE",
        "IDENTIFIED", "IMMEDIATE", "INCREMENT", "INITIAL", "INTERSECT", "LEVEL",
        "LIKE", "LOCK", "LONG", "MAXEXTENTS", "MINUS", "MODE", "MODIFY", "NOAUDIT",
        "NOCOMPRESS", "NOWAIT", "NULL", "NUMBER", "OFFLINE", "ONLINE", "OPTION",
        "PCTFREE", "PRIOR", "PRIVILEGES", "PUBLIC", "RAW", "RENAME", "RESOURCE",
        "REVOKE", "ROW", "ROWNUM", "ROWS", "SESSION", "SHARE", "SIZE", "SMALLINT",
        "START", "SUCCESSFUL", "SYNONYM", "SYSDATE", "THEN", "TO", "TRIGGER",
        "UID", "UNION", "UNIQUE", "VALIDATE", "VALUES", "VARCHAR", "VARCHAR2", "WITH"
    }
    
    if nombre_tabla.upper() in PALABRAS_RESERVADAS:
        logging.warning(f"El nombre de la tabla '{nombre_tabla}' es una palabra reservada.")
        return False
    
    return True


def crear_tabla_longitudes(engine, nombre_tabla: str, df, periodo: str | None = None) -> None:

    longitud_columnas = sacar_longitudes_max_columnas(df)

    if periodo:
        nombre_tabla = f"{nombre_tabla.lower()}_{periodo}"

    if not es_nombre_tabla_valido(nombre_tabla):
        logging.warning(f'El nombre de la tabla {nombre_tabla} no es válido. No se creará la tabla.')

    columnas_definicion = []
    for columna, longitud in longitud_columnas.items():
        columnas_definicion.append(f'"{columna}" VARCHAR2({longitud} CHAR)')

    columnas_definicion_str = ",".join(columnas_definicion)
    sentencia_create_sql = f"CREATE TABLE {nombre_tabla} ({columnas_definicion_str})"
    
    
    try:
        with engine.connect() as connection:
            inspector = inspect(connection)
            if inspector.has_table(nombre_tabla):
                logging.info(f'Tabla {nombre_tabla} existe. Eliminándola...')
                try:
                    connection.execute(text(f"DROP TABLE {nombre_tabla}")) 
                    logging.info(f'Tabla {nombre_tabla} eliminada.')
                except Exception as e_drop:
                    logging.warning(f'Error al eliminar la tabla {nombre_tabla}: {e_drop}')
            else:
                logging.info(f'La tabla {nombre_tabla} no existe. No se requiere eliminación.')


            # --- Creación de la tabla ---
            logging.info(f"Creando tabla {nombre_tabla} con sentencia: {sentencia_create_sql}")
            try:
                connection.execute(text(sentencia_create_sql))
                logging.info(f'Tabla {nombre_tabla} creada exitosamente.')
            except Exception as e_create: # Ser más específico con la excepción si es posible
                logging.error(f'Error al crear la tabla {nombre_tabla}: {e_create}')

            
    except Exception as e_conn: # Error de conexión o un error no capturado arriba
        logging.error(f'Error general durante la operación de tabla {nombre_tabla}: {e_conn}')

    # insertando datos a la tabla creada
    insertar_datos_oracle(engine, nombre_tabla, df)

    
def obtener_datos_oracle(engine, tabla: str) -> pd.DataFrame:
    with engine.connect() as connection:
        try:
            df = pd.read_sql(f"SELECT * FROM {tabla}", connection)
            logging.info(f'Se obtuvieron {df.shape[0]} registros de la tabla {tabla}')
            return df
        except Exception as e:
            logging.warning(f'Error al obtener los datos de la tabla {tabla} {e}')
            return pd.DataFrame()