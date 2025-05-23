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
            logging.info(f'Truncando tabla {tabla}')
            connection.execute(text(f"TRUNCATE TABLE {tabla}"))
        except Exception as e:
            logging.warning(f'error al truncar la tabla {tabla} {e}')
        df.to_sql(tabla, con=engine, if_exists='append', index=False)
        connection.close()
    logging.info(f'Se actualizaron {df.shape[0]} registros en la tabla {tabla}')


def creacion_tabla_actualizada(engine, df: pd.DataFrame, tabla: str, periodo: str) -> None:
    periodo_anterior = datetime.strptime(periodo, '%Y%m')
    periodo_anterior = periodo_anterior - relativedelta(months=1)
    periodo_anterior = periodo_anterior.strftime('%Y%m')
    nombre_tabla = tabla.lower() + '_' + periodo

    with engine.connect() as connection:
        inspector = inspect(connection)
        
        # Eliminar tabla si existe
        try:
            if inspector.has_table(nombre_tabla):
                logging.info(f'La tabla {nombre_tabla} existe. Eliminándola...')
                connection.execute(text(f"DROP TABLE {nombre_tabla}"))
            else:
                logging.info(f'La tabla {nombre_tabla} no existe. No se requiere eliminación.')
        except Exception as e:
            logging.warning(f'error al verificar o eliminar la tabla {nombre_tabla} {e}')

        # Crear copia de la tabla del periodo anterior
        try:
            logging.info(f'Creando tabla {nombre_tabla}')
            connection.execute(text(f"""          
                CREATE TABLE {nombre_tabla} AS
                SELECT * FROM {tabla.lower()}_{periodo_anterior} WHERE 1=0
            """))
        except Exception as e:
            logging.warning(f'error al crear la tabla {nombre_tabla} {e}')
            
        
    insertar_datos_oracle(engine, nombre_tabla, df)
    
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


def crear_tabla_longitudes(engine, nombre_tabla: str, longitud_columnas) -> None:

    if not es_nombre_tabla_valido(nombre_tabla):
        logging.warning(f'El nombre de la tabla {nombre_tabla} no es válido. No se creará la tabla.')
        return

    columnas_definicion = []
    for columna, longitud in longitud_columnas.items():
        columnas_definicion.append(f'"{columna}" VARCHAR2({longitud} CHAR)')

    columnas_definicion_str = ",".join(columnas_definicion)
    sentencia_create_sql = f"CREATE TABLE {nombre_tabla} ({columnas_definicion_str})"
    
    
    try:
        with engine.connect() as connection:
            # --- Manejo de DROP TABLE (opcional) ---
            inspector = inspect(connection) # O inspect(engine)
            if inspector.has_table(nombre_tabla): # (Para SQLAlchemy 1.4+)
                logging.info(f'Tabla {nombre_tabla} existe. Eliminándola...')
                try:
                    # En Oracle <23c, no hay "IF EXISTS" directo en DROP TABLE.
                    # Una alternativa es simplemente ejecutar DROP y capturar error si no existe.
                    connection.execute(text(f"DROP TABLE {nombre_tabla}")) 
                    # DDL usualmente auto-commitea en Oracle, pero si estás en una transacción explícita
                    # (connection.begin()), necesitarías connection.commit() aquí para el DROP.
                    # Sin embargo, para DDL simple, el autocommit de Oracle suele ser suficiente.
                    logging.info(f'Tabla {nombre_tabla} eliminada.')
                except Exception as e_drop: # Ser más específico con la excepción si es posible
                    logging.warning(f'Error al eliminar la tabla {nombre_tabla}: {e_drop}')
            else:
                logging.info(f'La tabla {nombre_tabla} no existe. No se requiere eliminación.')

            # --- Creación de la tabla ---
            logging.info(f"Creando tabla {nombre_tabla} con sentencia: {sentencia_create_sql}")
            try:
                connection.execute(text(sentencia_create_sql))
                # Como antes, DDL suele auto-commitear. Si iniciaste transacción con connection.begin(), necesitarás:
                # connection.commit() 
                logging.info(f'Tabla {nombre_tabla} creada exitosamente.')
            except Exception as e_create: # Ser más específico con la excepción si es posible
                logging.error(f'Error al crear la tabla {nombre_tabla}: {e_create}')
                # Podrías querer relanzar la excepción aquí si es un error crítico: raise

    except Exception as e_conn: # Error de conexión o un error no capturado arriba
        logging.error(f'Error general durante la operación de tabla {nombre_tabla}: {e_conn}')

    
def obtener_datos_oracle(engine, tabla: str) -> pd.DataFrame:
    with engine.connect() as connection:
        try:
            df = pd.read_sql(f"SELECT * FROM {tabla}", connection)
            logging.info(f'Se obtuvieron {df.shape[0]} registros de la tabla {tabla}')
            return df
        except Exception as e:
            logging.warning(f'Error al obtener los datos de la tabla {tabla} {e}')
            return pd.DataFrame()