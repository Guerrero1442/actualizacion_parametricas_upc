# Coneccion base de datos
from sqlalchemy import create_engine
import cx_Oracle
import configparser
import pandas as pd
import logging
import sys

# Constantes
CONFIG_PATH = 'config.ini'


def leer_configuracion_oracle():
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)
    return config


def conectar_base_oracle() -> create_engine:
    config: configparser.ConfigParser = leer_configuracion_oracle()

    host: str = config['oracle']['host']
    port: str = config['oracle']['port']
    sid: str = config['oracle']['service_name']
    user: str = config['oracle']['user']
    password: str = config['oracle']['password']

    dsn: str = cx_Oracle.makedsn(host, port, sid)
    try:
        engine: create_engine = create_engine(
            f'oracle+cx_oracle://{user}:{password}@{dsn}')
        logging.info('Conexión exitosa con la base de datos')
        return engine
    except Exception as e:
        logging.CRITICAL(f'error al conectar con la base de datos {e}')
        sys.exit(1)


def actualizar_datos_oracle(engine: create_engine, df: pd.DataFrame, tabla: str):
    with engine.connect() as connection:
        cursor = connection.connection.cursor()
        try:
            logging.info(f'Truncando tabla {tabla}')
            cursor.execute(f"TRUNCATE TABLE {tabla}")
        except Exception as e:
            logging.warning(f'error al truncar la tabla {tabla} {e}')
        df.to_sql(tabla, con=engine, if_exists='append', index=False)
        cursor.close()
    logging.info(f'Se actualizaron {
                 df.shape[0]} registros en la tabla {tabla}')


def creacion_tabla_actualizada(engine: create_engine, df: pd.DataFrame, tabla: str, periodo: str) -> None:

    periodo_anterior = str(int(periodo) - 1)

    with engine.connect() as connection:
        # Crear cursor
        cursor = connection.connection.cursor()

        # Eliminar tabla si existe
        try:
            logging.info(f'Eliminando tabla {tabla.lower()}_{periodo}')
            cursor.execute(f"""
                DROP TABLE {tabla.lower()}_{periodo}
            """)
        except Exception as e:
            logging.warning(f'error al eliminar la tabla {
                tabla.lower()}_{periodo} {e}')

        # Crear copia de la tabla del periodo anterior
        try:
            logging.info(f'Creando tabla {tabla.lower()}_{periodo}')
            cursor.execute(f"""
                CREATE TABLE {tabla.lower()}_{periodo} AS
                SELECT * FROM {tabla.lower()}_{periodo_anterior} WHERE 1=0
            """)
        except Exception as e:
            logging.warning(f'error al crear la tabla {
                tabla.lower()}_{periodo} {e}')

    # Hacer la insercion por chunk df
    chunksize = 50000
    for i in range(0, df.shape[0], chunksize):
        df.iloc[i:i+chunksize].to_sql(tabla.lower() +
                                      f'_{periodo}', engine, if_exists='append', index=False)

    logging.info(f'Se creó la tabla {tabla.lower()}_{
                 periodo} con {df.shape[0]} registros')
