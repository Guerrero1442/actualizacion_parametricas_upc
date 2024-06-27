# Coneccion base de datos
from sqlalchemy import create_engine
import cx_Oracle
import configparser
import pandas as pd

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
    engine: create_engine = create_engine(
        f'oracle+cx_oracle://{user}:{password}@{dsn}')
    return engine


def actualizar_datos_oracle(engine: create_engine, df: pd.DataFrame, tabla: str):
    with engine.connect() as connection:
        cursor = connection.connection.cursor()
        try:
            cursor.execute(f"TRUNCATE TABLE {tabla}")
        except Exception as e:
            print(e)
        df.to_sql(tabla, con=engine, if_exists='append', index=False)
        cursor.close()
    print(f'Se actualizaron {df.shape[0]} registros en la tabla {tabla}')


def creacion_tabla_actualizada(engine: create_engine, df: pd.DataFrame, tabla: str, periodo: str) -> None:

    periodo_anterior = str(int(periodo) - 1)

    with engine.connect() as connection:
        # Crear cursor
        cursor = connection.connection.cursor()

        # Eliminar tabla si existe
        try:
            cursor.execute(f"""
                DROP TABLE {tabla.lower()}_{periodo}
            """)
        except Exception as e:
            print(e)

        # Crear copia de la tabla del periodo anterior
        try:
            cursor.execute(f"""
                CREATE TABLE {tabla.lower()}_{periodo} AS
                SELECT * FROM {tabla.lower()}_{periodo_anterior} WHERE 1=0
            """)
        except Exception as e:
            print(e)

    # Hacer la insercion por chunk df
    chunksize = 50000
    for i in range(0, df.shape[0], chunksize):
        df.iloc[i:i+chunksize].to_sql(tabla.lower() +
                                      f'_{periodo}', engine, if_exists='append', index=False)

    print(f'''Tabla {tabla.lower()}_{
          periodo} creada con éxito, se insertaron {df.shape[0]} registros''')
