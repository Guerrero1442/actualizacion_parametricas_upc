# Coneccion base de datos
from sqlalchemy import create_engine
import cx_Oracle
import configparser
import pandas as pd

# Constantes
CONFIG_PATH = '../config.ini'


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
    engine: create_engine = create_engine(f'oracle+cx_oracle://{user}:{password}@{dsn}')
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
    print(f'Tabla {tabla} actualizada')