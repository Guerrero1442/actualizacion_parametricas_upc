import logging
from tkinter import filedialog
from sqlalchemy import create_engine
import pathlib
import pandas as pd
from operaciones_bdoracle import conectar_base_oracle, creacion_tabla_actualizada

# Constantes
# Archivos permitidos para cada grupo
beneficiario: list[str] = ['CORRECCION_BEN', 'AB4023']
cotizante: list[str] = ['CORRECCION_COT', 'AC4023']
liquidado: list[str] = ['EPSS05RESTITUCION', 'EPSS05LIQUIDACION']
# Objeto con nombre de tablas
tablas: dict[str, str] = {
    'beneficiario': 'TBL_OPE_COMPENSADO_BEN',
    'cotizante': 'TBL_OPE_COMPENSADO_COT',
    'liquidado': 'EPSS05LIQUIDACION'
}


def actualizar_compensados(engine: create_engine, ruta_compensados: pathlib.Path) -> None:
    # Preguntar periodo de actualizacion
    periodo = input("Ingrese el periodo de actualización (YYYYMM): ")
    logging.info(f'Actualizando compensados para el periodo {periodo}')

    archivos = [archivo for archivo in ruta_compensados.iterdir(
    ) if archivo.is_file() and archivo.suffix == '.txt']

    df_liquidados: list[pd.DataFrame] = []

    for archivo in archivos:
        if liquidado[0] in archivo.name or liquidado[1] in archivo.name:
            logging.info(f'Procesando archivo {archivo.name}')
            df: pd.DataFrame = pd.read_csv(
                archivo, sep=',', dtype='str', encoding='latin1')
            df.drop(columns=['CAUSAL_DE_RESTITUCION',
                    'TIPO DE NOVEDAD'], inplace=True, errors='ignore')
            df_liquidados.append(df)

    df_liquidado = pd.concat(df_liquidados)

    creacion_tabla_actualizada(
        engine, df_liquidado, tablas['liquidado'], periodo)

    df_cotizantes: list[pd.DataFrame] = []

    for archivo in archivos:
        if cotizante[0] in archivo.name or cotizante[1] in archivo.name:
            logging.info(f'Procesando archivo {archivo.name}')
            df = pd.read_csv(archivo, sep=',', dtype='str', encoding='latin1')
            df.rename(columns={'DEPARTAMENTO': 'COD_DEP', 'MUNICIPIO': 'COD_MUN',
                      'EXONERACIÓN': 'EXONERACION', 'CENTRO_COSTO': 'CENTRO_DE_COSTO'}, inplace=True)
            df_cotizantes.append(df)

    df_cotizante = pd.concat(df_cotizantes)

    creacion_tabla_actualizada(
        engine, df_cotizante, tablas['cotizante'], periodo)

    df_beneficiarios: list[pd.DataFrame] = []

    for archivo in archivos:
        if beneficiario[0] in archivo.name or beneficiario[1] in archivo.name:
            logging.info(f'Procesando archivo {archivo.name}')
            df = pd.read_csv(archivo, sep=',', dtype='str', encoding='latin1')
            df.rename(columns={'DEPARTAMENTO': 'COD_DEP', 'MUNICIPIO': 'COD_MUN',
                      'EXONERACIÓN': 'EXONERACION'}, inplace=True)
            df_beneficiarios.append(df)

    df_beneficiario = pd.concat(df_beneficiarios)

    creacion_tabla_actualizada(
        engine, df_beneficiario, tablas['beneficiario'], periodo)


if __name__ == '__main__':
    ruta_compensados = pathlib.Path(filedialog.askdirectory(
        initialdir=r'G:\.shortcut-targets-by-id\1buUUJ2naBFTn-E10CXNd8elJ6YQOlWCR\00_BASES_COMPENSACION_2024'))

    engine: create_engine = conectar_base_oracle()

    actualizar_compensados(engine, ruta_compensados)
