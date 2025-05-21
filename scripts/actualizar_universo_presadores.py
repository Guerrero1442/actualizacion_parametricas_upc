import pathlib
import tempfile
from tkinter import filedialog
import zipfile
import logging

import pandas as pd
from sqlalchemy import create_engine

from database.operaciones_bdoracle import actualizar_datos_oracle, conectar_base_oracle, creacion_tabla_actualizada, obtener_datos_oracle

# Constantes
NOMBRE_TABLA_PRESTADORES = 'tbl_ope_universo_prestadores'
COLUMNAS_PRESTADORES = [
    'DESCRIPCION PLAN',
    'FORMA CONTRATACION',
    'NUM ID',
    'TIPO ID',
    'TIPO PERSONA',
    'CODIGO SUCURSAL',
    'NOMBRE SUCURSAL',
    'CIUDAD',
    'DESCRIPCION CIUDAD',
    'DEPARTAMENTO',
    'DESCRIPCION ESPECIALIDAD',
    'ESTADO',
    'TIPO CONVENIO',
    'COD HABILITACION SUCURSAL',
    'HABILITACIÓN SEDE SUCURSAL',
    'FECHA INICIO CONVENIO',
    'FECHA FIN CONVENIO',
    'REGIONAL']

carpeta_inicial = pathlib.Path(
    r'G:\Mi unidad\Mis_Actividades\Actualizacion Parametricas UPC\PRESTADORES')

carpeta_regionales = pathlib.Path(
    r'G:\.shortcut-targets-by-id\1wT-pRaNOECz6KC5hndveeLOIGY381o4T\Alteryx\Proyectos\154._Tableros_RIPS\03.Salidas\_Tb_Regiones_.csv')


def actualizar_prestadores(engine: create_engine, ruta_prestadores: pathlib.Path) -> None:
    """
    Actualiza la información de los prestadores en la base de datos para un período específico.
    Esta función solicita al usuario un período de actualización en formato YYYYMM, valida la entrada y procesa 
    un archivo ZIP que contiene un archivo Excel con los datos de los prestadores. Extrae y transforma los 
    datos, realiza un cruce con información regional, y actualiza la base de datos Oracle con los nuevos 
    datos de los prestadores. Además, marca como 'FINALIZADO' aquellos prestadores que no están presentes 
    en la actualización actual.
    Args:
        engine (create_engine): Motor de la base de datos utilizado para las operaciones de lectura y escritura.
        ruta_prestadores (pathlib.Path): Ruta al archivo ZIP que contiene el archivo Excel con los datos de los prestadores.
    Returns:
        None
    """

    periodo = input('Ingrese el periodo de actualización (YYYYMM): ')
    
    if not periodo.isdigit() or len(periodo) != 6:
        logging.error('El periodo debe ser un número de 6 dígitos')
        return

    logging.info(f'Actualizando prestadores para el periodo {periodo}')

    with zipfile.ZipFile(ruta_prestadores, 'r') as zip_file:
        with tempfile.TemporaryDirectory() as temp_dir:
            zip_file.extractall(temp_dir)
            archivos = pathlib.Path(temp_dir).rglob(
                '*.xlsx')  # Esto es un generador
            try:
                # Obtenemos el primer archivo encontrado
                archivo = next(archivos, None)

                if archivo is None:
                    logging.error(
                        f'No se encontró ningún archivo .xlsx en el ZIP')
                else:
                    logging.info(f'Leyendo archivo {archivo}')
                    df_prestadores = pd.read_excel(archivo, sheet_name='E.P.S Sanitas',
                                                   skiprows=2, dtype='str', usecols=COLUMNAS_PRESTADORES)
            except Exception as e:
                logging.error(f'Error al leer el archivo de prestadores: {e}')

    try:
        df_regionales = pd.read_csv(carpeta_regionales, sep='|', dtype='str')
    except FileNotFoundError:
        logging.error(f'No se encontró el archivo de regionales en {carpeta_regionales}')
    except Exception as e:
        logging.error(f'Error al leer el archivo de regionales: {e}')

    # Preparacion para cruce de regionales
    df_regionales['regional_adaptada'] = df_regionales['Regional'].str.replace(
        ' ', '_', 1)
    df_regionales['nombre_region'] = df_regionales['regional_adaptada'].str.split(
        pat=' ', n=1).str[1]

    # Mantener unicos por nombre de regional y regional adaptada
    df_regionales.drop_duplicates(
        subset=['nombre_region', 'regional_adaptada'], inplace=True)

    # Cruce de regionales con prestadores
    df_prestadores = pd.merge(df_prestadores, df_regionales[[
                              'regional_adaptada', 'nombre_region']], left_on='REGIONAL', right_on='nombre_region', how='inner')
    df_prestadores.drop(columns=['nombre_region', 'REGIONAL'], inplace=True)
    df_prestadores.rename(
        columns={'regional_adaptada': 'REGIONAL'}, inplace=True)

    # Procesamiento prestadores
    df_prestadores['CODIGO SUCURSAL23'] = df_prestadores['CODIGO SUCURSAL']
    df_prestadores['AVICENA'] = ''
    df_prestadores['NIT2'] = df_prestadores['TIPO ID'].str.slice(
        0, 1) + df_prestadores['NUM ID']
    df_prestadores['MUNICIPIO AUTORIZADO'] = ''
    df_prestadores['COD_HABILITACION'] = df_prestadores['COD HABILITACION SUCURSAL'] + df_prestadores['HABILITACIÓN SEDE SUCURSAL']

    # Eliminar columnas no necesarias
    df_prestadores.drop(columns=['HABILITACIÓN SEDE SUCURSAL',
                        'COD HABILITACION SUCURSAL', 'TIPO ID'], inplace=True)
    # Define el nombre de las columnas
    column_renames = {
        "FORMA CONTRATACION": "RELACION EPS",
        "NUM ID": "NIT",
        "TIPO PERSONA": "TIPO_PERSONA",
        "CODIGO SUCURSAL": "CODIGO SUCURSAL22",
        "NOMBRE SUCURSAL": "PRESTADOR",
        "CIUDAD": "COD_CIUDAD",
        "DESCRIPCION CIUDAD": "CIUDAD",
        "DESCRIPCION ESPECIALIDAD": "GRUPO_SERVICIO",
        "ESTADO": "Estado Actual",
        "FECHA INICIO CONVENIO": "INICIO_CONTRATO",
        "FECHA FIN CONVENIO": "FIN_VIGENCIA",
        "COD_HABILITACION": "CODIGO DE HABILITACION2",
    }
        
    df_prestadores.rename(columns=column_renames, inplace=True)
    
    # limpieza columnas
    df_prestadores['TIPO_PERSONA'] = df_prestadores['TIPO_PERSONA'].str.upper()
    
    # renombrar columnas a lower case cuando la columna no tiene espacios
    df_prestadores.columns = [col.lower() if ' ' not in col else col for col in df_prestadores.columns]
    
    creacion_tabla_actualizada(engine, df_prestadores, NOMBRE_TABLA_PRESTADORES, periodo)
    
    # obtener universo prestadores
    df_universo_prestadores = obtener_datos_oracle(engine, NOMBRE_TABLA_PRESTADORES)
    
    # poner marca df_universo_prestadores['Estado Actual'] = 'FINALIZADO' si no se encuentra en df_prestadores
    df_universo_prestadores.loc[~df_universo_prestadores['nit'].isin(df_prestadores['nit']), 'Estado Actual'] = 'FINALIZADO'
    
    df_universo_prestadores_finalizados = df_universo_prestadores.loc[df_universo_prestadores['Estado Actual'] == 'FINALIZADO']

    
    df_universo_prestadores_completos = pd.concat([df_prestadores, df_universo_prestadores_finalizados], ignore_index=True)
    
    df_universo_prestadores_completos.drop_duplicates(inplace=True)

    actualizar_datos_oracle(engine, df_universo_prestadores_completos, NOMBRE_TABLA_PRESTADORES)


if __name__ == '__main__':

    engine: create_engine = conectar_base_oracle()

    ruta_prestadores = filedialog.askopenfilename(
        initialdir=carpeta_inicial, title='Seleccione el archivo de prestadores')

    actualizar_prestadores(engine, ruta_prestadores)
