import tempfile
import pathlib
import zipfile
import numpy as np
import pandas as pd
import logging
from database.operaciones_bdoracle import conectar_base_oracle, actualizar_datos_oracle, obtener_datos_oracle
from utils import seleccionar_archivo

def definir_cobertura_cups(df: pd.DataFrame, column: str) -> pd.DataFrame:
    df = df.copy()
    
    df['COBERTURA_MINSALUD'] = df[column]
    
    valores_pbs = ['Financiada UPC']
    
    df[column] = np.where(df[column].isin(valores_pbs), 'PBS', 'NPBS')

    return df

def actualizar_cups_homologo(df: pd.DataFrame, anio: int) -> pd.DataFrame:
    df = df.copy()
    
    engine = conectar_base_oracle()
        
    df_cups_anio_anterior = obtener_datos_oracle(engine, f'tbl_suf_cups_{anio - 1}')
    
    df_cups_anio_anterior.rename(columns=lambda x: x.upper(), inplace=True)

    df_cups_anio_anterior.dropna(subset=['CUPS_HOMOLOGO_PRIMERA_VEZ'], inplace=True)
    
    diccionario_cups = df_cups_anio_anterior.set_index('CODIGO')['CUPS_HOMOLOGO_PRIMERA_VEZ']
    
    df['CUPS_HOMOLOGO_PRIMERA_VEZ'] = df['CODIGO'].map(diccionario_cups)
        
    return df

def procesar_archivo_vigencia(engine, archivo: pathlib.Path, anio_actualizado: str, config: dict) -> None:
    nombre_archivo: str = archivo.name
    tablas_base = config.get('tablas_base')
    
    if not tablas_base:
        logging.error('No se encontraron tablas base en la configuración')
        return
    
    # TBL_OPE_SUF_INSUMOS_[AÑO]
    if nombre_archivo.startswith(f'{anio_actualizado}_INSUMOS'):
        tabla_insumos = f"{tablas_base['insumos']}{anio_actualizado}"
        logging.info(f'Procesando archivo {nombre_archivo}')

        df_insumos = pd.read_excel(archivo, skiprows=4, engine='pyxlsb')
        
        if codigo_duplicado(df_insumos, 'CÓDIGO') == 0:
            df_insumos_procesados = (
                df_insumos
                .drop(columns=['AÑO_VIGENCIA', ' '])
                .rename(columns={'CÓDIGO': 'CODIGO', 'DESCRIPCIÓN': 'DESCRIPCION'})
            )       
            actualizar_datos_oracle(engine, df_insumos_procesados, tabla_insumos)
        else:
            logging.error(f'El archivo {nombre_archivo} contiene códigos duplicados')

    # TBL_OPE_SUF_CIE10_[AÑO]
    elif nombre_archivo.startswith(f'{anio_actualizado}_TABLA DE REFERENCIA CIE-10'):
        tabla_cie10 = f"{tablas_base['cie10']}{anio_actualizado}"

        logging.info(f'Procesando archivo {nombre_archivo}')
        df_cie10 = pd.read_excel(archivo, skiprows=4,)
        
        if codigo_duplicado(df_cie10, 'Codigo') == 0:
            df_cie10_procesado = (
                df_cie10
                .rename(columns={
                'VIGENCIA': 'Tabla', 'Codigo': 'CIE10', 'Nombre': 'DESCRIPCIÓN CÓDIGOS DE CUATRO CARACTERES',
                'EDAD_LIM_INF': 'VALOR_LIM_INF', 'EDAD_LIM_SUP': 'VALOR_LIM_SUP'})
            )
            actualizar_datos_oracle(engine, df_cie10_procesado, tabla_cie10)
        else:
            logging.error(f'El archivo {nombre_archivo} contiene códigos duplicados')


    # TBL_SUF_CUPS_[AÑO]
    elif nombre_archivo.startswith(f'{anio_actualizado}_TR_CUPS') and 'COBERTURA' in nombre_archivo:
        tabla_cups = f"{tablas_base['cups']}{anio_actualizado}"
        logging.info(f'Procesando archivo {nombre_archivo}')
        
        df_cups = pd.read_excel(archivo, skiprows=3, engine='pyxlsb')
        
        if codigo_duplicado(df_cups, 'CÓDIGO') == 0:          
            df_cups_procesados = (
                df_cups
                .rename(columns= lambda x: x.strip())
                .drop(columns=['AÑO_VIGENCIA', ' '], errors='ignore')
                .rename(columns = {'CÓDIGO': 'CODIGO', 'DX_RELACIONADO': 'CIE_10 RELACIONADOS', 'DESCRIPCIÓN': 'DESCRIPCION'}) 
                .pipe(definir_cobertura_cups, column='COBERTURA')
                .pipe(actualizar_cups_homologo, anio=int(anio_actualizado))
            )
            
            actualizar_datos_oracle(
                engine, df_cups_procesados, tabla_cups)
        else:
            logging.error(f'El archivo {nombre_archivo} contiene códigos duplicados')

    # TBL_OPE_SUF_REPS_[AÑO]
    elif nombre_archivo.startswith(f'{anio_actualizado}_REPS'):
        tabla_reps = f"{tablas_base['reps']}{anio_actualizado}"
        logging.info(f'Procesando archivo {nombre_archivo}')
         
        if nombre_archivo.endswith('xlsx'):
            df_reps = pd.read_excel(archivo, skiprows=3)
        else:
            df_reps = pd.read_excel(archivo, skiprows=3, engine='pyxlsb')
        print(f'Columnas archivo {nombre_archivo}: {df_reps.columns}')
        if codigo_duplicado(df_reps, 'CÓDIGO HABILITACION') == 0:
            df_reps_procesado = (
                df_reps
                .drop(columns=['AÑO_VIGENCIA'], errors='ignore')
                .rename(columns={'CÓDIGO HABILITACION': 'COD_PRESTADOR', 'NOMBRE PRESTADOR': 'NOM_PRESTADOR', 'NIT': 'NIT'})
            )
            
            actualizar_datos_oracle(
                engine, df_reps_procesado, tabla_reps)
        else:
            logging.error(f'El archivo {nombre_archivo} contiene códigos de habilitacion duplicados')


def actualizar_vigencia_upc(config: dict) -> None:
    zip_path = seleccionar_archivo(titulo="Seleccione el archivo ZIP de la vigencia UPC", 
                                   extension='.zip', 
                                   tipos=[("ZIP files", "*.zip")])
    engine = conectar_base_oracle()
    anio_actualizado = input('Ingrese el año de actualización: ')

    with zipfile.ZipFile(zip_path, 'r') as archivo_zip:
        logging.info(f'Extrayendo archivos de {zip_path}')
        archivos = archivo_zip.namelist()

        with tempfile.TemporaryDirectory() as temp_dir:
            archivo_zip.extractall(temp_dir)
            archivos = [pathlib.Path(temp_dir) /
                        archivo for archivo in archivos]

            for archivo in archivos:
                procesar_archivo_vigencia(engine, archivo, anio_actualizado, config)

    logging.info('Proceso de actualización de vigencia UPC finalizado')


def codigo_duplicado(df: pd.DataFrame, columna: str) -> int:
    return df.loc[df.duplicated(subset=columna)].shape[0]


if __name__ == '__main__':
    config_prueba = {
        "tablas_base": {
            "insumos": "tbl_suf_insumos_",
            "cie10": "tbl_suf_cie10_",
            "cups": "tbl_suf_cups_",
            "reps": "tbl_suf_reps_"
        }
    }
    actualizar_vigencia_upc(config_prueba)