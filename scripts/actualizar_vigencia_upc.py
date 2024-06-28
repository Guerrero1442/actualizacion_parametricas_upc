import tempfile
import pathlib
from tkinter import filedialog
import zipfile
import pandas as pd
from sqlalchemy import create_engine
from operaciones_bdoracle import actualizar_datos_oracle, conectar_base_oracle


def procesar_archivo_vigencia(engine: create_engine, archivo: pathlib.Path, anio_actualizado: str) -> None:
    nombre_archivo: str = archivo.name

    if nombre_archivo.startswith(f'{anio_actualizado}_INSUMOS'):
        df = pd.read_excel(archivo, skiprows=4, engine='pyxlsb')
        if codigo_duplicado(df, 'CÓDIGO') == 0:
            df.drop(columns=['AÑO_VIGENCIA', ' '],
                    inplace=True, errors='ignore')
            df.rename(columns={'CÓDIGO': 'CODIGO',
                      'DESCRIPCIÓN': 'DESCRIPCION'}, inplace=True)
            actualizar_datos_oracle(
                engine, df, f'tbl_suf_insumos_{anio_actualizado}')

    elif nombre_archivo.startswith(f'{anio_actualizado}_TR_CIE10'):
        df = pd.read_excel(archivo, skiprows=4, engine='pyxlsb')
        if codigo_duplicado(df, 'CODIGO') == 0:
            df.rename(columns={
                'VIGENCIA': 'Tabla', 'CODIGO': 'CIE10', 'DESCRIPCION': 'DESCRIPCIÓN CÓDIGOS DE CUATRO CARACTERES',
                'EDAD_LIM_INF': 'VALOR_LIM_INF', 'EDAD_LIM_SUP': 'VALOR_LIM_SUP'
            }, inplace=True)
            actualizar_datos_oracle(
                engine, df, f'tbl_suf_cie10_{anio_actualizado}')

    elif nombre_archivo.startswith(f'{anio_actualizado}_TR_CUPS') and 'COBERTURA' in nombre_archivo:
        df = pd.read_excel(archivo, skiprows=4, engine='pyxlsb')
        if codigo_duplicado(df, 'CODIGO') == 0:
            df.drop(columns=['AÑO_VIGENCIA', ' '],
                    inplace=True, errors='ignore')
            df.rename(
                columns={'DX_RELACIONADO': 'CIE_10 RELACIONADOS'}, inplace=True)
            actualizar_datos_oracle(
                engine, df, f'tbl_suf_cups_{anio_actualizado}')

    elif nombre_archivo.startswith(f'{anio_actualizado}_REPS'):
        df = pd.read_excel(archivo, skiprows=4, engine='pyxlsb')
        if codigo_duplicado(df, 'CÓDIGO HABILITACION') == 0:
            df.drop(columns=['AÑO_VIGENCIA'], inplace=True, errors='ignore')
            df.rename(columns={
                'CÓDIGO HABILITACION': 'COD_PRESTADOR', 'NOMBRE PRESTADOR': 'NOM_PRESTADOR', 'NIT': 'NIT'
            }, inplace=True)
            actualizar_datos_oracle(
                engine, df, f'tbl_suf_reps_{anio_actualizado}')


def actualizar_vigencia_upc(engine: create_engine, zip_path: pathlib.Path) -> None:
    anio_actualizado = input('Ingrese el año de actualización: ')

    with zipfile.ZipFile(zip_path, 'r') as archivo_zip:
        archivos = archivo_zip.namelist()

        with tempfile.TemporaryDirectory() as temp_dir:
            archivo_zip.extractall(temp_dir)
            archivos = [pathlib.Path(temp_dir) /
                        archivo for archivo in archivos]

            for archivo in archivos:
                procesar_archivo_vigencia(engine, archivo, anio_actualizado)

    print('Actualización de vigencia UPC exitosa')


def codigo_duplicado(df: pd.DataFrame, columna: str) -> int:
    return df.loc[df.duplicated(subset=columna)].shape[0]


if __name__ == '__main__':
    engine = conectar_base_oracle()

    ruta_zip = pathlib.Path(filedialog.askopenfilename(
        initialdir=r'G:\Mi unidad\Mis Actividades\Actualizacion Parametricas UPC\Vigencia_UPC'))

    actualizar_vigencia_upc(engine, ruta_zip)