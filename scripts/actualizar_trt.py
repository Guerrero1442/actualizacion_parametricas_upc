import logging
from pathlib import Path
from unidecode import unidecode
import pandas as pd
from tkinter import filedialog
from database.operaciones_bdoracle import conectar_base_oracle, actualizar_datos_oracle, crear_tabla_longitudes
from scripts.limpieza_archivo import sacar_longitudes_max_columnas, quitar_espacios, limpiar_texto_columnas
from utils import seleccionar_archivo

NOMBRE_TABLA_TRT_ORACLE = 'TBL_OPE_NT_TRT_MEDICAMENTOS_'

def actualizar_trt_medicamentos() -> None:
    
    engine = conectar_base_oracle()
    ruta_trt = seleccionar_archivo(
        titulo="Seleccione el archivo de TRT Medicamentos",
        extension='.xlsx',
        tipos=[("Excel files", "*.xlsx")]
    )
    version_tabla = input('Ingrese el año y la versión de la tabla TRT (por ejemplo, 2024_41): ') 
    nombre_tabla = f'{NOMBRE_TABLA_TRT_ORACLE}{version_tabla}'.lower()
    
    logging.info(f'Actualizando TRT Medicamentos {ruta_trt.name}')
    
    df_columnas = pd.read_excel(ruta_trt, dtype='str', nrows=0)
    
    columnas = limpiar_texto_columnas(df_columnas)
        
    df = pd.read_excel(ruta_trt, dtype='str', names=columnas)
    
    
    df = (
        df.rename(columns=lambda x: x.strip())
        .rename(columns=lambda x: x.replace('.', ''))
        .rename(columns=lambda x: unidecode(x))
        .pipe(quitar_espacios)
    )

    longitudes_max_columnas = sacar_longitudes_max_columnas(df)
    
    crear_tabla_longitudes(engine ,nombre_tabla, longitudes_max_columnas)
    
    actualizar_datos_oracle(engine, df, nombre_tabla)
    
    crear_tabla_longitudes(engine , 'tbl_ope_nt_trt_medicamentos_2025', longitudes_max_columnas)

    actualizar_datos_oracle(engine, df, 'tbl_ope_nt_trt_medicamentos_2025')

if __name__ == '__main__':     
    actualizar_trt_medicamentos()