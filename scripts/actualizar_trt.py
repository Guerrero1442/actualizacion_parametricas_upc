import logging
from pathlib import Path
from unidecode import unidecode
import pandas as pd
from tkinter import filedialog
from database.operaciones_bdoracle import conectar_base_oracle, actualizar_datos_oracle, crear_tabla_bytes
from scripts.limpieza_archivo import sacar_longitudes_max_columnas, convertir_texto_dataframe, limpiar_texto_columnas

NOMBRE_TABLA_TRT_ORACLE = 'TBL_OPE_NT_TRT_MEDICAMENTOS_'

def actualizar_trt_medicamentos(engine, ruta_trt: Path, nombre_tabla: str) -> None:
    logging.info(f'Actualizando TRT Medicamentos {ruta_trt.name}')
    
    df_columnas = pd.read_excel(ruta_trt, dtype='str', nrows=0)
    
    columnas = limpiar_texto_columnas(df_columnas)
        
    df = pd.read_excel(ruta_trt, dtype='str', names=columnas)
    
    df.columns = df.columns.str.replace('.', '')
    
    df = df.pipe(convertir_texto_dataframe)

    longitudes_max_columnas = sacar_longitudes_max_columnas(df)
    
    crear_tabla_bytes(engine ,nombre_tabla, df, df.columns, longitudes_max_columnas)
    
    actualizar_datos_oracle(engine, df, nombre_tabla)
    
    crear_tabla_bytes(engine , 'tbl_ope_nt_trt_medicamentos_2024', df, df.columns, longitudes_max_columnas)

    actualizar_datos_oracle(engine, df, 'tbl_ope_nt_trt_medicamentos_2024')

def quitar_acentos(texto: str) -> str:
    return unidecode(texto)


if __name__ == '__main__':
    ruta_trt = Path(filedialog.askopenfilename())
    
    version_tabla = input('Ingrese el año y la versión de la tabla TRT (por ejemplo, 2024_41): ')
    
    nombre_tabla = f'{NOMBRE_TABLA_TRT_ORACLE}{version_tabla}'.lower()
    
    engine = conectar_base_oracle()
        
    actualizar_trt_medicamentos(engine, ruta_trt, nombre_tabla)