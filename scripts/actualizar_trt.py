import logging
from unidecode import unidecode
import pandas as pd
from database.operaciones_bdoracle import conectar_base_oracle, actualizar_datos_oracle, crear_tabla_longitudes
from scripts.limpieza_archivo import  quitar_espacios, limpiar_texto_columnas
from utils import seleccionar_archivo

def actualizar_trt_medicamentos(config: dict) -> None:
    
    engine = conectar_base_oracle()
    ruta_trt = seleccionar_archivo(
        titulo="Seleccione el archivo de TRT Medicamentos",
        extension='.xlsx',
        tipos=[("Excel files", "*.xlsx")]
    )
    version_tabla = input('Ingrese el año y la versión de la tabla TRT (por ejemplo, 2024_41): ') 
    nombre_tabla = f'{config.get('tabla_base', 'tbl_ope_nt_trt_medicamentos_')}{version_tabla}'.lower()
    
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

    
    crear_tabla_longitudes(engine ,nombre_tabla, df)
    
    actualizar_datos_oracle(engine, df, nombre_tabla)
    
    tabla_anual = config.get('tabla_actual', 'tbl_ope_nt_trt_medicamentos_2025')
    
    crear_tabla_longitudes(engine , tabla_anual, df)

    actualizar_datos_oracle(engine, df, tabla_anual)

if __name__ == '__main__':     
    config_prueba = {
        "tabla_base": "TBL_OPE_NT_TRT_MEDICAMENTOS_",
        "tabla_actual": "tbl_ope_nt_trt_medicamentos_2025"
    }
    actualizar_trt_medicamentos(config_prueba)        
    
