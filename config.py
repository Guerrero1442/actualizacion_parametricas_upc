# config.py

from pathlib import Path

ZIP_EXTENSION = '.zip'
EXCEL_EXTENSION = '.xlsx'
NOMBRE_TABLA_TRT_ORACLE = 'TBL_OPE_NT_TRT_MEDICAMENTOS_'

INITIAL_DIRS = {
    'vigencia': Path(r'G:\Mi unidad\Mis Actividades\Actualizacion Parametricas UPC\VIGENCIA_MINSALUD'),
    'compensados': Path(r'G:\.shortcut-targets-by-id\1buUUJ2naBFTn-E10CXNd8elJ6YQOlWCR\00_BASES_COMPENSACION_2024'),
    'nt_unicos': Path(r'G:\Mi unidad\Mis Actividades\Actualizacion Parametricas UPC\NT_UNICOS'),
    'trt_medicamentos': Path(r'G:\Mi unidad\Mis Actividades\Actualizacion Parametricas UPC\TRT'),
    'prestadores': Path(r'G:\Mi unidad\Mis_Actividades\Actualizacion Parametricas UPC\PRESTADORES')
}

OPCIONES_PERMITIDAS = ['1', '2', '3', '4', '5', '6']