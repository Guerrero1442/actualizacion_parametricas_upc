import logging
import pathlib
import pandas as pd
from tkinter import filedialog
from database.operaciones_bdoracle import conectar_base_oracle, actualizar_datos_oracle

# Crear el diccionario de mapeo
columnas_renombradas: dict = {
    'Cod. BH': 'Cod. BH',
    'Descripción BH': 'Descripción BH',
    'Cod. 5851': 'Cod. 5851 ',
    'Descripción 5851': 'Descripción 5851 ',
    'Codigo OSI': 'Codigo_OSI',
    'Descripcion OSI': 'DESCRIP_PROCEDIMIENTO',
    'CUPS': 'CUPS',
    'Grupo_Principal': 'Grupo_Principal',
    'Grupo_1': 'Grupo_1',
    'Grupo_2': 'Grupo_2',
    'Grupo_3': 'Grupo_3',
    'Jerarquia': 'JERARQUIA',
    'Agrupa': 'AGRUPA',
    'Tipo_Servicio': 'TIPO_SERVICIO',
    'Registros en Utilizacion': 'REGISTROS_EN_UTILIZACION',
    'Servicio': 'SERVICIO',
    'REPS': 'REPS',
    'REPS 2': 'REPS_2',
    'Especialidad': 'Especialidad',
    'GUIA': 'GUIA',
    'Novedad': 'NOVEDAD',
    'Tipo Novedad': 'TIPO_NOVEDAD',
    'Fecha Novedad': 'FECHA_NOVEDAD',
    'Historico Novedades': 'HISTORICO_NOVEDADES',
    'ACTIVO EN PRODUCTO PLAN': 'ACTIVO_EN_PRODUCTO_PLAN',
    'DESCRIPCION': 'DESCRIPCION',
    'INDICADOR SNS\nAUTORIZACION': 'INDICADOR_SNS_AUTORIZACION',
    'INDICADOR SNS\n PRESTACION': 'INDICADOR_SNS_PRESTACION',
    'ANTIGUO Grupo_Principal': 'ANTIGUO_GRUPO_PRINCIPAL',
    'ANTIGUO Grupo_1': 'ANTIGUO_GRUPO_1',
    'ANTIGUO Grupo_2': 'ANTIGUO_GRUPO_2',
    'ANTIGUO Grupo_3': 'ANTIGUO_GRUPO_3',
    'ANTIGUO Jerarquia': 'ANTIGUO_JERARQUIA',
    'ANTIGUO Agrupa': 'ANTIGUO_AGRUPA',
    'ANTIGUO Tipo_Servicio': 'ANTIGUO_TIPO_SERVICIO',
    'CUPS VALIDAR': 'CUPS_VALIDAR',
    'DESCRIPCION VALIDAR': 'DESCRIPCION_VALIDAR',
    'SEXO': 'SEXO',
    'ÁMBITO': 'ÁMBITO',
    'ESTANCIA': 'ESTANCIA',
    'COBERTURA': 'COBERTURA',
    'DUPLICADO': 'DUPLICADO',
    'VIGENTE': 'VIGENTE',
    'REPS3': 'REPS3',
    'AMBULATORIO': 'AMBULATORIO',
    'DOMICILIARIO': 'DOMICILIARIO',
    'HOSPITALIZACION': 'HOSPITALIZACION',
    'URGENCIA': 'URGENCIA',
    'ACTIVO EN PRODUCTO PLAN.1': 'ACTIVO_EN_PRODUCTO_PLAN2',
    'Inclusion a PBS 2022': 'INCLUSION_A_PBS_2022',
    'BHREPS-1': 'BHREPS_1',
    'BHREPS-2': 'BHREPS_2',
    'BHREPS-3': 'BHREPS_3',
    'CODIGO REPS 3100 OPCION UNO': 'CODIGO_REPS_3100_OPCION_UNO',
    'NOMBRE DEL REPS 3100 OPCION UNO': 'NOMBRE_DEL_REPS_3100_OPCION_UNO',
    'CODIGO REPS 3100 OPCION DOS': 'CODIGO_REPS_3100_OPCION_DOS',
    'NOMBRE DEL REPS 3100 OPCION DOS': 'NOMBRE_DEL_REPS_3100_OPCION_DOS',
    'CODIGO REPS 3100 OPCION TRES': 'CODIGO_REPS_3100_OPCION_TRES',
    'NOMBRE DEL REPS 3100 OPCION TRES': 'NOMBRE_DEL_REPS_3100_OPCION_TRES'
}

nombre_tabla_nt_unicos: str = 'tbl_ope_nt_unicos_2025'

def eliminar_registros_nulos(df, column_name):
    df = df.copy()
    return df.loc[df[column_name].notnull()]

def transformar_fecha_novedad(df, column_name:str='Fecha Novedad', format:str='%Y-%m-%d'):
    df = df.copy()
    df[column_name] = df[column_name].str.slice(stop=10)
    df[column_name] = pd.to_datetime(df[column_name], format=format)
    return df

def eliminar_registros_duplicados(df, column_name:str='Codigo OSI', sort_by:str='Fecha Novedad', ruta_nt_unicos=None):
    df = df.copy()
    codigos_duplicados = df.loc[df.duplicated(subset=[column_name])][column_name].unique()
    logging.info(f'Hay {len(codigos_duplicados)} {column_name} OSI duplicados en el archivo {ruta_nt_unicos.name} se tomara el que tenga fecha novedad mas reciente')
    return df.sort_values(by=[column_name, sort_by], ascending=[False, True]).drop_duplicates(subset=[column_name], keep='last')

def formatear_fecha(df, column_name:str='Fecha Novedad', format:str='%Y-%m-%d'):
    df = df.copy()
    df[column_name] = df[column_name].dt.strftime(format)
    return df

def limpiar_columnas(df):
    df = df.copy()
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.replace(r'\s{2,}', ' ', regex=True).str.strip()
    return df

def eliminar_columnas(df, columns: list):
    df = df.copy()
    df.drop(columns=columns, inplace=True, errors='ignore')
    return df

def renombrar_columnas(df):
    df = df.copy()
    df['DESC_CUPS'] = df['Descripción BH']
    df.rename(columns=columnas_renombradas, inplace=True)
    return df    

def actualizar_nt_unicos(engine, ruta_nt_unicos):
    df = pd.read_excel(ruta_nt_unicos, dtype='str')
    logging.info(f'Procesando archivo {ruta_nt_unicos.name}')
    
    df_limpio = (df.pipe(eliminar_registros_nulos, 'Codigo OSI')
                    .pipe(transformar_fecha_novedad, 'Fecha Novedad', '%Y-%m-%d')
                    .pipe(eliminar_registros_duplicados, 'Codigo OSI', 'Fecha Novedad', ruta_nt_unicos)
                    .pipe(formatear_fecha,'Fecha Novedad', '%Y-%m-%d')
                    .pipe(limpiar_columnas)
                    .pipe(eliminar_columnas, ['#', '-', 'CONSULTA VIRTUAL', 'TELEXPERTICIA'])
                    .pipe(renombrar_columnas)
                )

    actualizar_datos_oracle(engine, df_limpio, nombre_tabla_nt_unicos)


if __name__ == '__main__':
    engine = conectar_base_oracle()

    ruta_nt_unicos = pathlib.Path(filedialog.askopenfilename(
        initialdir=r'G:\Mi unidad\Mis Actividades\Actualizacion Parametricas UPC\NT_UNICOS'))

    actualizar_nt_unicos(engine, ruta_nt_unicos)
