import logging
import pathlib
import pandas as pd
from tkinter import filedialog
from operaciones_bdoracle import conectar_base_oracle, actualizar_datos_oracle

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

nombre_tabla_nt_unicos: str = 'tbl_ope_nt_unicos_2024'


def actualizar_nt_unicos(engine, ruta_nt_unicos):
    df = pd.read_excel(ruta_nt_unicos, dtype='str')
    logging.info(f'Procesando archivo {ruta_nt_unicos.name}')

    if df.loc[df.duplicated(subset=['Codigo OSI'])].shape[0] > 0:
        print('Hay duplicados en el archivo')
        return

    if df.loc[df['Codigo OSI'].isnull()].shape[0] > 0:
        print('Hay valores nulos en el archivo')
        return

    for col in df.columns:
        df[col] = df[col].str.replace(r'\s{2,}', ' ', regex=True).str.strip()

    df.drop(columns=['#', '-', 'CONSULTA VIRTUAL',
            'TELEXPERTICIA'], inplace=True, errors='ignore')

    df['DESC_CUPS'] = df['Descripción BH']

    df.rename(columns=columnas_renombradas, inplace=True)

    actualizar_datos_oracle(engine, df, nombre_tabla_nt_unicos)


if __name__ == '__main__':
    engine = conectar_base_oracle()

    ruta_nt_unicos = pathlib.Path(filedialog.askopenfilename(
        initialdir=r'G:\Mi unidad\Mis Actividades\Actualizacion Parametricas UPC\NT_UNICOS'))

    actualizar_nt_unicos(engine, ruta_nt_unicos)
