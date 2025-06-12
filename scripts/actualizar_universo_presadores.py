import pathlib
import tempfile
import zipfile
import logging
import pandas as pd
from database.operaciones_bdoracle import actualizar_datos_oracle, conectar_base_oracle, crear_tabla_longitudes, obtener_datos_oracle
from utils import seleccionar_archivo
 

def ajustar_regionales(df_regionales: pd.DataFrame) -> pd.DataFrame:
    
    df_regionales = df_regionales.copy()
    # Preparacion para cruce de regionales
    df_regionales['regional_adaptada'] = df_regionales['Regional'].str.replace(
        ' ', '_', 1)
    df_regionales['nombre_region'] = df_regionales['regional_adaptada'].str.split(
        pat=' ', n=1).str[1]

    # Mantener unicos por nombre de regional y regional adaptada
    df_regionales.drop_duplicates(
        subset=['nombre_region', 'regional_adaptada'], inplace=True)

    return df_regionales

def crear_columnas_nuevas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.assign(
        AVICENA = '',
        MUNICIPIO_AUTORIZADO = '',
        CODIGO_SUCURSAL23 = df['CODIGO SUCURSAL'].copy(),
        NIT2 = df['TIPO ID'].str.slice(0, 1) + df['NUM ID'],
        COD_HABILITACION = df['COD HABILITACION SUCURSAL'] + df['HABILITACIÓN SEDE SUCURSAL']
    )
    
    df.drop(columns=['HABILITACIÓN SEDE SUCURSAL', 'COD HABILITACION SUCURSAL', 'TIPO ID'], inplace=True)

    return df 

def limpiar_formatear_columnas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    df['TIPO PERSONA'] = df['TIPO PERSONA'].str.upper()
    
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.strip().str.replace(',', '', regex=False)
        
    return df


def actualizar_prestadores(config: dict) -> None:
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
    
    engine = conectar_base_oracle()
    ruta_prestadores = seleccionar_archivo(
        titulo='Seleccione el archivo ZIP de prestadores',
        extension='.zip',
        tipos=[("ZIP files", "*.zip")],
    )
    

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
                    return
                else:
                    logging.info(f'Leyendo archivo {archivo}')
                    df_prestadores = pd.read_excel(archivo, sheet_name= config.get('hoja_archivo', 'E.P.S Sanitas'),
                                                   skiprows=2, dtype='str', usecols=config.get('columnas_prestadores'))
            except Exception as e:
                logging.error(f'Error al leer el archivo de prestadores: {e}, no se puede continuar')
                return

    ruta_archivo_regionales = config.get('archivo_regionales')
    if ruta_archivo_regionales is None:
        logging.error('La clave "archivo_regionales" no está configurada en el diccionario recibido o es None, no se puede continuar')
        return

    try:
        df_regionales = pd.read_csv(ruta_archivo_regionales, sep='|', dtype='str')
    except FileNotFoundError:
        logging.error(f'No se encontró el archivo de regionales en {ruta_archivo_regionales}, no se puede continuar')
        return
    except Exception as e:
        logging.error(f'Error al leer el archivo de regionales: {e}, no se puede continuar')
        return 


    df_regionales = (
        ajustar_regionales(df_regionales)
    )
    

    # Cruce de regionales con prestadores
    df_prestadores_cruzado = pd.merge(df_prestadores, df_regionales[[
                              'regional_adaptada', 'nombre_region']], left_on='REGIONAL', right_on='nombre_region', how='inner')
    df_prestadores_cruzado.drop(columns=['nombre_region', 'REGIONAL'], inplace=True)


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
        "ESTADO": "ESTADO_ACTUAL",
        "FECHA INICIO CONVENIO": "INICIO_CONTRATO",
        "FECHA FIN CONVENIO": "FIN_VIGENCIA",
        "COD_HABILITACION": "CODIGO DE HABILITACION2",
        "MUNICIPIO_AUTORIZADO": "MUNICIPIO AUTORIZADO",
        "CODIGO_SUCURSAL23": "CODIGO SUCURSAL23",
        "regional_adaptada": "REGIONAL"
    }        
    
    df_prestadores_procesado = (
        df_prestadores_cruzado
        .pipe(crear_columnas_nuevas)
        .pipe(limpiar_formatear_columnas)
        .rename(columns=column_renames)
        .rename(columns=str.upper)
    )
    
    nombre_tabla = config.get('tabla_oracle', 'tbl_ope_universo_prestadores')
    
    crear_tabla_longitudes(engine, nombre_tabla, df_prestadores_procesado, periodo)    
    
    # obtener universo prestadores
    df_universo_prestadores = obtener_datos_oracle(engine, nombre_tabla)
    
    df_universo_prestadores.rename(columns= str.upper, inplace=True)
    
    # poner marca df_universo_prestadores['Estado Actual'] = 'FINALIZADO' si no se encuentra en df_prestadores_procesado
    df_universo_prestadores.loc[~df_universo_prestadores['NIT'].isin(df_prestadores_procesado['NIT']), 'ESTADO_ACTUAL'] = 'FINALIZADO'
    
    df_universo_prestadores_finalizados = df_universo_prestadores.loc[df_universo_prestadores['ESTADO_ACTUAL'] == 'FINALIZADO']
    
    df_universo_prestadores_completos = pd.concat([df_prestadores_procesado, df_universo_prestadores_finalizados], ignore_index=True)
    
    df_universo_prestadores_completos.drop_duplicates(inplace=True)
    
    df_universo_prestadores_completos.to_excel(
        f'{nombre_tabla}_{periodo}.xlsx', index=False
    )

    actualizar_datos_oracle(engine, df_universo_prestadores_completos, nombre_tabla)
    
if __name__ == "__main__":
    config_prueba = {
        "tabla_oracle": "tbl_ope_universo_prestadores",
        "archivo_regionales": "G:\\.shortcut-targets-by-id\\1wT-pRaNOECz6KC5hndveeLOIGY381o4T\\Alteryx\\Proyectos\\154._Tableros_RIPS\\03.Salidas\\_Tb_Regiones_.csv"
    }

    actualizar_prestadores(config_prueba)