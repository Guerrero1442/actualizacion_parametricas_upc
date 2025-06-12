import logging
import pandas as pd
from database.operaciones_bdoracle import conectar_base_oracle, crear_tabla_longitudes
from utils import seleccionar_carpeta

# Constantes
# Archivos permitidos para cada grupo
beneficiario: list[str] = ['CORRECCION_BEN', 'AB4023']
cotizante: list[str] = ['CORRECCION_COT', 'AC4023']
liquidado: list[str] = ['EPSS05RESTITUCION', 'EPSS05LIQUIDACION']
# Objeto con nombre de tablas
tablas: dict[str, str] = {
    'beneficiario': 'TBL_OPE_COMPENSADO_BEN',
    'cotizante': 'TBL_OPE_COMPENSADO_COT',
    'liquidado': 'EPSS05LIQUIDACION'
}


def actualizar_compensados(config: dict) -> None:
    
    engine = conectar_base_oracle()
    
    ruta_compensados = seleccionar_carpeta(
        titulo="Seleccione la carpeta con los archivos de compensados"
    )
    
    # Preguntar periodo de actualizacion
    periodo = input("Ingrese el periodo de actualización (YYYYMM): ")
    logging.info(f'Actualizando compensados para el periodo {periodo}')
    
    # Cargar configuracion de archivos
    archivos_liquidados = config.get('archivos_liquidados', liquidado)
    archivos_cotizantes = config.get('archivos_cotizantes', cotizante)
    archivos_beneficiarios = config.get('archivos_beneficiarios', beneficiario)
    
    # Cargar configuraciones de tablas
    tabla_liquidado = config.get('tabla_liquidado', tablas['liquidado'])
    tabla_cotizante = config.get('tabla_cotizante', tablas['cotizante'])
    tabla_beneficiario = config.get('tabla_beneficiario', tablas['beneficiario'])
    
    
    # Procesamiento de archivos
    archivos = [archivo for archivo in ruta_compensados.iterdir(
    ) if archivo.is_file() and archivo.suffix == '.txt']

    df_liquidados: list[pd.DataFrame] = []

    for archivo in archivos:
        if archivos_liquidados[0] in archivo.name or archivos_liquidados[1] in archivo.name:
            logging.info(f'Procesando archivo {archivo.name}')
            df: pd.DataFrame = pd.read_csv(
                archivo, sep=',', dtype='str', encoding='latin1')
            df.drop(columns=['CAUSAL_DE_RESTITUCION',
                    'TIPO DE NOVEDAD'], inplace=True, errors='ignore')
            df_liquidados.append(df)

    df_liquidado = pd.concat(df_liquidados)

    crear_tabla_longitudes(engine, tabla_liquidado, df_liquidado, periodo)

    df_cotizantes: list[pd.DataFrame] = []

    for archivo in archivos:
        if archivos_cotizantes[0] in archivo.name or archivos_cotizantes[1] in archivo.name:
            logging.info(f'Procesando archivo {archivo.name}')
            df = pd.read_csv(archivo, sep=',', dtype='str', encoding='latin1')
            df.rename(columns={'DEPARTAMENTO': 'COD_DEP', 'MUNICIPIO': 'COD_MUN',
                      'EXONERACIÓN': 'EXONERACION', 'CENTRO_COSTO': 'CENTRO_DE_COSTO'}, inplace=True)
            df_cotizantes.append(df)

    df_cotizante = pd.concat(df_cotizantes)

    crear_tabla_longitudes(engine, tabla_cotizante, df_cotizante, periodo)

    df_beneficiarios: list[pd.DataFrame] = []

    for archivo in archivos:
        if archivos_beneficiarios[0] in archivo.name or archivos_beneficiarios[1] in archivo.name:
            logging.info(f'Procesando archivo {archivo.name}')
            df = pd.read_csv(archivo, sep=',', dtype='str', encoding='latin1')
            df.rename(columns={'DEPARTAMENTO': 'COD_DEP', 'MUNICIPIO': 'COD_MUN',
                      'EXONERACIÓN': 'EXONERACION'}, inplace=True)
            df_beneficiarios.append(df)

    df_beneficiario = pd.concat(df_beneficiarios)

    df_beneficiario.drop(columns = ['MES_PROCESO'], inplace=True)
    
    crear_tabla_longitudes(engine, tabla_beneficiario, df_beneficiario, periodo)

if __name__ == '__main__':
    config_prueba = {
        "tabla_beneficiario": "TBL_OPE_COMPENSADO_BEN",
        "tabla_cotizante": "TBL_OPE_COMPENSADO_COT",
        "tabla_liquidado": "EPSS05LIQUIDACION",
        "archivos_beneficiario": ["CORRECCION_BEN", "AB4023"],
        "archivos_cotizante": ["CORRECCION_COT", "AC4023"],
        "archivos_liquidado": ["EPSS05RESTITUCION", "EPSS05LIQUIDACION"]
    }
    actualizar_compensados(config_prueba)
