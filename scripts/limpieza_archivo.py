import pandas as pd
from unidecode import unidecode


def sacar_longitudes_max_columnas(df: pd.DataFrame) -> dict:
    return {col: max(1, df[col].astype(str).str.len().max()) for col in df.columns}

def quitar_espacios(df:pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        try:
            df[col] = df[col].str.strip()
        except AttributeError:
            print(f'No se pudo convertir la columna {col} a texto')
    return df
            
def limpiar_texto_columnas(df: pd.DataFrame) -> list[str]:
    # Reemplazar espacios dobles de las columnas
    df.columns = df.columns.str.replace(r'\s{2,}', ' ', regex=True).str.strip()
    
    df.columns = df.columns.str.replace(' ', '_')
    
    # Agregar guion bajo antes de paréntesis de apertura si no está precedido por uno
    df.columns = df.columns.str.replace(r'([^\_])\(', r'\1_(', regex=True)
    
    # Agregar guion bajo antes de paréntesis de cierre si no está precedido por uno y está seguido de un carácter
    df.columns = df.columns.str.replace(r'([^\_])\)(?=\w)', r'\1_)', regex=True)
    
    # Eliminar los paréntesis
    df.columns = df.columns.str.replace('(', '')
    df.columns = df.columns.str.replace(')', '')
    
    # Quitar acentos de las columnas
    df.columns = [quitar_acentos(col).upper() for col in df.columns]
    
    columnas = df.columns.tolist()
    
    return columnas

    
            
def quitar_acentos(texto: str) -> str:
    return unidecode(texto)