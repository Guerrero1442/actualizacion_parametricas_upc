from pathlib import Path
from tkinter import Tk, filedialog
import yaml


def seleccionar_archivo(
    titulo: str,
    extension: str, 
    tipos: list[tuple[str, str]], 
    directorio_inicial: Path = Path('G:\\Mi unidad\\Mis Actividades\\Actualizacion Parametricas UPC')
) -> Path:
    root = Tk()
    root.withdraw()
    root.attributes('-topmost', True)
    
    ruta = filedialog.askopenfilename(title=titulo, initialdir=directorio_inicial, defaultextension=extension, filetypes=tipos)
    
    root.destroy()
    
    if ruta:
        return Path(ruta)
    else:
        raise FileNotFoundError("No se seleccionó ningún archivo")
    
def seleccionar_carpeta(
    titulo: str,
    directorio_inicial: Path = Path('G:\\Mi unidad\\Mis Actividades\\Actualizacion Parametricas UPC')
) -> Path:
    root = Tk()
    root.withdraw()
    root.attributes('-topmost', True)

    ruta = Path(filedialog.askdirectory(title=titulo, initialdir=directorio_inicial))
    
    root.destroy()
    
    if ruta:
        return ruta
    else:
        raise FileNotFoundError("No se seleccionó ninguna carpeta")
            
def leer_config(ruta_config: str = 'config\\config.yaml') -> dict:
    """
    Lee la configuración desde un archivo YAML.
    
    Args:
        ruta_config (str): Ruta al archivo de configuración YAML.
        
    Returns:
        dict: Diccionario con la configuración leída.
    """
    with open(ruta_config, 'r') as file:
        config = yaml.safe_load(file)
    return config