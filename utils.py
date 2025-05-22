from pathlib import Path
from tkinter import Tk, filedialog
import yaml
from config import OPCIONES_PERMITIDAS


def seleccionar_archivo(titulo: str, directorio_inicial: Path, extension: str, tipos: list[tuple[str,str]]) -> Path:
    root = Tk()
    
    root.withdraw()
    
    ruta = filedialog.askopenfilename(title=titulo, initialdir=directorio_inicial, defaultextension=extension, filetypes=tipos)
    
    root.destroy()
    
    if ruta:
        return Path(ruta)
    else:
        raise FileNotFoundError("No se seleccionó ningún archivo")
    
def seleccionar_carpeta(titulo: str, directorio_inicial: Path) -> Path:
    root = Tk()
    
    root.withdraw()
    
    ruta = Path(filedialog.askdirectory(title=titulo, initialdir=directorio_inicial))
    
    root.destroy()
    
    if ruta:
        return ruta
    else:
        raise FileNotFoundError("No se seleccionó ninguna carpeta")
    
def eligir_opcion() -> str:
    print("Seleccione que parametrica desea actualizar")
    while True:
        opcion = input("Ingrese el número de la parametrica a actualizar: ")
        if opcion in OPCIONES_PERMITIDAS:
            return opcion
        else:
            print("Por favor ingrese una opcion valida o cancele la operación")