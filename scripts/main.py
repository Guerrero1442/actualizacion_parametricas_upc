from pathlib import Path
from tkinter import filedialog
from sqlalchemy import create_engine
from operaciones_bdoracle import conectar_base_oracle
from actualizar_vigencia_upc import actualizar_vigencia_upc
from actualizar_compensados import actualizar_compensados
from actualizar_nt_unicos import actualizar_nt_unicos
from actualizar_universo_presadores import actualizar_prestadores

# Contastantes
ZIP_EXTENSION = '.zip'
EXCEL_EXTENSION = '.xlsx'
INITIAL_DIRS = {
    'vigencia': r'G:\Mi unidad\Mis Actividades\Actualizacion Parametricas UPC\VIGENCIA_MINSALUD',
    'compensados': r'G:\.shortcut-targets-by-id\1buUUJ2naBFTn-E10CXNd8elJ6YQOlWCR\00_BASES_COMPENSACION_2024',
    'nt_unicos': r'G:\Mi unidad\Mis Actividades\Actualizacion Parametricas UPC\NT_UNICOS',
    'prestadores': r'G:\Mi unidad\Mis_Actividades\Actualizacion Parametricas UPC\PRESTADORES'
}
opciones_permitidas: list[str] = ['1', '2', '3', '4', '5']


def main():
    print("Seleccione el numero de parametrica que desea actualizar")
    print("""
          1. Actualizar Vigencia UPC
          2. Actualizar Compensados
          3. Actualizar NT UNICOS
          4. Actualizar Universo Prestadores
          5. Actualizar todas las parametricas
          """)

    opcion = eligir_opcion()

    engine = conectar_base_oracle()

    if opcion == '1':
        actualizar_vigencia_upc(engine, seleccionar_archivo(titulo="Seleccione el archivo ZIP de la vigencia UPC",
                                directorio_inicial=INITIAL_DIRS['vigencia'], extension=ZIP_EXTENSION, tipos=[("ZIP files", "*.zip")]))

    elif opcion == '2':
        actualizar_compensados(engine, seleccionar_carpeta(
            titulo="Seleccione la carpeta con los archivos de compensados", directorio_inicial=INITIAL_DIRS['compensados']))
    elif opcion == '3':
        actualizar_nt_unicos(engine, seleccionar_archivo(titulo="Seleccione el archivo de NT UNICOS",
                             directorio_inicial=INITIAL_DIRS['nt_unicos'], extension=EXCEL_EXTENSION, tipos=[("Excel files", "*.xlsx")]))
    elif opcion == '4':
        actualizar_prestadores(engine, seleccionar_archivo(titulo="Seleccione el archivo ZIP de prestadores",
                               directorio_inicial=INITIAL_DIRS['prestadores'], extension=ZIP_EXTENSION, tipos=[("ZIP files", "*.zip")]))
    elif opcion == '5':
        actualizar_vigencia_upc(engine, seleccionar_archivo(titulo="Seleccione el archivo ZIP de la vigencia UPC",
                                directorio_inicial=INITIAL_DIRS['vigencia'], extension=ZIP_EXTENSION, tipos=[("ZIP files", "*.zip")]))
        actualizar_compensados(engine, seleccionar_carpeta(
            titulo="Seleccione la carpeta con los archivos de compensados", directorio_inicial=INITIAL_DIRS['compensados']))
        actualizar_nt_unicos(engine, seleccionar_archivo(titulo="Seleccione el archivo de NT UNICOS",
                             directorio_inicial=INITIAL_DIRS['nt_unicos'], extension=EXCEL_EXTENSION, tipos=[("Excel files", "*.xlsx")]))
        actualizar_prestadores(engine, seleccionar_archivo(titulo="Seleccione el archivo ZIP de prestadores",
                               directorio_inicial=INITIAL_DIRS['prestadores'], extension=ZIP_EXTENSION, tipos=[("ZIP files", "*.zip")]))


def eligir_opcion() -> str:
    print("Seleccione que parametrica desea actualizar")
    while True:
        opcion = input("Ingrese el número de la parametrica a actualizar: ")
        if opcion in opciones_permitidas:
            return opcion
        else:
            print("Por favor ingrese una opcion valida o cancele la operación")


def seleccionar_archivo(titulo: str, directorio_inicial: str, extension: str, tipos: list[tuple[str, str]]) -> Path:
    while True:
        ruta = filedialog.askopenfilename(title=titulo, initialdir=directorio_inicial,
                                          defaultextension=extension, filetypes=tipos)
        if ruta:
            return Path(ruta)
        else:
            print("Por favor seleccione un archivo valido o cancele la operación")


def seleccionar_carpeta(titulo: str, directorio_inicial: str) -> Path:
    while True:
        ruta = Path(filedialog.askdirectory(
            title=titulo, initialdir=directorio_inicial))
        if ruta:
            return Path(ruta)
        else:
            print("Por favor seleccione una carpeta valida o cancele la operación")


if __name__ == '__main__':
    main()
