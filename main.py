from log import setup_logging
from database.operaciones_bdoracle import conectar_base_oracle
from scripts.actualizar_vigencia_upc import actualizar_vigencia_upc
from scripts.actualizar_compensados import actualizar_compensados
from scripts.actualizar_nt_unicos import actualizar_nt_unicos
from scripts.actualizar_universo_presadores import actualizar_prestadores
from scripts.actualizar_trt import actualizar_trt_medicamentos
from config import ZIP_EXTENSION, EXCEL_EXTENSION, NOMBRE_TABLA_TRT_ORACLE, INITIAL_DIRS
from utils import eligir_opcion, seleccionar_archivo, seleccionar_carpeta


def main():
    print("Seleccione el numero de parametrica que desea actualizar")
    print("""
          1. Actualizar Vigencia UPC
          2. Actualizar Compensados
          3. Actualizar NT UNICOS
          4. Actualizar Universo Prestadores
          5. Actualizar TRT Medicamentos
          6. Actualizar todas las parametricas
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
        version_tabla = input('Ingrese el año y la versión de la tabla TRT (por ejemplo, 2024_41): ') 
        nombre_tabla = f'{NOMBRE_TABLA_TRT_ORACLE}{version_tabla}'.lower()
        actualizar_trt_medicamentos(engine, seleccionar_archivo(titulo="Seleccione el archivo de TRT Medicamentos",
                                    directorio_inicial=INITIAL_DIRS['trt_medicamentos'], extension=EXCEL_EXTENSION, tipos=[("Excel files", "*.xlsx")]), nombre_tabla)
    
    elif opcion == '6':
        actualizar_vigencia_upc(engine, seleccionar_archivo(titulo="Seleccione el archivo ZIP de la vigencia UPC",
                                directorio_inicial=INITIAL_DIRS['vigencia'], extension=ZIP_EXTENSION, tipos=[("ZIP files", "*.zip")]))
        actualizar_compensados(engine, seleccionar_carpeta(
            titulo="Seleccione la carpeta con los archivos de compensados", directorio_inicial=INITIAL_DIRS['compensados']))
        actualizar_nt_unicos(engine, seleccionar_archivo(titulo="Seleccione el archivo de NT UNICOS",
                             directorio_inicial=INITIAL_DIRS['nt_unicos'], extension=EXCEL_EXTENSION, tipos=[("Excel files", "*.xlsx")]))
        actualizar_prestadores(engine, seleccionar_archivo(titulo="Seleccione el archivo ZIP de prestadores",
                               directorio_inicial=INITIAL_DIRS['prestadores'], extension=ZIP_EXTENSION, tipos=[("ZIP files", "*.zip")]))


if __name__ == '__main__':
    setup_logging()
    main()
