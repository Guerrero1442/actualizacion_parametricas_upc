from tkinter import filedialog
from sqlalchemy import create_engine
from operaciones_bdoracle import conectar_base_oracle
from actualizar_vigencia_upc import actualizar_vigencia_upc

# Contastantes
ZIP_EXTENSION = '.zip'


def main():
    engine: create_engine = conectar_base_oracle()

    zip_path: str = filedialog.askopenfilename(
        initialdir=r'G:\Mi unidad\Mis_Actividades\Actualizacion Parametricas UPC\VIGENCIA_MINSALUD')

    if zip_path.endswith(ZIP_EXTENSION):
        actualizar_vigencia_upc(engine, zip_path)
    else:
        print('El archivo seleccionado no es un archivo .zip')
        print('Por favor, seleccione un archivo .zip')


if __name__ == '__main__':
    main()
