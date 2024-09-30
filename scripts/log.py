import logging
import os


def setup_logging(log_file=r"D:\Keralty scripts\automatizaciones_python\actualizacion_parametricas_upc\scripts\logs\actualizacion_parametricas.log"):

    # asegurarse de que el directorio logs exista
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logging.basicConfig(
        filename=log_file,
        encoding="utf-8",
        filemode="a",
        format="%(asctime)s - %(levelname)s - %(message)s",
        level=logging.DEBUG,
        datefmt="%Y-%m-%d %H:%M",
    )
