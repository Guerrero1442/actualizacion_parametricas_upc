import logging
import os
from pathlib import Path

LOG_FILE = Path(__file__).resolve().parent / 'logs' / 'actualizacion_parametricas.log'

def setup_logging():
    # asegurarse de que el directorio logs exista
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

    logging.basicConfig(
        filename=LOG_FILE,
        encoding="utf-8",
        filemode="a",
        format="%(asctime)s - %(levelname)s - %(message)s",
        level=logging.DEBUG,
        datefmt="%Y-%m-%d %H:%M",
    )
