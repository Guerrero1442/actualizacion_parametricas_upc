import inquirer
from log import setup_logging
from scripts.actualizar_vigencia_upc import actualizar_vigencia_upc
from scripts.actualizar_compensados import actualizar_compensados
from scripts.actualizar_nt_unicos import actualizar_nt_unicos
from scripts.actualizar_universo_presadores import actualizar_prestadores
from scripts.actualizar_trt import actualizar_trt_medicamentos
from utils import leer_config

FUNCIONES_DISPONIBLES = {
    'actualizar_vigencia_upc': actualizar_vigencia_upc,
    'actualizar_compensados': actualizar_compensados,
    'actualizar_nt_unicos': actualizar_nt_unicos,
    'actualizar_universo_presadores': actualizar_prestadores,
    'actualizar_trt_medicamentos': actualizar_trt_medicamentos
}

def obtener_parametrica_seleccionada(config: dict) -> dict | None:
    if 'parametricas' not in config:
        print("No se encontró la configuración de paramétricas.")
        return
            
    parametricas_map = {p['nombre']: p for p in config['parametricas']}
    opciones_parametricas = list(parametricas_map.keys())
    
    if not opciones_parametricas:
        print("No existe ninguna parametrica para actualizar.")
        return
    
    pregunta = [
        inquirer.List(
            "seleccion",
            message="Seleccione que parametrica desea actualizar",
            choices=opciones_parametricas,
        )
    ]
    
    respuesta = inquirer.prompt(pregunta)
    
    if not respuesta:
        print("No se seleccionó ninguna opción. Saliendo...")
        return
    
    parametrica_seleccionada = respuesta['seleccion']

    return parametricas_map.get(parametrica_seleccionada)

def ejecutar_funcion_parametrica(parametrica: dict) -> None:
    if not parametrica:
        print("No se recibio la parametrica.")
        return
    
    nombre_parametrica = parametrica.get('nombre')
    nombre_funcion = parametrica.get('funcion')

    if not nombre_parametrica or not nombre_funcion:
        print("No se recibieron los datos necesarios para ejecutar la función.")
        return

    funcion_a_ejecutar = FUNCIONES_DISPONIBLES.get(nombre_funcion)
    
    if callable(funcion_a_ejecutar):
        print(f'Actualizando {nombre_parametrica}...')
        funcion_a_ejecutar(parametrica)
        print(f'Actualización de {nombre_parametrica} completada.')
    else:
        print(f'No se encontró la función para {funcion_a_ejecutar}.')


def main():
    config = leer_config()

    if not config:
        print("Error al cargar la configuración.")
        return
    
    parametrica_seleccionada = obtener_parametrica_seleccionada(config)
    
    if parametrica_seleccionada:
        ejecutar_funcion_parametrica(parametrica_seleccionada)
    else:
        print("No se seleccionó ninguna parametrica para actualizar.")
        return

if __name__ == '__main__':
    setup_logging()
    main()