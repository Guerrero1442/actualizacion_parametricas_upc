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

def main():
    config = leer_config()
    
    print(config['parametricas'])
    
    opciones_parametricas = [x['nombre'] for x in config['parametricas']]
    print(opciones_parametricas)

    parametricas_actualizar = [
        inquirer.List(
            "seleccion",
            message="Seleccione que parametrica desea actualizar",
            choices=opciones_parametricas,
        )
    ]
    
    respuesta = inquirer.prompt(parametricas_actualizar)
    
    if not respuesta:
        print("No se seleccionó ninguna opción. Saliendo...")
        return
    
    parametrica_seleccionada = respuesta['seleccion']
    
    nombre_funcion_encontrada = False
    nombre_funcion = ''
    
    for parametrica in config['parametricas']:
        if parametrica['nombre'] == parametrica_seleccionada:
            nombre_funcion = parametrica['funcion']
            nombre_funcion_encontrada = True
            break

    
    if nombre_funcion_encontrada:
        funcion_a_ejecutar = FUNCIONES_DISPONIBLES.get(nombre_funcion)
    
        if funcion_a_ejecutar:
            print(f'Actualizando {parametrica_seleccionada}...')
            funcion_a_ejecutar()
        else:
            print(f'No se encontró la función para {parametrica_seleccionada}.')
            
    else:
        print(f'La opcion {parametrica_seleccionada} no es válida.')
    
if __name__ == '__main__':
    setup_logging()
    main()