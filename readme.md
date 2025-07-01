# Proyecto de Actualización de Paramétricas UPC

Este proyecto automatiza la actualización de diversas tablas paramétricas en una base de datos Oracle a partir de archivos de entrada en formatos como .zip, .xlsx, y .csv.

## Características

- **Actualización de Múltiples Paramétricas**: Soporta la actualización de varias tablas como universo de prestadores, usuarios compensados, medicamentos TRT, NT únicos y vigencia UPC.
- **Interfaz de Línea de Comandos (CLI)**: Utiliza `inquirer` para ofrecer un menú interactivo que guía al usuario en la selección de la paramétrica a actualizar.
- **Configuración Centralizada**: La configuración de las paramétricas, incluyendo nombres de tablas, funciones de actualización y rutas de archivos, se gestiona desde un archivo `config.yaml`.
- **Manejo de Múltiples Formatos de Archivo**: Procesa archivos `.zip` (extrayendo su contenido en memoria), `.xlsx`, `.xlsb` y `.csv`.
- **Conexión a Base de Datos Oracle**: Utiliza `oracledb` y `SQLAlchemy` para interactuar con la base de datos.
- **Logging**: Registra los eventos importantes del proceso en un archivo de log.

## Instalación

1.  **Clonar el repositorio:**

    ```bash
    git clone <URL-DEL-REPOSITORIO>
    cd <NOMBRE-DEL-DIRECTORIO>
    ```

2.  **Crear un entorno virtual:**

    ```bash
    python -m venv env
    ```

3.  **Activar el entorno virtual:**

    -   En Windows:
        ```bash
        .\env\Scripts\activate
        ```
    -   En macOS/Linux:
        ```bash
        source env/bin/activate
        ```

4.  **Instalar las dependencias:**

    ```bash
    pip install -r requirements.txt
    ```

## Uso

1.  **Configurar `config.yaml`**:
    Asegúrate de que el archivo `config/config.yaml` esté correctamente configurado con los nombres de las tablas, las funciones y las rutas de los archivos necesarios para cada paramétrica.

2.  **Ejecutar el script principal**:

    ```bash
    python main.py
    ```

3.  **Seleccionar la paramétrica**:
    El script mostrará un menú con las paramétricas disponibles para actualizar. Selecciona la que desees y el script se encargará del resto.

## Scripts

-   `main.py`: El punto de entrada del programa. Carga la configuración, muestra el menú de selección y ejecuta la función de actualización correspondiente.
-   `log.py`: Configura el sistema de logging para registrar los eventos en un archivo.
-   `utils.py`: Contiene funciones de utilidad, como la lectura del archivo de configuración.
-   `database/operaciones_bdoracle.py`: Gestiona la conexión y las operaciones con la base de datos Oracle.

### Scripts de Actualización

Estos scripts se encuentran en el directorio `scripts/` y contienen la lógica específica para cada paramétrica:

-   `actualizar_compensados.py`: Actualiza las tablas de usuarios compensados (cotizantes y beneficiarios).
-   `actualizar_nt_unicos.py`: Actualiza la tabla de NT únicos.
-   `actualizar_trt.py`: Actualiza la tabla de medicamentos TRT.
-   `actualizar_universo_presadores.py`: Actualiza la tabla del universo de prestadores.
-   `actualizar_vigencia_upc.py`: Procesa un archivo `.zip` que contiene múltiples archivos `.xlsx` y `.xlsb` para actualizar las tablas de vigencia UPC (insumos, cie10, cups, reps).
