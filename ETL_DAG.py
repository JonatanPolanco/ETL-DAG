from airflow import DAG
from airflow.operators.python import PythonOperator 
from datetime import datetime, timedelta
import os
import logging
from dotenv import load_dotenv
import pandas as pd
import pycountry
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas

# Se crearon datos de prueba con faker para probar el script
# Se implementa codigo DRY y mantenible
# Se puede crear un contenedor Docker para ejecutar el script en cualquier entorno
# La iniciativa de validar la calidad y hacer tests sobre los datos tambien se puede desarrollar en dbt. 
# Junto con tests de integridad referencial (relaciones entre el modelo de datos)

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Constantes
TARGET_TABLES = {
    'sales': 'FACT_TABLE',
    'products': 'PRODUCTS',
    'customers': 'CUSTOMERS'
}

def get_csv_paths():
    """Retorna los paths de los CSV desde variables de entorno"""
    # usamos carga de archivos almacenados localmente para probar con menos dependencias. 
    # Para una salida se puede cambiar a la ruta de los archivos en el bucket de S3 o cloud storage

    return {
        'sales': os.getenv('CSV_SALES_PATH'),
        'products': os.getenv('CSV_PRODUCTS_PATH'),
        'customers': os.getenv('CSV_CUSTOMERS_PATH')
    }

def load_env_vars():
    """Carga y valida variables de entorno requeridas"""
    #se utiliza dotenv para la carga de variables de entorno
    #en caso de usar un entorno cloud se pueden cargar las variables directamente en el entorno o un key manager o docker compose

    required_vars = ['CSV_SALES_PATH','CSV_PRODUCTS_PATH','CSV_CUSTOMERS_PATH','SNOWFLAKE_USER','SNOWFLAKE_PASSWORD', 'SNOWFLAKE_ACCOUNT',
                    'SNOWFLAKE_WAREHOUSE', 'SNOWFLAKE_DATABASE','SNOWFLAKE_SCHEMA', 'SNOWFLAKE_ROLE']
    
    load_dotenv('.env')
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        raise ValueError(f'Variables faltantes en .env: {", ".join(missing_vars)}')
    
    logger.info("Variables de entorno validadas correctamente")

def validate_files():
    """Valida existencia de los archivos CSV"""
    csv_paths = get_csv_paths()
    
    for file_type, path in csv_paths.items():
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Archivo {file_type} no encontrado: {path}")
    
    logger.info("Validación de archivos exitosa")

def get_connection():
    """Crea conexión a Snowflake"""
    #Se manejan errores en la conexion a snowflake

    try:
        conn = connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE")
        )
        logger.info("Conexión a Snowflake establecida")
        return conn
    except Exception as e:
        logger.error(f"Error de conexión: {str(e)}")
        raise

def validate_data(df, file_type):
    """validaciones específicas para cada tipo de dato"""
    #usamos pandas teniendo en cuenta que no trabamos con grandes volumenes de datos
    #de tener grandes volumenes de datos se podria usar polars o pyspark para mejorar el rendimiento

    #se validan valores criticos como fechas, valores nulos, valores negativos, etc esto en pro de la calidad de los datos

    validations = {
        'sales': {
            'required_cols': ['TRANSACTION_ID', 'TRANSACTION_DATE', 'CUSTOMER_ID', 'PRODUCT_ID', 'AMOUNT'],
            'checks': [
                (lambda d: d['AMOUNT'] > 0, 'Cantidades deben ser positivas'),
                (lambda d: pd.to_datetime(d['TRANSACTION_DATE'], errors='coerce').notna(),
                 'Fechas inválidas')
            ]
        },
        'products': {
            'required_cols': ['PRODUCT_ID', 'PRODUCT_NAME', 'CATEGORY', 'PRICE'],
            'checks': [
                (lambda d: d['PRICE'] >= 0, 'Precios no pueden ser negativos')
            ]
        },
        'customers': {
            'required_cols': ['CUSTOMER_ID', 'NAME', 'EMAIL', 'COUNTRY'],
            'checks': [
                (lambda d: d['EMAIL'].str.contains(r'^[\w\.-]+@[\w\.-]+\.\w+$', na=False),
                 'Emails inválidos')
            ]
        }
    }
    
    config = validations[file_type]

    # Validar columnas requeridas
    # calculamos la diferencia entre las columnas requeridas y las columnas presentes en el dataframe
    
    missing_cols = set(config['required_cols']) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Columnas faltantes en {file_type}: {', '.join(missing_cols)}")

    # Ejecutar validaciones
    # Dado el caso en el que algun registro no cumpla con las validaciones se almacena en un array de errores
    
    errors = []
    for check, message in config['checks']:
        invalid = df[~check(df)]
        if not invalid.empty:
            errors.append(f"{message} ({len(invalid)} registros)")
    
    if errors:
        raise ValueError(f"Errores en {file_type}:\n- " + "\n- ".join(errors))
    
    logger.info(f"Datos de {file_type} validados correctamente")

def transform_country(country_name):
    """Transforma nombres de países a ISO alpha-3 con manejo de errores"""
    try:
        country = pycountry.countries.search_fuzzy(country_name)[0]
        return country.alpha_3
    except LookupError:
        logger.warning(f"País no reconocido: {country_name}")
        return None

def load_data():
    """Proceso completo de transformación y carga de datos"""
    try:
        csv_paths = get_csv_paths()

        # Leer datos
        #usamos pandas teniendo en cuenta que no trabamos con grandes volumenes de datos
        #de tener grandes volumenes de datos se podria usar polars o pyspark para mejorar el rendimiento
        
        sales_df = pd.read_csv(csv_paths['sales'])
        products_df = pd.read_csv(csv_paths['products'])
        customers_df = pd.read_csv(csv_paths['customers'])

        # Transformamos nombres de campos para mejor lectura
        sales_df = sales_df.rename(columns={
            'TransactionID': 'TRANSACTION_ID', 
            'Date': 'TRANSACTION_DATE', 
            'CustomerID': 'CUSTOMER_ID',
            'ProductID': 'PRODUCT_ID',
            'Amount': 'AMOUNT',
        })

        products_df = products_df.rename(columns={
            'ProductID': 'PRODUCT_ID', 
            'ProductName': 'PRODUCT_NAME', 
            'Category': 'CATEGORY',
            'Price': 'PRICE'
        })

        customers_df = customers_df.rename(columns={
            'CustomerID': 'CUSTOMER_ID', 
            'Name': 'NAME', 
            'Email': 'EMAIL',
            'Country': 'COUNTRY'
        })

        # Transformar países
        # se retorna error en caso de que no se pueda transformar el pais. Se almacenan los paises no validos en un array
        # se llama la funcion transform_country para hacer la transformacion de los paises
        
        customers_df['COUNTRY'] = customers_df['COUNTRY'].apply(transform_country)

        # Validar si hay valores nulos después de la transformación
        if customers_df['COUNTRY'].isnull().any():
            invalid = customers_df[customers_df['COUNTRY'].isnull()]['COUNTRY'].unique()
            if invalid.any():
                raise ValueError(f"Países no válidos: {', '.join([str(country) for country in invalid if country is not None])}")

        # Validar datos
        validate_data(sales_df, 'sales')
        validate_data(products_df, 'products')
        validate_data(customers_df, 'customers')

        # Cargar en orden adecuado
        with get_connection() as conn:
            cursor = conn.cursor()

            # Truncar las tablas antes de insertar datos
            for table in TARGET_TABLES.values():
                cursor.execute(f"TRUNCATE TABLE {table}")
                logger.info(f"Tabla {table} truncada")

            def safe_load(df, table):
                success, _, num_rows, _ = write_pandas(
                    conn,
                    df,
                    table,
                    quote_identifiers=False,
                    auto_create_table=True  # Crear la tabla si no existe
                )
                if not success:
                    raise RuntimeError(f"Falló carga en {table}")
                logger.info(f"Cargadas {num_rows} filas en {table}")
            
            safe_load(products_df, TARGET_TABLES['products'])
            safe_load(customers_df, TARGET_TABLES['customers'])
            safe_load(sales_df, TARGET_TABLES['sales'])

    except pd.errors.ParserError as e:
        logger.error(f"Error de formato en CSV: {str(e)}")
        raise
    except ValueError as e:
        logger.error(f"Error de validación: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error inesperado en load_data: {str(e)}")
        raise

# Definir el DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_pipeline',
    default_args=default_args,
    description='Un pipeline de datos para cargar y transformar datos en Snowflake',
    schedule_interval=timedelta(days=1), # se define para que corra diario
)

# Definir las tareas
t1 = PythonOperator(
    task_id='load_env_vars',
    python_callable=load_env_vars,
    dag=dag,
)

t2 = PythonOperator(
    task_id='validate_files',
    python_callable=validate_files,
    dag=dag,
)

t3 = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# Definir el orden de las tareas
t1 >> t2 >> t3