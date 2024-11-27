import os
import pandas as pd
from google.cloud import bigquery


def convertir_a_string_sin_cambiar_numeros(df):
    # Función para evitar que se agregue ".0" a los enteros usando map
    return df.astype(str).apply(lambda col: col.map(lambda x: str(int(float(x))) if x.replace('.', '', 1).isdigit() and float(x).is_integer() else x))


def validar_archivo_y_cargar_a_bigquery(ruta_archivo, dataset_id, tabla_id,file_json):
    # 1. Validar si el archivo existe
    if not os.path.exists(ruta_archivo):
        raise FileNotFoundError(f"El archivo {ruta_archivo} no se encontró.")
    print(f"El archivo {ruta_archivo} existe.")
    
    # 2. Cargar el archivo Excel en un DataFrame
    try:
        df = pd.read_excel(ruta_archivo)
        df = df.astype(str)
        print("Todas las columnas convertidas a tipo string.")
        df = convertir_a_string_sin_cambiar_numeros(df)
        print(f"Archivo Excel cargado correctamente. Primeras filas:\n{df.head()}")
    except Exception as e:
        raise Exception(f"Error al leer el archivo Excel: {str(e)}")

    # 3. Crear el cliente de BigQuery
    client = bigquery.Client.from_service_account_json(file_json)

    # 4. Definir el ID completo de la tabla
    tabla_id_completo = f"{client.project}.{dataset_id}.{tabla_id}"
    
    # 5. Eliminar la tabla si existe
    try:
        client.delete_table(tabla_id_completo, not_found_ok=True)
        print(f"Tabla {tabla_id_completo} eliminada (si existía).")
    except Exception as e:
        raise Exception(f"Error al eliminar la tabla en BigQuery: {str(e)}")

    # 6. Definir el schema a partir de las columnas del DataFrame (tipo STRING para todas las columnas)
    schema = [bigquery.SchemaField(col, 'STRING') for col in df.columns]

    # 7. Crear la tabla en BigQuery
    tabla = bigquery.Table(tabla_id_completo, schema=schema)
    
    try:
        client.create_table(tabla)
        print(f"Tabla {tabla_id_completo} creada en BigQuery con el schema basado en el archivo Excel.")
    except Exception as e:
        raise Exception(f"Error al crear la tabla en BigQuery: {str(e)}")

    # 8. Cargar el DataFrame en la tabla de BigQuery
    try:
        job = client.load_table_from_dataframe(df, tabla_id_completo)
        job.result()  # Esperar a que el job se complete
        print(f"Datos cargados exitosamente en la tabla {tabla_id_completo}.")
    except Exception as e:
        raise Exception(f"Error al cargar los datos en BigQuery: {str(e)}")

# Parámetros
file_json = r"D:\python\credenciales biq query\sistemas-bi-7a46b3894448.json"
ruta_archivo = r'D:\BI\objetivos\garantia\archivo_input\trx_cargar_gar_faltante_agosto2024.xlsx'  # Ruta del archivo Excel
dataset_id = 'SPSA'               # ID del dataset en BigQuery
tabla_id = 'tmp_ticket_gar_faltante'                   # ID de la tabla en BigQuery

# Ejecutar la función
validar_archivo_y_cargar_a_bigquery(ruta_archivo, dataset_id, tabla_id,file_json)
