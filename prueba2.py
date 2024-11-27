from est_ora import oracle
import pandas as pd
from est_bq import bigq
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime

# Configuración inicial
inicio = datetime.datetime.now()
usuario = "SINTERFACE"
contraseña = "SF5590X"
ip_server_name = "10.20.11.20/SPT01"
file_json = r"D:\python\credenciales biq query\vta_linea_sistemas-bi-438c564c407c.json"

# Parámetros de carga
batch_size = 1000000  # Tamaño del lote
num_threads = 10  # Número de hilos para la carga en paralelo

project = "sistemas-bi"
dataset = "SPSA_STAGE"
table = "tmp_invbalee"

# Inicializar conexiones
obj_bq = bigq(file_json)
obj_bq.clear_table(project, dataset, table)
obj_ora = oracle(usuario, contraseña, ip_server_name)
obj_ora.inicializar_oracle()
obj_cnx = obj_ora.conectar_oracle()

# Función para obtener un rango de filas usando ROWNUM
def fetch_data_chunk(start_row, end_row):
    query = f"""
    SELECT * FROM (
        SELECT a.*, ROWNUM AS rnum 
        FROM INVBALEE a 
        WHERE ROWNUM <= {end_row}
    ) 
    WHERE rnum > {start_row}
    """
    cursor = obj_cnx.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    df = pd.DataFrame(rows, columns=[col[0] for col in cursor.description])
    return df.astype(str)

# Crear el ThreadPoolExecutor para ejecución paralela
executor = ThreadPoolExecutor(max_workers=num_threads)
futures = []

try:
    # Dividir en bloques y ejecutar cada consulta en paralelo
    for start_row in range(0, 21000000, batch_size):  # Asumiendo 20 millones de filas
        end_row = start_row + batch_size
        future = executor.submit(fetch_data_chunk, start_row, end_row)
        futures.append(future)
    
    # Cargar cada bloque en BigQuery cuando esté listo
    for future in as_completed(futures):
        try:
            df = future.result()
            obj_bq.ins_table(project, dataset, table, df)
        except Exception as e:
            print(f"Error en la carga de un lote: {e}")

except Exception as e:
    print(f"Error durante la extracción de los datos: {e}")

finally:
    executor.shutdown(wait=True)
    obj_ora.cerrar()

# Calcular el tiempo total
fin = datetime.datetime.now()
elapsed_time = fin - inicio
elapsed_minutes = elapsed_time.total_seconds() / 60
print(f"Tiempo de procesamiento: {elapsed_minutes:.2f} minutos")
