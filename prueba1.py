from est_ora import oracle
import pandas as pd
from est_bq import bigq
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime

inicio=datetime.datetime.now()
usuario= "SINTERFACE"
contraseña= "SF5590X"
ip_server_name= "10.20.11.20/SPT01"

file_json = r"D:\python\credenciales biq query\vta_linea_sistemas-bi-438c564c407c.json"

# Parámetros de carga
batch_size = 3000000  # Tamaño del lote
num_threads = 10  # Número de hilos para la carga en paralelo

project="sistemas-bi"
dataset="SPSA_STAGE"
table="tmp_invbalee"

obj_bq=bigq(file_json)
obj_bq.clear_table(project,dataset,table)

obj_ora=oracle(usuario,contraseña,ip_server_name)
obj_ora.inicializar_oracle()
obj_cnx=obj_ora.conectar_oracle()

query=f"""
select a.* from INVBALEE a
--where a.org_lvl_child=1816
"""
obj_cursor=obj_cnx.cursor()

# Crear el ThreadPoolExecutor
executor = ThreadPoolExecutor(max_workers=num_threads)
futures = []

try:
    obj_cursor.execute(query)
    while True:
        rows=obj_cursor.fetchmany(batch_size)
        if not rows:
            break
        df=pd.DataFrame(rows,columns=[col[0] for col in obj_cursor.description])
        df=df.astype(str)
        #obj_bq.ins_table(project,dataset,table,df)

        # Enviar el trabajo al hilo
        future = executor.submit(obj_bq.ins_table, project,dataset,table,df)
        futures.append(future)
    
        # Esperar a que todos los hilos terminen
        for future in as_completed(futures):
            try:
                future.result()  # Obtiene el resultado para manejar posibles errores
            except Exception as e:
                print(f"Error en la carga de un lote: {e}")

except  Exception as e:
    print(f"error durante la extracción de los datos, {e}")
finally:
    executor.shutdown(wait=True)
    obj_cursor.close()
    obj_ora.cerrar()

fin=datetime.datetime.now()
elapsed_time = fin - inicio
elapsed_minutes = elapsed_time.total_seconds() / 60  # Convertir a minutos
print(f"Tiempo de procesamiento: {elapsed_minutes:.2f} minutos")