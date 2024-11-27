import pysftp
import fnmatch
import pandas as pd
from io import StringIO
from unidecode import unidecode

import est_bq as bqy
from datetime import datetime,timedelta
import pytz
import tempfile

ip = "34.134.251.183"
us = "qa_spsa"
pw = "R&XfDeDXQ9se!2"

file_json = r"D:\python\credenciales biq query\vta_linea_sistemas-bi-438c564c407c.json"

# Obtener la fecha actual
zona_horaria_lima = pytz.timezone('America/Lima')
fecha_actual = datetime.now(zona_horaria_lima)
fecha_formateada = fecha_actual.strftime("%m_%d_%Y")
fecha_formateada = fecha_formateada.replace("0", "", 1)  # Solo reemplaza el primer cero en el mes
fecha_anterior=fecha_actual-timedelta(days=1)
fecha_anterior_formateada=datetime.strftime(fecha_anterior,"%Y-%m-%d")

q_val_data_allocation=f"""
select distinct a.fecha
from
`sistemas-bi.SPSA.fact_wms_allocation` a
where a.fecha='{fecha_anterior_formateada}'
"""

q_data_allocation=f"""
select a.*
from
`sistemas-bi.SPSA.fact_wms_allocation` a
where a.fecha='{fecha_anterior_formateada}'
"""

remote_path_o = r"/spsa_data/scheduler/rpt_log_input/"
patron_archivo_input = f"*rpt_log_JFON_sms_rp_allocation_mod_qa_{fecha_formateada}_*.csv"  # Ajusta este patrón según lo que necesites

print(patron_archivo_input)

remote_path_d=r"/spsa_data/scheduler/rpt_log_output/"
patron_archivo_salida=f"bi_sms_rp_allocation_{fecha_anterior_formateada}.csv"
remote_file_path_d=remote_path_d+patron_archivo_salida

remote_path_o_procesado = r"/spsa_data/scheduler/rpt_log_input/procesado/"

cnopts = pysftp.CnOpts()
cnopts.hostkeys = None 

with pysftp.Connection(host=ip, username=us, password=pw, cnopts=cnopts) as sftp:
    # Verificar si la ruta existe
    val_exists = sftp.exists(remote_path_o)
    if val_exists:
        print(f'La ruta {remote_path_o} sí existe')

        # Listar los archivos en el directorio
        archivos = sftp.listdir(remote_path_o)
        
        print(archivos)
        # Filtrar los archivos que coinciden con el patrón
        archivos_filtrados = fnmatch.filter(archivos, patron_archivo_input)
        
        if archivos_filtrados:
            archivo_a_descargar = archivos_filtrados[0]  # Si hay varios, tomamos el primero
            ruta_completa = f"{remote_path_o}/{archivo_a_descargar}"
            print(f'Descargando archivo: {ruta_completa}')
            
            with sftp.open(ruta_completa, 'r') as file:
                file_content = file.read().decode('utf-8') 
                data = StringIO(file_content)
                df = pd.read_csv(data)
                df.columns = [unidecode(col).lower().replace(' ', '_') for col in df.columns]
                df = df.astype(str)

                print('Archivo descargado y cargado en un DataFrame.')

                project="sistemas-bi"
                dataset="SPSA_STAGE"
                table="tmp_wms_allocation"

                obj_bq=bqy.bigq(file_json)
                obj_bq.clear_table(project,dataset,table)
                obj_bq.ins_table(project,dataset,table,df)

                obj_bq.exec_query_sin_param("call `sistemas-bi.SPSA.ins_fact_wms_allocation`()")
                print("Se terminó de procesar el sp call `sistemas-bi.SPSA.ins_fact_wms_allocation`()")
                
                print("Validando data allocation en bigquery ")
                df_val_fecha_allocation=obj_bq.consultar_table(q_val_data_allocation)

                if df_val_fecha_allocation.empty!=True:
                    df_data_allocation=obj_bq.consultar_table(q_data_allocation)

                    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_file:
                        # Guardar el DataFrame como CSV en el archivo temporal
                        df_data_allocation.to_csv(tmp_file.name, index=False,sep="|")
                        tmp_file.flush()  # Asegurarse de que todos los datos se escriban en el archivo

                        val_exists = sftp.exists(remote_path_d)
                        if val_exists:
                            # Subir el archivo temporal al servidor SFTP
                            sftp.put(localpath=tmp_file.name, remotepath=remote_file_path_d)
                            print(f'Archivo cargado exitosamente a {remote_path_d}')
                        else:
                            print(f'La ruta {remote_path_d} no existe en el servidor SFTP')

                    print(f"Se terminó de cargar el archivo {remote_path_d} en directorio output server SFTP")
                    ruta_completa_procesado=f"{remote_path_o_procesado}/{archivo_a_descargar}"
                    sftp.rename(ruta_completa,ruta_completa_procesado)
                    print("se movio el archivo origen a procesado...")
                else:
                    print("No hay data allocation en big query para el dia procesado")
        else:
            print('No se encontraron archivos en server SFTP que coincidan con el patrón especificado.')
    else:
        print(f'La ruta {remote_path_o} no existe.')
