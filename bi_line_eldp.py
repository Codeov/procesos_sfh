import datetime
import pytz
import os
import fnmatch
import pandas as pd
import est_bq as bq

file_json = r"D:\python\credenciales biq query\vta_linea_sistemas-bi-438c564c407c.json"

time_zone=pytz.timezone("America/Lima")
fecha_actual=datetime.datetime.now(time_zone)
fecha_actual_f=datetime.datetime.strftime(fecha_actual,"%Y%m%d")
print(fecha_actual_f)

ruta_archivo_input=r"D:\archivos_prueba"
#patron_archivo_line=f"line_{fecha_actual_f}.csv"
patron_archivo_line=f"line.csv"
patron_archivo_line_detalle=f"line_detalle.csv"
patron_archivo_regla_freezing=f"regla_freezing.csv"

list_archivos_patron=[]
list_archivos_patron.append(patron_archivo_line)
list_archivos_patron.append(patron_archivo_line_detalle)
list_archivos_patron.append(patron_archivo_regla_freezing)

archivos=os.listdir(ruta_archivo_input)
obj_bq=bq.bigq(file_json)

for i in list_archivos_patron:
    print(i)
    archivofiftrado=(fnmatch.filter(archivos,i))[0]
    if archivofiftrado:
        print("el archivo si está disponible")
        ruta_archivo_input_complete=os.path.join(ruta_archivo_input,i)
        print(ruta_archivo_input_complete)

        df = pd.read_csv(ruta_archivo_input_complete,delimiter="|",encoding='utf-8',encoding_errors='ignore')
        print(df)

        df.astype(str)
        if patron_archivo_line==i:
            project="sistemas-bi"
            dataset="SPSA_STAGE"
            table="tmp_line"
            obj_bq.ins_table_param_segun_input(project,dataset,table,df)

        if patron_archivo_line_detalle==i:
            project="sistemas-bi"
            dataset="SPSA_STAGE"
            table="tmp_line_detalle"
            obj_bq.ins_table_param_segun_input(project,dataset,table,df)

        if patron_archivo_regla_freezing==i:
            project="sistemas-bi"
            dataset="SPSA_STAGE"
            table="tmp_regla_freezing"
            obj_bq.ins_table_param_segun_input(project,dataset,table,df)
    
    else :
        print(f"el archivo {i} no se encuentra dispobible")
        continue
