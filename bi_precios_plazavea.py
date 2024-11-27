

from google.cloud import bigquery as bq 
import datetime
from dateutil.relativedelta import relativedelta
import os

ruta_destino=r"D:\archivo_prueba"
file_json_pb= r'D:\python\credenciales biq query\sistemas-bi-7a46b3894448.json'

fecha=datetime.datetime.now()
fecha_var=fecha+relativedelta(days=0)
format_fecha_var="PreciosPlazaVea_"+(datetime.datetime.strftime(fecha_var,"%Y%m%d"))+".csv"

ruta_destino_rename=os.path.join(ruta_destino,format_fecha_var)

query_precios_plazavea="""
  select 
    a.*
    from `sistemas-bi.Tacho.precios_plazavea` a

"""

def req_bq(json,query):
    try:
        oBq=bq.Client.from_service_account_json(file_json_pb)
        reqBq=oBq.query(query_precios_plazavea)
        reqBq.result()
        df=reqBq.to_dataframe()
        return df
    except Exception as e:
        raise Exception("Error en BigQuery")


df=req_bq(file_json_pb,query_precios_plazavea)
if df.empty==True:
    
    #df.to_csv(ruta_destino_rename,index=False,sep=",",encoding='ISO-8859-1', errors='replace')
    df.to_csv(ruta_destino_rename,index=False,sep=",",encoding='UTF-16')
    print("No hay data")
else:
    df.to_csv(ruta_destino_rename,index=False,sep=",",encoding='UTF-16')
    #df.to_csv(ruta_destino_rename,index=False,sep=",")
    print("Si hay data")
