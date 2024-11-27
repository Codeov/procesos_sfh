

from google.cloud import bigquery as bq 
import datetime
from dateutil.relativedelta import relativedelta
import os

ruta_destino=r"D:\archivo_prueba"
file_json_pb= r'D:\python\credenciales biq query\sistemas-bi-7a46b3894448.json'

fecha=datetime.datetime.now()
fecha_var=fecha+relativedelta(days=-1)
format_fecha_var="CostoCero_"+(datetime.datetime.strftime(fecha_var,"%Y-%m-%d"))+".csv"

ruta_destino_rename=os.path.join(ruta_destino,format_fecha_var)

query_costo_cero="""
select a.fecha,a.codigo_producto,a.codigo_local,sum(a.unidades) unidades,
sum(round(a.costo_valorizado_pmm,2)) costo_valorizado_pmm from
(
  select a.dato_f2 fecha,a.codigo_producto,ifnull(b.nombre_producto,'sin nombre') nombre_producto,c.codigo_alterno codigo_local,c.nombre,
  sum(a.venta_unidad) unidades,sum(cast(a.costo_fee as float64)) costo_valorizado_pmm from `SPSA.int_fact_ticket` a
  left join `SPSA.dim_producto` b
  on a.codigo_producto=b.codigo_producto
  inner join `SPSA.dim_local` c
  on a.codigo_local=c.codigo_local
  where a.dato_f2 >= '2024-01-01'
  and cast(a.costo_fee as float64) = 0
  and a.venta_unidad <> 0
  --and b.des_tipo_material= 'Concesion'
  and a.codigo_tipo_trx = 'PVT'
  group by a.dato_f2,a.codigo_producto,b.nombre_producto,c.codigo_alterno,c.nombre
  union all
  select a.dato_f2 fecha,a.codigo_producto,ifnull(b.nombre_producto,'sin nombre') nombre_producto,c.codigo_alterno codigo_local,c.nombre,
  sum(a.venta_unidad) unidades,sum(cast(a.costo_fee as float64)) costo_valorizado_pmm from `SPSA.int_fact_ticket_tu_entrada` a
  left join `SPSA.dim_producto` b
  on a.codigo_producto=b.codigo_producto
  inner join `SPSA.dim_local` c
  on a.codigo_local=c.codigo_local
  where a.dato_f2 >= '2024-01-01'
  --and b.des_tipo_material= 'Concesion'
  and a.codigo_tipo_trx = 'PVT'
  and cast(a.costo_fee as float64) = 0
  and a.venta_unidad <> 0
  group by a.dato_f2,a.codigo_producto,b.nombre_producto,c.codigo_alterno,c.nombre
) a
group by a.fecha,a.codigo_producto,a.codigo_local
"""

def req_bq(json,query):
    try:
        oBq=bq.Client.from_service_account_json(file_json_pb)
        reqBq=oBq.query(query_costo_cero)
        reqBq.result()
        df=reqBq.to_dataframe()
        return df
    except Exception as e:
        raise Exception("Error en BigQuery")


df=req_bq(file_json_pb,query_costo_cero)
if df.empty==True:
    df.to_csv(ruta_destino_rename,index=False,sep="|")
    print("No hay costo cero")
else:
    df.to_csv(ruta_destino_rename,index=False,sep="|")
    print("Si hay costo cero")
