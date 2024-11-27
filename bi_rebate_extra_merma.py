import pandas as pd
import os
import est_sql as sql
from est_bq import bigq 
from google.cloud import bigquery

tipo_proceso="reconocimiento_merma_202410"
ruta_file=r"D:\BI\soporte diario\rebate\archivo_input\reconocimiento_merma\reconocimiento_merma_202410.xlsx"
file_json_pbi = r"D:\python\credenciales biq query\spsa-sistemasbi-powerbi-e0770fbf3fa0.json"

#credenciales 18
ip = '10.20.1.5'
bd= 'Retail_Stage'
us= 'operador'
pw= 'operador'

def cargar_excel_df(ruta_file):
    if os.path.exists(ruta_file):
        #df=pd.read_excel(ruta_file)
        df=pd.DataFrame(data=(pd.read_excel(ruta_file)),columns=["periodo","tipo_local","codigo_producto","nombre_producto","cantidad","costo_total","local","nombre_local","division","descripcion_area"])
        df['periodo']=df['periodo'].map(str)
        df['tipo_local']=df['tipo_local'].map(str)
        df['codigo_producto']=df['codigo_producto'].map(str)
        df['nombre_producto']=df['nombre_producto'].map(str)
        df['cantidad']=df['cantidad'].map(str)
        df['costo_total']=df['costo_total'].map(str)
        df['local']=df['local'].map(str)
        df['nombre_local']=df['nombre_local'].map(str)
        df['division']=df['division'].map(str)
        df['descripcion_area']=df['descripcion_area'].map(str)
    return df

df=cargar_excel_df(ruta_file)
print(df)

objbq=bigq(file_json_pbi)
project='spsa-sistemasbi-powerbi'
dataset='REB_EXTRA'
table='tmp_Adi_Rec_Merma'
#objbq.clear_table(project,dataset,table)
#objbq.ins_table(project,dataset,table,cargar_excel_df(ruta_file))

#objbq.exec_query_sin_param("call `spsa-sistemasbi-powerbi.REB_EXTRA.sp_extra_rm` ()")
print("se termino de procesar la distribución de reconocimiento merma")
sql=sql.SQLServer('10.20.1.5','Retail_DW','operador','operador');
sql.connect_to_sql_server();
sql.query("truncate table Retail_Stage.dbo.Pre_Aporte_Distribuido")

query_req="""
select a.tiempo_key,a.local_key,a.proveedor_key,
a.producto_key,a.sistema_key,a.numero_aporte_key,
sum(a.Monto_Distribuido) monto,
a.claseacuerdo_key,a.tipo_venta_key,a.nivel_jerarquia,a.tipo_extra_key
from `spsa-sistemasbi-powerbi.REB_EXTRA.tmp_fact_aporte_distribuido_pre` a
group by 
a.tiempo_key,a.local_key,a.proveedor_key,
a.producto_key,a.sistema_key,a.numero_aporte_key,
a.claseacuerdo_key,a.tipo_venta_key,a.nivel_jerarquia,a.tipo_extra_key
"""
sql.query_insert_tupla("insert into Retail_Stage.dbo.Pre_Aporte_Distribuido values (?,?,?,?,?,?,?,?,?,?,?)",(objbq.consultar_table(query_req)).values.tolist())
sql.commit()





















    

