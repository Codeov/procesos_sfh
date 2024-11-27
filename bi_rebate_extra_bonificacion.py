import pandas as pd
import os
import est_sql as sql
from est_bq import bigq 
from google.cloud import bigquery

tipo_proceso="sin_distribucion_202410"
ruta_file=r"D:\BI\soporte diario\rebate\archivo_input\bonificacion_logistica\sin_distribucion_202410.xlsx"
file_json_pbi = r"D:\python\credenciales biq query\spsa-sistemasbi-powerbi-e0770fbf3fa0.json"

#credenciales 18
ip = '10.20.1.5'
bd= 'Retail_Stage'
us= 'operador'
pw= 'operador'

def cargar_excel_df(ruta_file):
    if os.path.exists(ruta_file):
        #df=pd.read_excel(ruta_file)
        df=pd.DataFrame(data=(pd.read_excel(ruta_file)),columns=["Periodo","Codigo_Proveedor","Flag_Filtrar_Proveedor","Nivel_Jerarquia","Codigo_Jerarquia","Monto","Clase_Acuerdo","Formato","Tipo_Venta","Tipo_Marca"])
        df['Periodo']=df['Periodo'].map(str)
        df['Codigo_Proveedor']=df['Codigo_Proveedor'].map(str)
        df['Flag_Filtrar_Proveedor']=df['Flag_Filtrar_Proveedor'].map(str)
        df['Nivel_Jerarquia']=df['Nivel_Jerarquia'].map(str)
        df['Codigo_Jerarquia']=df['Codigo_Jerarquia'].map(str)
        df['Monto']=df['Monto'].map(str)
        df['Clase_Acuerdo']=df['Clase_Acuerdo'].map(str)
        df['Formato']=df['Formato'].map(str)
        df['Tipo_Venta']=df['Tipo_Venta'].map(str)
        df['Tipo_Marca']=df['Tipo_Marca'].map(str)
    return df

objbq=bigq(file_json_pbi)
project='spsa-sistemasbi-powerbi'
dataset='REB_EXTRA'
table='tmp_adi_sdgac'
objbq.clear_table(project,dataset,table)
objbq.ins_table(project,dataset,table,cargar_excel_df(ruta_file))
objbq.exec_query_sin_param("call `spsa-sistemasbi-powerbi.REB_EXTRA.sp_extra_bl` ()")
print("se termino de procesar la distribución de bonificación logistica")

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






























"""
select
(
select sum(cast(a.monto as float64)) monto
from
`spsa-sistemasbi-powerbi.REB_EXTRA.tmp_adi_sdgac` a
) 
-
(
select sum(a.Monto_Distribuido) monto from
`spsa-sistemasbi-powerbi.REB_EXTRA.tmp_fact_aporte_distribuido_pre` a
) diferencia_monto;
"""




    

