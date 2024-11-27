import pandas as pd
import os
import est_sql as sql
from est_bq import bigq 
from google.cloud import bigquery

pd.set_option('future.no_silent_downcasting', True)

tipo_proceso="aporte_toh_202410"
ruta_file=r"D:\BI\soporte diario\rebate\archivo_input\tarjeta_oh\aporte_toh_202410.xlsx"
file_json_pbi = r"D:\python\credenciales biq query\spsa-sistemasbi-powerbi-e0770fbf3fa0.json"

#credenciales 18
ip = '10.20.1.5'
bd= 'Retail_Stage'
us= 'operador'
pw= 'operador'

def valor_reemplazo(col):
    if col.dtype == 'float64' or col.dtype == 'int64':
        return 0  
    elif col.dtype == 'object':
        return ''  
    elif col.dtype == 'datetime64[ns]':
        return pd.Timestamp('1900-01-01')
    else:
        return None  
    
def cargar_excel_df(ruta_file):
    if os.path.exists(ruta_file):
        df=pd.read_excel(ruta_file)
        df=pd.DataFrame(data=(pd.read_excel(ruta_file)),columns=["periodo","codigo","categoria","tipo","formato","clase_acuerdo","funo_sin_igv"])
        for col in df.columns:
            df[col]=df[col].fillna(valor_reemplazo(df[col]))
        df['periodo']=df['periodo'].map(str)
        df['codigo']=df['codigo'].map(str)
        df['categoria']=df['categoria'].map(str)
        df['tipo']=df['tipo'].map(str)
        df['formato']=df['formato'].map(str)
        df['clase_acuerdo']=df['clase_acuerdo'].map(str)
        df['funo_sin_igv']=df['funo_sin_igv'].map(str)
    return df


df=cargar_excel_df(ruta_file)
print(df)

objbq=bigq(file_json_pbi)
project='spsa-sistemasbi-powerbi'
dataset='REB_EXTRA'
table='tmp_tarjeta_oh'
objbq.clear_table(project,dataset,table)
objbq.ins_table(project,dataset,table,cargar_excel_df(ruta_file))


objbq.exec_query_sin_param("call `spsa-sistemasbi-powerbi.REB_EXTRA.sp_extra_toh` ()")
print("se termino de procesar la distribución de tarjeta oh")

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























    

