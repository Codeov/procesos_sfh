
import os
import pandas as pd
from est_sql import *
from google.cloud import bigquery
import time

#credenciales 18
ip = '10.20.17.45'
bd= 'BI_STG'
us= 'sa'
pw= 'agilito2030'

ruta_cajas=r"D:\python\archivos_input_big_query\cajas_scm"
nombre_file="mapeo_terminales_202311.xlsx"
ruta_json_sbi= r'D:\python\credenciales biq query\sistemas-bi-7a46b3894448.json'


q_var_terminal_scm="""
select distinct a.mes, codigo_local,a.numero_terminal,a.descripcion
from [dbo].[var_terminales_scm] a
"""

#

def validar_file(ruta_cajas,nombre_file):
    ruta_file=os.path.join(ruta_cajas,nombre_file)
    if ruta_file:
        print("El archivo si se encuentra disponible")
        return True
    else:
        raise Exception("El archivo no se encuentra")

def carga_excel_df(ruta_cajas,nombre_file):
    pd_caj=pd.read_excel(os.path.join(ruta_cajas,nombre_file),sheet_name="cajas")
    pd_caj['mes']=pd_caj['mes'].map(str)
    pd_caj['tienda']=pd_caj['tienda'].map(str)
    pd_caj['caja_1']=pd_caj['caja_1'].map(str)
    pd_caj['caja_2']=pd_caj['caja_2'].map(str)
    pd_caj['caja_3']=pd_caj['caja_3'].map(str)
    pd_caj['caja_4']=pd_caj['caja_4'].map(str)
    pd_caj['caja_5']=pd_caj['caja_5'].map(str)
    pd_caj['caja_6']=pd_caj['caja_6'].map(str)
    pd_caj['caja_7']=pd_caj['caja_7'].map(str)
    pd_caj['caja_8']=pd_caj['caja_8'].map(str)
    pd_caj['caja_9']=pd_caj['caja_9'].map(str)
    pd_caj['caja_10']=pd_caj['caja_10'].map(str)
    pd_caj['caja_11']=pd_caj['caja_11'].map(str)
    pd_caj['caja_12']=pd_caj['caja_12'].map(str)
    pd_caj['caja_13']=pd_caj['caja_13'].map(str)
    pd_caj['caja_14']=pd_caj['caja_14'].map(str)
    pd_caj['caja_15']=pd_caj['caja_15'].map(str)
    pd_caj['caja_16']=pd_caj['caja_16'].map(str)
    pd_caj['caja_17']=pd_caj['caja_17'].map(str)
    pd_caj['caja_18']=pd_caj['caja_18'].map(str)
    pd_caj['caja_19']=pd_caj['caja_19'].map(str)
    pd_caj['caja_20']=pd_caj['caja_20'].map(str)
    pd_caj['caja_21']=pd_caj['caja_21'].map(str)
    pd_caj['caja_22']=pd_caj['caja_22'].map(str)
    pd_caj['caja_23']=pd_caj['caja_23'].map(str)
    pd_caj['caja_24']=pd_caj['caja_24'].map(str)
    pd_caj['caja_25']=pd_caj['caja_25'].map(str)
    pd_caj['caja_26']=pd_caj['caja_26'].map(str)
    pd_caj['caja_27']=pd_caj['caja_27'].map(str)
    pd_caj['caja_28']=pd_caj['caja_28'].map(str)
    pd_caj['caja_29']=pd_caj['caja_29'].map(str)
    pd_caj['caja_30']=pd_caj['caja_30'].map(str)
    pd_caj['caja_31']=pd_caj['caja_31'].map(str)
    pd_caj['caja_32']=pd_caj['caja_32'].map(str)
    pd_caj['caja_33']=pd_caj['caja_33'].map(str)
    pd_caj['caja_34']=pd_caj['caja_34'].map(str)
    pd_caj['caja_35']=pd_caj['caja_35'].map(str)
    pd_caj['caja_36']=pd_caj['caja_36'].map(str)
    pd_caj['caja_37']=pd_caj['caja_37'].map(str)
    pd_caj['caja_38']=pd_caj['caja_38'].map(str)
    pd_caj['caja_39']=pd_caj['caja_39'].map(str)
    pd_caj['caja_40']=pd_caj['caja_40'].map(str)
    pd_caj['caja_41']=pd_caj['caja_41'].map(str)
    pd_caj['caja_42']=pd_caj['caja_42'].map(str)
    pd_caj['caja_43']=pd_caj['caja_43'].map(str)
    pd_caj['caja_44']=pd_caj['caja_44'].map(str)
    pd_caj['caja_45']=pd_caj['caja_45'].map(str)
    pd_caj['caja_46']=pd_caj['caja_46'].map(str)
    pd_caj['caja_47']=pd_caj['caja_47'].map(str)
    pd_caj['caja_48']=pd_caj['caja_48'].map(str)
    pd_caj['caja_49']=pd_caj['caja_49'].map(str)
    pd_caj['caja_50']=pd_caj['caja_50'].map(str)
    pd_caj['caja_51']=pd_caj['caja_51'].map(str)
    pd_caj['caja_52']=pd_caj['caja_52'].map(str)
    pd_caj['caja_53']=pd_caj['caja_53'].map(str)
    pd_caj['caja_54']=pd_caj['caja_54'].map(str)
    pd_caj['caja_55']=pd_caj['caja_55'].map(str)
    pd_caj['caja_56']=pd_caj['caja_56'].map(str)
    pd_caj['caja_57']=pd_caj['caja_57'].map(str)
    pd_caj['caja_58']=pd_caj['caja_58'].map(str)
    pd_caj['caja_59']=pd_caj['caja_59'].map(str)
    pd_caj['caja_60']=pd_caj['caja_60'].map(str)
    ls_pd_caj=pd_caj.values.tolist()
    return ls_pd_caj

def cargar_procesar_var_terminales_scm(obj,ls_ter):
    obj.query("truncate table tmp_terminales_scm")
    obj.query_insert_tupla("insert into tmp_terminales_scm (mes,tienda,caja_1,caja_2,caja_3,caja_4,caja_5,caja_6,caja_7,caja_8,caja_9,caja_10,caja_11,caja_12,caja_13,caja_14,caja_15,caja_16,caja_17,caja_18,caja_19,caja_20,caja_21,caja_22,caja_23,caja_24,caja_25,caja_26,caja_27,caja_28,caja_29,caja_30,caja_31,caja_32,caja_33,caja_34,caja_35,caja_36,caja_37,caja_38,caja_39,caja_40,caja_41,caja_42,caja_43,caja_44,caja_45,caja_46,caja_47,caja_48,caja_49,caja_50,caja_51,caja_52,caja_53,caja_54,caja_55,caja_56,caja_57,caja_58,caja_59,caja_60) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",ls_ter)
    obj.commit()
    obj.query("exec ins_var_terminales_scm")
    obj.commit()

def select_var_terminales_scm(obj):
    ls=obj.query_return(q_var_terminal_scm)
    dfv=pd.DataFrame(data=ls,columns=['mes','codigo_local','numero_terminal','descripcion'])
    dfv['mes']=dfv['mes'].map(str)
    dfv['codigo_local']=dfv['codigo_local'].map(str)
    dfv['numero_terminal']=dfv['numero_terminal'].map(str)
    dfv['descripcion']=dfv['descripcion'].map(str)
    return dfv

def load_table_fact_and_tmp_var_terminales_scm(df):
  ins_pbi=bigquery.Client.from_service_account_json(ruta_json_sbi)
  request_pbi=ins_pbi.query("delete `sistemas-bi.SPSA.tmp_var_terminales_scm` where true",location="us")
  request_pbi.result()
  
  struct_ins_pbi = bigquery.LoadJobConfig (
         autodetect=True,
     )

  table_id_pbi='sistemas-bi.SPSA.tmp_var_terminales_scm'

  job_ins_pbi = ins_pbi.load_table_from_dataframe(df,table_id_pbi,job_config=struct_ins_pbi)
  while job_ins_pbi.state != 'DONE':
      time.sleep(0.1)
      job_ins_pbi.reload()
  print("se terminó de procesar - "+str(table_id_pbi)+' '+str(job_ins_pbi.result()))
  request_sp=ins_pbi.query("call `sistemas-bi.SPSA.ins_fact_var_terminales_scm` ()",location="us")
  request_sp.result()
  print("se terminó de procesar el SP sistemas-bi.SPSA.ins_fact_var_terminales_scm")

validar_file(ruta_cajas,nombre_file)
sql=SQLServer(ip,bd,us,pw)
sql.connect_to_sql_server()
cargar_procesar_var_terminales_scm(sql,carga_excel_df(ruta_cajas,nombre_file))
df_var_ter=select_var_terminales_scm(sql)
load_table_fact_and_tmp_var_terminales_scm(df_var_ter)
sql.cerrar()



    
#ins_var_terminales_scm
#ruta_file=os.path.join(ruta_cajas,nombre_file)
#print(ruta_file)

#select a.* from tmp_terminales_scm a;


