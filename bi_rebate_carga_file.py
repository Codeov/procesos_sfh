import pandas as pd
import os
import est_sql as sql
from est_bq import bigq 
from google.cloud import bigquery

tipo_proceso="sin_distribucion"
ruta_file=r"D:\BI\soporte diario\rebate\archivo_input\sin_distribucion\sin_distribucion.xlsx"

#credenciales 18
ip = '10.20.1.5'
bd= 'Retail_Stage'
us= 'operador'
pw= 'operador'

def cargar_excel_ls(ruta_file):
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
        print(df)
        ls=df.values.tolist()
    return ls

obj=sql.SQLServer(ip,bd,us,pw)
obj.connect_to_sql_server()
obj.query("truncate table Tmp_Adi_SDGAC")
obj.query_insert_tupla("insert into Tmp_Adi_SDGAC values (?,?,?,?,?,?,?,?,?,?)",cargar_excel_ls(ruta_file))
obj.commit()
obj.cerrar();




    

