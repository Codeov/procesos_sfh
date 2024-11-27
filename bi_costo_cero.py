
from google.cloud import bigquery as bq
import pandas as pd
import oracledb

usuario= "SINTERFACE"
contraseña= "SF5590X"
ip_server_name= "10.20.11.20/SPT01"
directory_co=r"D:\instaladores\biblioteca cliente oracle\instantclient_11_2"


ruta_file_json= r"D:\python\credenciales biq query\sistemas-bi-7a46b3894448.json"

fecha_inicial='2023-09-01'
fecha_final='2023-09-25'

query_2= """
select distinct a.fecha fecha,b.codigo_alterno codigo_local,a.codigo_producto codigo_producto,
ifnull(a.costo_valorizado,0) costo_unitario from `SPSA.tmp_venta_costo_cero` a
inner join `SPSA.dim_local` b
on a.codigo_local=b.codigo_local
"""

class big_query:
    def __init__(self,file):
        self.file=file

    def query_sp(self,query):
        try:
                obj=bq.Client.from_service_account_json(self.file)
                self.query=obj.query(query)
                self.query.result()
                return True
        except Exception as e:
            raise print('error bigquery ...'+e)
    def query_qr(self,query):
        try:
                obj=bq.Client.from_service_account_json(self.file)
                self.query=(obj.query(query)).result()
                return self.query
        except Exception as e:
            raise print('error bigquery ...'+e)

class oracle :
    def __init__(self,usuario,contraseña,dsn):
        self.usuario=usuario
        self.contraseña= contraseña
        self.dsn=dsn

    def conectar_oracle(self):
        try:
            self.cnxo=oracledb.connect(user=self.usuario,password=self.contraseña,dsn=self.dsn)
            print("conexion exitosa to oracle")
            return self.cnxo
        except Exception as e:
            print(e)
            return None
    def ejecutar_query(self,query):
        try:
            cursor=self.cnxo.cursor()
            cursor.execute(query)
            #self.data=cursor.fetchall()
            #return(self.data)
        except Exception as e:
            print(e)
            return None
        
    def ejecutar_query_cab(self,query):
        try:
            cursor=self.cnxo.cursor()
            cursor.execute(query)
            self.cab = [col[0] for col in cursor.description]
            return(self.cab)
        except Exception as e:
            print(e)
            return None
        
    def cerrar(self):
        try:
            self.close= self.cnxo.close()
            print("conexion to oracle cerrada")
            return self.close
        except Exception as e:
            print(e)
            return None
    
    def query_insert_tupla(self,sql_statement,tupla):
        if self.cnxo is None:
            print("conexion fallida")
            return
        try:
            cursor=self.cnxo.cursor()
            #cursor.fast_executemany=True
            cursor.executemany(sql_statement,tupla,
                   batcherrors=True)
            
        except Exception as e:
            #for error in cursor.getbatcherrors():
             #print("Error", error.message, "at row offset", error.offset)
            print(e)

    def commit(self):
        if self.cnxo is None:
            print("no hay conexion")
            return
        try:
            self.cnxo.commit()
  
        except Exception as e:
            raise Exception ("error de commit sql, detalle :" + str(e))

def query_1(ini,fin):

    query= f"""
    call `SPSA.ins_tmp_venta_costo_cero` ('{ini}', '{fin}')
    """
    return query

req=big_query(ruta_file_json)
req.query_sp(query_1(fecha_inicial,fecha_final))
res_qr=req.query_qr(query_2).to_dataframe()
df=pd.DataFrame(data=res_qr,columns=['fecha','codigo_local','codigo_producto','costo_unitario'])
df['fecha']=df['fecha'].map(str)
df['codigo_local']=df['codigo_local'].map(str)
df['codigo_producto']=df['codigo_producto'].map(str)
df['costo_unitario']=df['costo_unitario'].map(float)
list_cf=df.values.tolist()

oracledb.init_oracle_client(directory_co) 
obj_ora= oracle(usuario,contraseña,ip_server_name)
obj_ora.conectar_oracle()
obj_ora.ejecutar_query("truncate table EINTERFACE.BI_OV_COST_FALTANTE")
obj_ora.query_insert_tupla("insert into EINTERFACE.BI_OV_COST_FALTANTE(fecha, codigo_local, codigo_producto, costo_unitario) values (:1,:2,:3,:4)",list_cf)
obj_ora.commit()

"""
select to_char(a.fecha,'yyyy-mm-dd') fecha,a.codigo_local,a.codigo_producto,a.cst_uni,a.tipo
 from EINTERFACE.BI_RSL_COSTO a
 inner join EINTERFACE.BI_OV_COST_FALTANTE b
 on to_char(a.fecha,'yyyy-mm-dd')=b.fecha
 and a.codigo_local=b.codigo_local
 and cast(a.codigo_producto as integer)=cast(b.codigo_producto as integer)
 where a.cst_uni<>0;
"""




