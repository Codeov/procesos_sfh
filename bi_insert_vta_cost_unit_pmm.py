import datetime
import oracledb
from google.cloud import bigquery
import pandas
import time

usuario= "SINTERFACE"
contraseña= "SF5590X"
ip_server_name= "10.20.11.20/SPT01"
directory_co=r"D:\instaladores\biblioteca cliente oracle\instantclient_11_2"

fecha_inicio='2023-09-01'
fecha_fin='2023-09-30'

f_fecha_inicio=datetime.datetime.strptime(fecha_inicio,'%Y-%m-%d')
f_fecha_fin=datetime.datetime.strptime(fecha_fin,'%Y-%m-%d')

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

 ###################################################
def query (fecha):
    query= f"""
     select a.dato_f2 fecha,b.codigo_alterno,cast(a.codigo_producto as int64) codigo_producto,
    round(ifnull(c.costo_fee,0),3) costo_unitario
    ,round(sum(ifnull(a.venta_unidad,0)),3) unidades
    , 0 tipo
    from `SPSA.int_fact_ticket` a
    left join 
    (
    select a.fecha,a.codigo_local,a.codigo_producto,a.costo_fee from `SPSA.fact_costo` a
    where a.fecha between '{fecha}' and '{fecha}'
    ) c
    on a.dato_f2=c.fecha and a.codigo_local=c.codigo_local and a.codigo_producto=c.codigo_producto
    inner join `SPSA.dim_local` b
    on a.codigo_local=b.codigo_local 
    where a.dato_f2 between 
    '{fecha}' and '{fecha}'
    --and b.codigo_alterno in  ('195','1198')
    group by  a.dato_f2,b.codigo_alterno,a.codigo_producto,c.costo_fee
    """
    return query

def request (fecha):
    ruta_file_json= r"D:\python\credenciales biq query\sistemas-bi-7a46b3894448.json"
    request=bigquery.Client.from_service_account_json(ruta_file_json)
    job_request=request.query(query(fecha),location="us")
    job_request.result()
    df=job_request.to_dataframe()
    data=pandas.DataFrame(data=df,columns=["fecha","codigo_alterno","codigo_producto","costo_unitario","unidades","tipo"]
                          )

    data['fecha'] = data["fecha"].map(str)
    data['codigo_alterno'] = data["codigo_alterno"].map(str)
    data['codigo_producto'] = data["codigo_producto"].map(str)
    data['costo_unitario'] = data["costo_unitario"].map(float)
    data['unidades'] = data["unidades"].map(float)
    data['tipo'] = data["tipo"].map(str)

    list_data=data.values.tolist()
    #print(list_data)
    return list_data

oracledb.init_oracle_client(directory_co) 
obj_ora= oracle(usuario,contraseña,ip_server_name)
obj_ora.conectar_oracle()
obj_ora.ejecutar_query("truncate table EINTERFACE.BI_OVR_COSTO")

while f_fecha_inicio <= f_fecha_fin:
    v_fecha=datetime.datetime.strftime(f_fecha_inicio,'%Y-%m-%d')
    obj_ora.query_insert_tupla("insert into EINTERFACE.BI_OVR_COSTO (FECHA, CODIGO_LOCAL, CODIGO_PRODUCTO, CST_UNI, UNIDADES, TIPO) values (:1,:2,:3,:4,:5,:6)",request(v_fecha))
    obj_ora.commit()
    #time.sleep(5)
    print(v_fecha)
    f_fecha_inicio=f_fecha_inicio+datetime.timedelta(days=1)

obj_ora.cerrar()