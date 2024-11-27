from google.cloud import bigquery
import os
import time
import pyodbc as odbc
import pandas as pd

file_json = r"D:\python\credenciales biq query\sistemas-bi-7a46b3894448.json"
print(file_json)
os.environ['GOOGLE_APPLICATION_CREDENTIALS']=file_json

query_op = """
select distinct  cast(a.codigo as integer) cod_promocion from 
(
	select distinct a.codigo,b.Promocion_Key from Retail_Stage.dbo.Tmp_Adi_Tarjeta a
	inner join Dim_Promocion_Ticket b on a.codigo=b.Codigo_Promocion
) a
left join 
(
	select distinct c.promocion_key from Fact_Promocion_Trx c --8456
	inner join dim_local d on c.Local_Key= d.Local_Key
	where c.Tiempo_Key between  7092 and 7121
	--and d.Codigo_SAP   in ('SO02','SO03','SO04','SO05') 6967
    and 1=2
) c
on a.Promocion_Key=c.Promocion_Key 
where c.Promocion_Key is null
;
"""
query_op_hed = """
select distinct  cast(a.codigo as integer) cod_promocion from 
(
	select distinct a.codigo,b.Promocion_Key from Retail_Stage.dbo.Tmp_Adi_Tarjeta a
	inner join Dim_Promocion_Ticket b on a.codigo=b.Codigo_Promocion
) a
left join 
(
	select distinct c.promocion_key from Fact_Promocion_Trx c --8456
	inner join dim_local d on c.Local_Key= d.Local_Key
	where c.Tiempo_Key between  6940 and 6970
	--and d.Codigo_SAP   in ('SO02','SO03','SO04','SO05')
) c
on a.Promocion_Key=c.Promocion_Key 
where c.Promocion_Key is null and 1=2;
"""

query_bg = """
SELECT a.* FROM `sistemas-bi.SPSA.int_fact_promocion` a
inner join `sistemas-bi.Tacho.promo_1` b
on a.codigo_promocion=cast(b.cod_promocion as string)
WHERE a.fecha between '2024-06-01' and '2024-06-30' and 1=5
--and a.codigo_local  in ('SO02','SO03','SO04','SO05');
"""

 ###################################################
class SQLServer:

    autocommit= True

    def __init__(self,SERVER,DATABASE,UID,PWD):
        self.SERVER=SERVER
        self.DATABASE=DATABASE
        self.UID=UID
        self.PWD=PWD

    def connect_to_sql_server(self):
        try:
            self.conn= odbc.connect(self._connection_string())
            #print('Connected')
            return self.conn

        except Exception as e:
            print(e)
            return None
    
    def _connection_string(self):

        SERVER= self.SERVER
        DATABASE=self.DATABASE
        UID=self.UID
        PWD=self.PWD
        autocommit= self.autocommit

        conn_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+SERVER+';DATABASE='+DATABASE+';UID='+UID+';PWD='+PWD+';autocommit='+str(autocommit)+''
        return conn_string
    #DRIVER={SQL Server Native Client 11.0};SERVER=
    
    ###################

    ###################EjecutarQuery
    def query(self,sql_statement):
        if self.conn is None:
            print("conexion fallida")
            return
        try:
            cursor=self.conn.cursor()
            cursor.execute(sql_statement)

        except Exception as e:
            print(e)

    def query_response(self,sql_statement):
        if self.conn is None:
            print("conexion fallida")
            return
        try:
            cursor=self.conn.cursor()
            cursor.execute(sql_statement)
            self.data=cursor.fetchall()
            return(self.data)

        except Exception as e:
            print(e)

    def query_response_cab(self,query):
        try:
            cursor=self.conn.cursor()
            cursor.execute(query)
            self.cab = [col[0] for col in cursor.description]
            return(self.cab)
        except Exception as e:
            print(e)
            return None
    
    def query_insert_tupla(self,sql_statement,tupla):
        if self.conn is None:
            print("conexion fallida")
            return
        try:
            cursor=self.conn.cursor()
            cursor.fast_executemany=True
            cursor.executemany(sql_statement,tupla)

        except Exception as e:
            print(e)

    def commit(self):
        if self.conn is None:
            print("no hay conexion")
            return
        try:
            self.conn.commit()
  
        except Exception as e:
            print(e)
    
    def cerrar(self):
        if self.conn is None:
            print("no hay conexion")
            return
        try:
            self.conn.close()
  
        except Exception as e:
            print(e)

 ###################################################

client = bigquery.Client()
def cargar_table_df (query) :

    try:
        request= client.query(query)

        while request.state!= 'DONE':
            time.sleep(2)
            request.reload()

        if request.state == 'DONE':
            pdb=request.to_dataframe()
            list_data=pdb.values.tolist()
            return list_data          
    except Exception as e:
        print(e)

sql=SQLServer('10.20.1.5','Retail_DW','operador','operador');
sql.connect_to_sql_server();

def ins_data_df(query,cab):
   tupla=sql.query_response(query)
   lst_data_hed=sql.query_response_cab(cab)
   lst_data = [list(row) for row in tupla]
   df = pd.DataFrame(lst_data,columns=lst_data_hed)
   return df

def ins_data_promo_gcp():
 client_cab= bigquery.Client()
 job_config_cab = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition='WRITE_TRUNCATE'
    )
 target_table_id='sistemas-bi.Tacho.promo_1' 
 job_cab = client_cab.load_table_from_dataframe(ins_data_df(query_op,query_op_hed),target_table_id,job_config=job_config_cab)
 while job_cab.state != 'DONE':
    time.sleep(1)
    job_cab.reload()
    print("se terminó de procesar la promo_1 - "+str(job_cab.result()))

ins_data_promo_gcp()
sql.query("truncate table tmp_int_fact_promocion")
sql.query_insert_tupla("insert into tmp_int_fact_promocion values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",cargar_table_df(query_bg))
sql.commit()
