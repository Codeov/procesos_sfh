import pyodbc as odbc
import pandas as pd
import datetime

#credenciales bi_45
ip = 'dwhsp.sql.azuresynapse.net'
bd= 'sqlpoolsp'
us= 'admdatamart'
pw= 'D4tXui7D/*-.5o0-\Q'

#fecha_inicial= '2023-09-01' 
#fecha_fin= '2023-09-25'


def q_dwh_fecha(inicial,final):
    query_dwh= f"""
    select a.fecha_venta fecha,sum(a.venta_neta) venta_neta from fact_data_trx a
    where a.fecha_venta between '{inicial}' and  '{final}'
    group by a.fecha_venta;
    """
    return query_dwh

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

    def query_return(self,sql_statement):
        if self.conn is None:
            print("conexion fallida")
            return
        try:
            cursor=self.conn.cursor()
            cursor.execute(sql_statement)
            self.data=cursor.fetchall()
            self.list=[list(row) for row in self.data]
            return self.list

        except Exception as e:
            print(e)
    
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

dwh=SQLServer(ip,bd,us,pw)
dwh.connect_to_sql_server()

def listar_fecha():
 fechas=dwh.query_return("select distinct cast(a.fecha as varchar(10)) fecha from sv_fechas a order by 1 asc")
 df=pd.DataFrame(data=fechas,columns=["fecha"])
 print(df)

def rep_salt():
 lst=dwh.query_return("select distinct cast(a.fecha as varchar(10)) fecha from sv_fechas a order by 1 asc")
 dfac_dwh_fecha=pd.DataFrame(data=[],columns=["fecha","venta_neta"])
 for fecha_dwh in lst:
     fecha_dwh_f="".join(fecha_dwh)
     df=pd.DataFrame(data=(dwh.query_return(q_dwh_fecha(fecha_dwh_f,fecha_dwh_f))),columns=["fecha","venta_neta"])
     dfac_dwh_fecha=pd.concat([dfac_dwh_fecha,df])
 print(dfac_dwh_fecha)
 listar_fecha()

def rep_seq():
 fecha_inicial=dwh.query_return("select distinct cast(min(a.fecha) as varchar(10)) fecha from sv_fechas a")
 fecha_final=dwh.query_return("select distinct cast(max(a.fecha) as varchar(10)) fecha from sv_fechas a")

 f_fecha_inicial="".join(fecha_inicial[0])
 f_fecha_final="".join(fecha_final[0])

 df=pd.DataFrame(data=(dwh.query_return(q_dwh_fecha(f_fecha_inicial,f_fecha_final))),columns=["fecha","venta_neta"])
 print(df)
 listar_fecha()

tip_rep= 'salt' #salt

if tip_rep=='seq':
    rep_seq()
elif tip_rep=='salt':
    rep_salt()

    

