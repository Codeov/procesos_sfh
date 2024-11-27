import pyodbc as odbc
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
        #conn_string = 'DRIVER={SQL Server Native Client 11.0};SERVER='+SERVER+';DATABASE='+DATABASE+';UID='+UID+';PWD='+PWD+';autocommit='+str(autocommit)+''
        
        return conn_string
    
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

    def query_return(self,query):
        try:
            cursor=self.conn.cursor()
            cursor.execute(query)
            self.data=[list(i) for i in cursor.fetchall()]
            return(self.data)
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


