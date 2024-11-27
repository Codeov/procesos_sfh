import oracledb

directory_co=r"D:\instaladores\biblioteca cliente oracle\instantclient_11_2"
#directory_co=r"D:\instaladores\biblioteca cliente oracle\instantclient_11_2"

class oracle :
    def __init__(self,usuario,contraseña,dsn):
        self.usuario=usuario
        self.contraseña= contraseña
        self.dsn=dsn

    def inicializar_oracle(self):
        oracledb.init_oracle_client(directory_co)
        return True
 
    def conectar_oracle(self):
        self.cnxo=oracledb.connect(user=self.usuario,password=self.contraseña,dsn=self.dsn)
        return self.cnxo

    def ejecutar_query(self,query):
        cursor=self.cnxo.cursor()
        cursor.execute(query)
        self.data=cursor.fetchall()
        return(self.data)

    def ejecutar_query_sin_return(self,query):
        cursor=self.cnxo.cursor()
        cursor.execute(query)
        return True
        
    def ejecutar_query_cab(self,query):
        cursor=self.cnxo.cursor()
        cursor.execute(query)
        self.cab = [col[0] for col in cursor.description]
        return(self.cab)

    def cerrar(self):
        self.close= self.cnxo.close()
        return self.close
    
    def query_insert_tupla(self,sql_statement,tupla):
        cursor=self.cnxo.cursor()
        cursor.executemany(sql_statement,tupla,
                batcherrors=True)
        
    def commit(self):    
        self.cnxo.commit()
  


