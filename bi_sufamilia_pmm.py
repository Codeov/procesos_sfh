import oracledb
import pyodbc as odbc
import pandas as pd

########variables########
usuario= "SINTERFACE"
contraseña= "SF5590X"
ip_server_name= "10.20.11.20/SPT01"
directory_co=r"D:\instaladores\biblioteca cliente oracle\instantclient_11_2"

##Retail_Stage
ip = '10.20.1.5'
bd= 'Retail_Stage'
us= 'operador'
pw= 'operador'

##Retail_DW
ip_dw = '10.20.1.5'
bd_dw= 'Retail_DW'
us_dw= 'operador'
pw_dw= 'operador'

subf_pmm = """
Select 
Distinct J.SFAM_LVL_NUMBER as cod_subfamilia, 
J.SFAM_DESC as desc_subfamilia,
J.FAM_LVL_NUMBER as cod_familia,
J.FAM_DESC as desc_familia,
 J.LIN_LVL_NUMBER as cod_linea,
J.LIN_DESC as desc_linea, 
J.SEC_LVL_NUMBER as cod_seccion, 
J.SEC_DESC as desc_seccion,
J.AREA_LVL_NUMBER as cod_area, 
J.AREA_DESC as desc_area, 
B.VALUE as cod_nac_unidas,
IFH_PKG_FUNC_PROD.FN_RETORNA_ATRIBUTO_DES(J.SFAM_LVL_CHILD,'HDRCATBY') cod_categoria_by, 
IFH_PKG_FUNC_PROD.FN_RETORNA_ATRIBUTO_DESC(J.SFAM_LVL_CHILD,'HDRCATBY') desc_categoria_by, 
--IFH_PKG_FUNC_PROD.FN_RETORNA_ATRIBUTO_DES(J.SFAM_LVL_CHILD,'HDRMUNCONS') "Cod Mundo Consumo",
--IFH_PKG_FUNC_PROD.FN_RETORNA_ATRIBUTO_DESC(J.SFAM_LVL_CHILD,'HDRMUNCONS') "Des Mundo Consumo" T.COD_MC "Cod Mundo Consumo", 
T.MUNDO_CONSUMO desc_mundo_consumo
From EDSR.IFHJRQMST J 
LEFT JOIN BASVALEE B 
ON B.TECH_KEY1 = J.SFAM_LVL_CHILD AND B.FIELD_CODE='NU' AND B.ENTITY_NAME='PRDMSTEE' 
LEFT JOIN EINTERFACE.IFHCATMUNDCONS T ON RTRIM(T.COD_CAT) = IFH_PKG_FUNC_PROD.FN_RETORNA_ATRIBUTO_DES(J.SFAM_LVL_CHILD,'HDRCATBY')
"""
subf_pmm_cab= """
Select 
Distinct J.SFAM_LVL_NUMBER as cod_subfamilia, 
J.SFAM_DESC as desc_subfamilia,
J.FAM_LVL_NUMBER as cod_familia,
J.FAM_DESC as desc_familia,
 J.LIN_LVL_NUMBER as cod_linea,
J.LIN_DESC as desc_linea, 
J.SEC_LVL_NUMBER as cod_seccion, 
J.SEC_DESC as desc_seccion,
J.AREA_LVL_NUMBER as cod_area, 
J.AREA_DESC as desc_area, 
B.VALUE as cod_nac_unidas,
IFH_PKG_FUNC_PROD.FN_RETORNA_ATRIBUTO_DES(J.SFAM_LVL_CHILD,'HDRCATBY') cod_categoria_by, 
IFH_PKG_FUNC_PROD.FN_RETORNA_ATRIBUTO_DESC(J.SFAM_LVL_CHILD,'HDRCATBY') desc_categoria_by, 
--IFH_PKG_FUNC_PROD.FN_RETORNA_ATRIBUTO_DES(J.SFAM_LVL_CHILD,'HDRMUNCONS') "Cod Mundo Consumo",
--IFH_PKG_FUNC_PROD.FN_RETORNA_ATRIBUTO_DESC(J.SFAM_LVL_CHILD,'HDRMUNCONS') "Des Mundo Consumo" T.COD_MC "Cod Mundo Consumo", 
T.MUNDO_CONSUMO desc_mundo_consumo
From EDSR.IFHJRQMST J 
LEFT JOIN BASVALEE B 
ON B.TECH_KEY1 = J.SFAM_LVL_CHILD AND B.FIELD_CODE='NU' AND B.ENTITY_NAME='PRDMSTEE' 
LEFT JOIN EINTERFACE.IFHCATMUNDCONS T ON RTRIM(T.COD_CAT) = IFH_PKG_FUNC_PROD.FN_RETORNA_ATRIBUTO_DES(J.SFAM_LVL_CHILD,'HDRCATBY')
where 1=2
"""
########poo########
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
            raise Exception ("error de conexion, detalle :" + str(e))
    def ejecutar_query(self,query):
        try:
            cursor=self.cnxo.cursor()
            cursor.execute(query)
            self.data=cursor.fetchall()
            return(self.data)
        except Exception as e:
            raise Exception ("error de ejecucion de query ora, detalle :" + str(e))
        
    def ejecutar_query_cab(self,query):
        try:
            cursor=self.cnxo.cursor()
            cursor.execute(query)
            self.cab = [col[0] for col in cursor.description]
            return(self.cab)
        except Exception as e:
            raise Exception ("error de ejecucion de query cab ora, detalle :" + str(e))
           
        
    def cerrar(self):
        try:
            self.close= self.cnxo.close()
            print("conexion to oracle cerrada")
            return self.close
        except Exception as e:
            print(e)
            return None
        
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
            return self.conn

        except Exception as e:
            raise Exception ("error de conexion de qsql, detalle :" + str(e))

    
    def _connection_string(self):

        SERVER= self.SERVER
        DATABASE=self.DATABASE
        UID=self.UID
        PWD=self.PWD
        autocommit= self.autocommit

        conn_string = 'DRIVER={SQL Server Native Client 11.0};SERVER='+SERVER+';DATABASE='+DATABASE+';UID='+UID+';PWD='+PWD+';autocommit='+str(autocommit)+''
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
            raise Exception ("error de ejecucion de query sql, detalle :" + str(e))
    
    def query_insert_tupla(self,sql_statement,tupla):
        if self.conn is None:
            print("conexion fallida")
            return
        try:
            cursor=self.conn.cursor()
            cursor.fast_executemany=True
            cursor.executemany(sql_statement,tupla)

        except Exception as e:
             raise Exception ("error de insercion tupla sql, detalle :" + str(e))

    def commit(self):
        if self.conn is None:
            print("no hay conexion")
            return
        try:
            self.conn.commit()
  
        except Exception as e:
            raise Exception ("error de commit sql, detalle :" + str(e))
    
    def cerrar(self):
        if self.conn is None:
            print("no hay conexion")
            return
        try:
            self.conn.close()
  
        except Exception as e:
            print(e)

########cargas########

def ins_data_df(query,cab):
   tupla=extension_oracle.ejecutar_query(subf_pmm)
   lst_data_hed=extension_oracle.ejecutar_query_cab(subf_pmm_cab)
   lst_data = [list(row) for row in tupla]
   df = pd.DataFrame(lst_data,columns=lst_data_hed)
   list_data_subf=df.values.tolist()
   return list_data_subf

def ins_data_cab_list_bd():
    extension_sql.query_insert_tupla("insert into tmp_subfamilia values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",ins_data_df(subf_pmm,subf_pmm_cab))
    extension_sql.commit()

oracledb.init_oracle_client(directory_co) 
extension_oracle=oracle(usuario,contraseña,ip_server_name)
extension_oracle.conectar_oracle()
extension_sql=SQLServer(ip,bd,us,pw)
extension_sql.connect_to_sql_server()

extension_sql.query("truncate table tmp_subfamilia")
ins_data_cab_list_bd()
extension_sql.cerrar()
extension_sql_dw=SQLServer(ip_dw,bd_dw,us_dw,pw_dw)
extension_sql_dw.connect_to_sql_server()
extension_sql_dw.query("exec ins_subf_pmm")
extension_sql.cerrar()


