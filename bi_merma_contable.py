import oracledb
import pandas as pd
import datetime
import time
from google.cloud import bigquery
from dateutil.relativedelta import relativedelta

usuario= "SINTERFACE"
contraseña= "SF5590X"
ip_server_name= "10.20.11.20/SPT01"
directory_co=r"D:\instaladores\biblioteca cliente oracle\instantclient_11_2"
ruta_file_json= r"D:\python\credenciales biq query\sistemas-bi-7a46b3894448.json"

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
            self.data=cursor.fetchall()
            return(self.data)
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

fecha_actual=(datetime.datetime.strftime(datetime.datetime.now(),'%m/%d/%Y'))
f_fecha_actual=(datetime.datetime.strptime(fecha_actual,'%m/%d/%Y'))- pd.DateOffset(months=1)

param_an=datetime.datetime.strftime(f_fecha_actual,"%Y")
param_mes=datetime.datetime.strftime(f_fecha_actual,"%m")

fin_year=param_an
fin_periodo=str(int(param_mes))
periodo=fin_year+str(param_mes)
print(f'{fin_year}\n{fin_periodo}')

query_merma_contable=f"""
 select /*+ index(a) */
    AUDIT_NUMBER, PERIODO, PRD_LVL_NUMBER, PRD_FULL_NAME, TRANS_UOM, TRANS_QTY, TRANS_COST, TRANS_EXT_COST, INV_TYPE_DESC,
    TBL_CODE_NAME, ORG_LVL_NUMBER, ORG_NAME_FULL, TRANS_DATE, POSTED_DATE_TIME, INV_MRPT_CODE, INV_MRPT_DESC, INV_DRPT_CODE,
    INV_DRPT_DESC, TRANS_REF, TRANS_REF2, COD_AREA, DES_AREA, COD_SEC, DES_SEC, COD_CAT, DES_CAT, COD_FAM, DES_FAM, COD_SFAM, 
    DES_SFAM, TRANS_USER, VENDOR_NUMBER, VENDOR_NAME, PMG_PO_NUMBER, TRANS_SESSION, TRANS_SEQUENCE, COD_TIPNEG, FEC_INSERT, FIN_YEAR,
    FIN_PERIOD, FIN_WEEK
    from JST_MERMA_CONTABLE a
    where fin_year='{fin_year}' and fin_period='{fin_periodo}'
    --and org_lvl_number=1198
"""

oracledb.init_oracle_client(directory_co)
obj_ora=oracle(usuario,contraseña,ip_server_name)
obj_ora.conectar_oracle()

def req_ora ():
    m_c=obj_ora.ejecutar_query(query_merma_contable)
    df=pd.DataFrame(data=m_c,columns=["AUDIT_NUMBER","PERIODO","PRD_LVL_NUMBER","PRD_FULL_NAME","TRANS_UOM","TRANS_QTY","TRANS_COST","TRANS_EXT_COST","INV_TYPE_DESC","TBL_CODE_NAME","ORG_LVL_NUMBER","ORG_NAME_FULL","TRANS_DATE","POSTED_DATE_TIME","INV_MRPT_CODE","INV_MRPT_DESC","INV_DRPT_CODE","INV_DRPT_DESC","TRANS_REF","TRANS_REF2","COD_AREA","DES_AREA","COD_SEC","DES_SEC","COD_CAT","DES_CAT","COD_FAM","DES_FAM","COD_SFAM","DES_SFAM","TRANS_USER","VENDOR_NUMBER","VENDOR_NAME","PMG_PO_NUMBER","TRANS_SESSION","TRANS_SEQUENCE","COD_TIPNEG","FEC_INSERT","FIN_YEAR","FIN_PERIOD","FIN_WEEK"
    ])
    df['AUDIT_NUMBER']=df['AUDIT_NUMBER'].map(str)
    df['PERIODO']=df['PERIODO'].map(str)
    df['PRD_LVL_NUMBER']=df['PRD_LVL_NUMBER'].map(str)
    df['PRD_FULL_NAME']=df['PRD_FULL_NAME'].map(str)
    df['TRANS_UOM']=df['TRANS_UOM'].map(str)
    df['TRANS_QTY']=df['TRANS_QTY'].map(str)
    df['TRANS_COST']=df['TRANS_COST'].map(str)
    df['TRANS_EXT_COST']=df['TRANS_EXT_COST'].map(str)
    df['INV_TYPE_DESC']=df['INV_TYPE_DESC'].map(str)
    df['TBL_CODE_NAME']=df['TBL_CODE_NAME'].map(str)
    df['ORG_LVL_NUMBER']=df['ORG_LVL_NUMBER'].map(str)
    df['ORG_NAME_FULL']=df['ORG_NAME_FULL'].map(str)
    df['TRANS_DATE']=df['TRANS_DATE'].map(str)
    df['POSTED_DATE_TIME']=df['POSTED_DATE_TIME'].map(str)
    df['INV_MRPT_CODE']=df['INV_MRPT_CODE'].map(str)
    df['INV_MRPT_DESC']=df['INV_MRPT_DESC'].map(str)
    df['INV_DRPT_CODE']=df['INV_DRPT_CODE'].map(str)
    df['INV_DRPT_DESC']=df['INV_DRPT_DESC'].map(str)
    df['TRANS_REF']=df['TRANS_REF'].map(str)
    df['TRANS_REF2']=df['TRANS_REF2'].map(str)
    df['COD_AREA']=df['COD_AREA'].map(str)
    df['DES_AREA']=df['DES_AREA'].map(str)
    df['COD_SEC']=df['COD_SEC'].map(str)
    df['DES_SEC']=df['DES_SEC'].map(str)
    df['COD_CAT']=df['COD_CAT'].map(str)
    df['DES_CAT']=df['DES_CAT'].map(str)
    df['COD_FAM']=df['COD_FAM'].map(str)
    df['DES_FAM']=df['DES_FAM'].map(str)
    df['COD_SFAM']=df['COD_SFAM'].map(str)
    df['DES_SFAM']=df['DES_SFAM'].map(str)
    df['TRANS_USER']=df['TRANS_USER'].map(str)
    df['VENDOR_NUMBER']=df['VENDOR_NUMBER'].map(str)
    df['VENDOR_NAME']=df['VENDOR_NAME'].map(str)
    df['PMG_PO_NUMBER']=df['PMG_PO_NUMBER'].map(str)
    df['TRANS_SESSION']=df['TRANS_SESSION'].map(str)
    df['TRANS_SEQUENCE']=df['TRANS_SEQUENCE'].map(str)
    df['COD_TIPNEG']=df['COD_TIPNEG'].map(str)
    df['FEC_INSERT']=df['FEC_INSERT'].map(str)
    df['FIN_YEAR']=df['FIN_YEAR'].map(str)
    df['FIN_PERIOD']=df['FIN_PERIOD'].map(str)
    df['FIN_WEEK']=df['FIN_WEEK'].map(str)

    return df

obj_bq=bigquery.Client.from_service_account_json(ruta_file_json)
request_bigquery=obj_bq.query("delete `sistemas-bi.SPSA.tmp_merma_contable` where true")
request_bigquery.result()

job_config= bigquery.LoadJobConfig (
        autodetect=True,
        #write_disposition='WRITE_TRUNCATE'
    )
target_table_id='sistemas-bi.SPSA.tmp_merma_contable'
job_cab = obj_bq.load_table_from_dataframe(req_ora(),target_table_id,job_config=job_config)
while job_cab.state != 'DONE':
 time.sleep(1)
 job_cab.reload()
 print("se terminó de procesar - "+str(target_table_id)+' '+str(job_cab.result())+" con periodo= "+str(periodo))

request_bigquery=obj_bq.query("CALL `sistemas-bi.SPSA.ins_fact_merma_contable`()")
request_bigquery.result()
print("se terminó de procesar - fact_merma_contable ")

obj_ora.cerrar()