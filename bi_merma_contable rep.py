import oracledb
import pandas as pd
import datetime
import time
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery
from email.message import EmailMessage
from smtplib import SMTP
import logging

usuario= "SINTERFACE"
contraseña= "SF5590X"
ip_server_name= "10.20.11.20/SPT01"
directory_co=r"D:\instaladores\biblioteca cliente oracle\instantclient_11_2"
ruta_file_json = r"D:\python\credenciales biq query\vta_linea_sistemas-bi-438c564c407c.json"


fecha_actual=datetime.datetime.now()
fecha_ini='2024-07-01'
fecha_fin='2024-07-01'
file_salida_loggin=rf"D:\BI\log_{fecha_ini}.txt"
print(file_salida_loggin)
logging.basicConfig(
    level=logging.DEBUG,  # Nivel de log
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Formato del mensaje
    filename=file_salida_loggin,  # Archivo de salida
    filemode='w'  # Modo de apertura del archivo (w para sobrescribir, a para agregar)
)

class oracle :
    def __init__(self,usuario,contraseña,dsn):
        self.usuario=usuario
        self.contraseña= contraseña
        self.dsn=dsn

    def conectar_oracle(self):
        self.cnxo=oracledb.connect(user=self.usuario,password=self.contraseña,dsn=self.dsn)
        print("conexion exitosa to oracle")
        return self.cnxo
    
    def ejecutar_query(self,query):
        cursor=self.cnxo.cursor()
        cursor.execute(query)
        self.data=cursor.fetchall()
        return(self.data)
        
    def ejecutar_query_cab(self,query):
        cursor=self.cnxo.cursor()
        cursor.execute(query)
        self.cab = [col[0] for col in cursor.description]
        return(self.cab)
        
    def cerrar(self):
        self.close= self.cnxo.close()
        print("conexion to oracle cerrada")
        return self.close
    
    def query_insert_tupla(self,sql_statement,tupla):
        cursor=self.cnxo.cursor()
        #cursor.fast_executemany=True
        cursor.executemany(sql_statement,tupla,
                batcherrors=True)

    def commit(self):
        self.cnxo.commit()

 ###################################################

logger = logging.getLogger('mi_logger')

format_ini=datetime.datetime.strptime(fecha_ini,"%Y-%m-%d")
fecha_fin=datetime.datetime.strptime(fecha_fin,"%Y-%m-%d")

periodo=fecha_ini
def enviar_correo (mensaje_error,mensaje_informe,asunto,ft):
  try:
    remitente = "over.salazar@spsa.pe"
    destinatario = ['over.salazar@spsa.pe']
    
 
    if ft=="error":
        mensaje = f"""Estimados por favor revisar el proceso de carga {asunto}, está cayendo con el siguiente error:<br><h2 style="color:red">{mensaje_error}</h2><br>Saludos<br>Over Salazar"""
    else:
        mensaje = f"""{mensaje_informe}<br><br>Saludos<br>Over Salazar"""
    email = EmailMessage()
    email["From"] = remitente
    email["To"] = destinatario
    email["Subject"] = asunto
    email.set_content(mensaje, subtype="html")

    smtp = SMTP(host="smtp.office365.com",port="587")
    smtp.starttls()
    smtp.login(remitente, "xbsjcjhsqwjfxvbm")
    smtp.sendmail(remitente, destinatario, email.as_string())
    smtp.quit()
    smtp.close()
  except Exception as e:
    print(e)
    raise Exception("Error en el envío de correo")

dff=pd.DataFrame(data=[],columns=["AUDIT_NUMBER","PERIODO","PRD_LVL_NUMBER","PRD_FULL_NAME","TRANS_UOM","TRANS_QTY","TRANS_COST","TRANS_EXT_COST","INV_TYPE_DESC","TBL_CODE_NAME","ORG_LVL_NUMBER","ORG_NAME_FULL","TRANS_DATE","POSTED_DATE_TIME","INV_MRPT_CODE","INV_MRPT_DESC","INV_DRPT_CODE","INV_DRPT_DESC","TRANS_REF","TRANS_REF2","COD_AREA","DES_AREA","COD_SEC","DES_SEC","COD_CAT","DES_CAT","COD_FAM","DES_FAM","COD_SFAM","DES_SFAM","TRANS_USER","VENDOR_NUMBER","VENDOR_NAME","PMG_PO_NUMBER","TRANS_SESSION","TRANS_SEQUENCE","COD_TIPNEG","FEC_INSERT","FIN_YEAR","FIN_PERIOD","FIN_WEEK"
    ])

def p_query(fin_year,fin_periodo):
    query_merma_contable=f"""
    select /*+ index(a) */
    AUDIT_NUMBER, PERIODO, PRD_LVL_NUMBER, PRD_FULL_NAME, TRANS_UOM, TRANS_QTY, TRANS_COST, TRANS_EXT_COST, INV_TYPE_DESC,
    TBL_CODE_NAME, ORG_LVL_NUMBER, ORG_NAME_FULL, TRANS_DATE, POSTED_DATE_TIME, INV_MRPT_CODE, INV_MRPT_DESC, INV_DRPT_CODE,
    INV_DRPT_DESC, TRANS_REF, TRANS_REF2, COD_AREA, DES_AREA, COD_SEC, DES_SEC, COD_CAT, DES_CAT, COD_FAM, DES_FAM, COD_SFAM, 
    DES_SFAM, TRANS_USER, VENDOR_NUMBER, VENDOR_NAME, PMG_PO_NUMBER, TRANS_SESSION, TRANS_SEQUENCE, COD_TIPNEG, FEC_INSERT, FIN_YEAR,
    FIN_PERIOD, FIN_WEEK
    from JST_MERMA_CONTABLE a 
    where fin_year={fin_year} and fin_period={fin_periodo}
    and org_lvl_number=1198
    and EXISTS
    (
    SELECT 'X'
    FROM SF_RSLYPWAE R
    WHERE R.YEAR={fin_year}
    AND R.PERIOD={fin_periodo}
    AND R.ORG_GL_CO IN ('SPSA','PVO','MKCO','PVVI','MSCO','JOKR')
    AND R.DOWNLOAD_DATE IS NOT NULL
    GROUP BY R.YEAR
    HAVING COUNT('X')=6
    )
    --and org_lvl_number=1198
    """
    return query_merma_contable

def req_ora (query):
    m_c=obj_ora.ejecutar_query(query)
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
try:
    logger.info('Conexion a la base de datos PMM')
    oracledb.init_oracle_client(directory_co)
    obj_ora=oracle(usuario,contraseña,ip_server_name)
    obj_ora.conectar_oracle()
    logger.info('Inicio de Iteracion Consulta')
    while format_ini <= fecha_fin:
        print(format_ini)
        param_an_ini=datetime.datetime.strftime(format_ini,"%Y")
        param_mes_ini=str(int(datetime.datetime.strftime(format_ini,"%m")))
        periodo_ini=param_an_ini+'-'+str(int(param_mes_ini))
        logger.info(f'Consulta de Merma Contable a PMM para el periodo {format_ini}')
        df_merma_contable=req_ora(p_query(param_an_ini,param_mes_ini))
        if df_merma_contable.empty!=True:
            dff=pd.concat([dff,(df_merma_contable)])
            logger.info(f'Se cargo la data a DataFrame para el periodo {format_ini}')
            format_ini=format_ini+relativedelta(months=1)
        else:
            logger.warning(f'No hay data de Merma Contable en PMM para el periodo {format_ini}')
            enviar_correo("",f"Estimados informar que aun no hay data de Merma Contable en la base de PMM para el periodo {format_ini} , se volverá a validar.","Merma Contable","informe")
            logger.warning(f'Se envió correo indicando que no hay data de Merma Contable en PMM para el periodo {format_ini}')
            logger.warning(f'Voliendo a validar el periodo {format_ini}')
            format_ini=format_ini+relativedelta(months=0)

    if dff.empty!=True:
        logger.info(f'Creando instancia big Query')
        obj_bq=bigquery.Client.from_service_account_json(ruta_file_json)
        logger.info(f'Limpiando tabla `sistemas-bi.SPSA_STAGE.tmp_merma_contable`')
        request_bigquery=obj_bq.query("truncate table `sistemas-bi.SPSA_STAGE.tmp_merma_contable`")
        request_bigquery.result()

        job_config= bigquery.LoadJobConfig (
                autodetect=True,
            )
        target_table_id='sistemas-bi.SPSA_STAGE.tmp_merma_contable'
        job_cab = obj_bq.load_table_from_dataframe(dff,target_table_id,job_config=job_config)
        while job_cab.state != 'DONE':
            time.sleep(1)
            job_cab.reload()
            print("se terminó de procesar - "+str(target_table_id)+' '+str(job_cab.result()))
        logger.info(f'Se termino de procesar la tabla `sistemas-bi.SPSA_STAGE.tmp_merma_contable`')

        request_bigquery=obj_bq.query("CALL `sistemas-bi.SPSA.ins_fact_merma_contable`()")
        request_bigquery.result()
        logger.info(f'Se termino de procesar el SP de merma contable `sistemas-bi.SPSA.ins_fact_merma_contable`()')
        enviar_correo("",f"Estimados informar que se termino de procesar la data de merma contable en BigQuery, periodo {periodo}","Merma Contable","informe")
        logger.info(f'Correo enviado, todo OK!!')
        obj_ora.cerrar()
        logger.info(f'Conexion de oracle cerrado')
except Exception as e:
    enviar_correo (e,"","Merma Contable","error")
    logger.error(f'Error en el proceso de carga:{e}')


   










