import datetime
import est_ora as ora
import est_bq as bq
import pandas as pd
import concurrent.futures
import threading
from email.message import EmailMessage
from smtplib import SMTP
import queue
import pytz
import io
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.identity import AzureCliCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from time import sleep

usuario= "SXCT2SP"
contraseña= "CT2remiX"
ip_server_name= "10.20.11.11/SPT03"

# Variables de configuración blob storage azure
connection_string = "DefaultEndpointsProtocol=https;AccountName=lkdatossp;AccountKey=zjC5uH8oWtl3zBTbegMHQ76/s9gqfwiic27sqyXc9Nbs8TfMRcUaUu4yf7hvO/J1RSbMbFo859W2XilrRuhi4Q==;EndpointSuffix=core.windows.net"
container_name = "blddatossp"
blob_name_cab = "venta/linea/cabecera/cabecera.csv"
blob_name_det = "venta/linea/detalle/detalle.csv"

# Variables de configuración data factory azure
subscription_id = '8df11707-3cc8-4f7f-a784-2ccbceee31b1'
resource_group_name = 'GRDatosSP'
data_factory_name = 'DFDatosSP'
pipeline_name = 'pipe_sql_vta_linea'
parameters = {}

c=150

zona_horaria_peru = pytz.timezone('America/Lima')
hora_actual=datetime.datetime.now(zona_horaria_peru).time()

formato_hora = "%H:%M:%S"
hora_1_1 = "00:00:00"
hora_1_1_f = datetime.datetime.strptime(hora_1_1, formato_hora).time()
hora_1_2 = "06:00:00"
hora_1_2_f = datetime.datetime.strptime(hora_1_2, formato_hora).time()

hora_2_1 = "13:30:00"
hora_2_1_f = datetime.datetime.strptime(hora_2_1, formato_hora).time()
hora_2_2 = "14:00:00"
hora_2_2_f = datetime.datetime.strptime(hora_2_2, formato_hora).time()

hora_3_1 = "19:30:00"
hora_3_1_f = datetime.datetime.strptime(hora_3_1, formato_hora).time()
hora_3_2 = "20:00:00"
hora_3_2_f = datetime.datetime.strptime(hora_3_2, formato_hora).time()


if hora_actual>=hora_1_1_f and hora_actual<=hora_1_2_f:
    fecha_actual_i=datetime.datetime.now()+datetime.timedelta(days=-1)
    flag_carga=2
elif hora_actual>=hora_2_1_f and hora_actual<=hora_2_2_f:
    fecha_actual_i=datetime.datetime.now()+datetime.timedelta(days=-1)
    flag_carga=2
elif hora_actual>=hora_3_1_f and hora_actual<=hora_3_2_f:
    fecha_actual_i=datetime.datetime.now()+datetime.timedelta(days=-1)
    flag_carga=2
else:
   fecha_actual_i=datetime.datetime.now()#+datetime.timedelta(days=-1)###
   flag_carga=1

fecha_actual_f_i=datetime.datetime.strftime(fecha_actual_i,"%Y%m%d")
fecha_actual_f=datetime.datetime.now()#+datetime.timedelta(days=-1)###
fecha_actual_f_f=datetime.datetime.strftime(fecha_actual_f,"%Y%m%d")

print(f"Fecha de Carga {fecha_actual_f_i}")
print(f"Fecha de Carga {fecha_actual_f_f}")

q_local="""
SELECT distinct l.loc_numero    AS cod_suc_pos
    FROM irs_locales       l
        ,mae_proceso_local p
        ,irs_cadenas i
   WHERE l.loc_numero = p.cod_local
   and l.cad_numero=i.cad_numero
     AND p.cod_proceso in (13,29,22)
     AND l.loc_activo = 'S'
     AND p.tip_estado = 'A'
     and l.loc_numero <=5000
     and i.cad_numero not in (14,5,11)
     --and l.loc_numero in (195,264,37,81,64)
     --and l.loc_numero in (195)
     order by 1 
"""

class ThreadWithException(threading.Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.exception = None

    def run(self):
        try:
            if self._target:
                self._target(*self._args, **self._kwargs)
        except Exception as e:
            self.exception = e

def r_q_upd_tabla_carga(fecha_i,fecha_f):
    q_upd_tabla_carga=f"""
    update EXCT2SP.ict2_trxs_vta_bi_group h
    set h.flg_envvta=1
    where h.hed_fcontable in (to_date({fecha_i},'yyyymmdd'),to_date({fecha_f},'yyyymmdd'))
    and h.flg_envvta=0
    and exists
                (SELECT 1
                    FROM 
                    (
                        select distinct a.hed_pais,a.hed_origentrx,a.hed_local,a.hed_pos,a.hed_numtrx,a.hed_fechatrx,a.hed_horatrx
                        from  EXCT2SP.ict2_trxs_vta_bi_group a
                        left join EXCT2SP.ICT2_VAL_CARGA_VTA_BI b
                        on a.hed_fcontable=b.hed_fcontable and a.hed_local=b.hed_local
                        WHERE a.flg_envvta = 0
                        AND a.hed_fcontable in (to_date({fecha_i},'yyyymmdd'),to_date({fecha_f},'yyyymmdd'))
                        and b.hed_fcontable is null
                    )i
                    WHERE h.hed_pais = i.hed_pais
                    AND h.hed_origentrx = i.hed_origentrx
                    AND h.hed_local = i.hed_local
                    AND h.hed_pos = i.hed_pos
                    AND h.hed_numtrx = i.hed_numtrx
                    AND h.hed_fechatrx = i.hed_fechatrx
                    AND h.hed_horatrx = i.hed_horatrx
                )
    """
    return q_upd_tabla_carga

def r_q_trx_carga(fecha_i,fecha_f,codigo_local):
    q_trx_carga=f"""
    SELECT /*+ index(h) */ h.hed_pais
            ,h.hed_origentrx
            ,h.hed_local
            ,h.hed_pos
            ,h.hed_numtrx
            ,h.hed_fechatrx
            ,h.hed_horatrx
            ,h.hed_fcontable
            ,h.hed_tipotrx
            ,h.hed_tipodoc
            ,h.hed_subtipo
            ,SYSDATE
            ,h.hed_anulado
        FROM ctx_header_trx h
            ,mae_transaccion m
        WHERE h.hed_fcontable in (to_date('{fecha_i}','yyyymmdd'),to_date('{fecha_f}','yyyymmdd'))
        AND h.hed_local = {codigo_local}
        AND h.hed_tipotrx = m.mae_tipotrx
        AND h.hed_tipodoc = m.mae_tipodoc
        AND m.mae_tipotrx = 'PVT'
        AND m.mae_estadobi = 1
        AND h.hed_anulado = 'N'
        AND NOT EXISTS
            (SELECT 1
                FROM EXCT2SP.ict2_trxs_vta_bi_group i
                WHERE h.hed_pais = i.hed_pais
                AND h.hed_origentrx = i.hed_origentrx
                AND h.hed_local = i.hed_local
                AND h.hed_pos = i.hed_pos
                AND h.hed_numtrx = i.hed_numtrx
                AND h.hed_fechatrx = i.hed_fechatrx
                AND h.hed_horatrx = i.hed_horatrx)
    """
    return q_trx_carga

def r_q_cabecera(fecha_i,fecha_f,codigo_local):
    q_cabecera=f"""
    SELECT /*+ index(h) */ to_char(h.hed_fcontable,'YYYY-MM-DD') fecha,
        h.hed_local codigo_local,
        h.hed_pos numero_terminal,
        h.hed_numtrx numero_transaccion,
        h.hed_tipotrx codigo_tipo_transaccion,
        h.hed_tipodoc codigo_tipo_comprobante,
        h.rpl_fecha_graba rpl_fecha_graba,
        'V' estado_transaccion,
        h.hed_horatrx fecha_hora
    FROM ctx_header_trx h
        ,EXCT2SP.ict2_trxs_vta_bi_group b
  WHERE
    h.hed_pais = b.hed_pais
    AND h.hed_origentrx = b.hed_origentrx
    AND h.hed_local = b.hed_local
    AND h.hed_pos = b.hed_pos
    AND h.hed_numtrx = b.hed_numtrx
    AND h.hed_fechatrx = b.hed_fechatrx
    AND h.hed_horatrx = b.hed_horatrx
    AND b.hed_tipotrx IN ('PVT')
    AND b.hed_anulado = 'N'
    AND b.hed_local = {codigo_local}
    AND b.hed_fcontable in (to_date('{fecha_i}','yyyymmdd'),to_date('{fecha_f}','yyyymmdd'))
    AND b.flg_envvta = 0
    """
    return q_cabecera

def r_q_detalle(fecha_i,fecha_f,codigo_local):
    q_detalle=f"""
    SELECT /*+ index(p) */ to_char(p.ptr_fcontable,'YYYY-MM-DD') fecha,
        p.hed_local codigo_local,
        p.hed_pos numero_terminal,
        p.hed_numtrx numero_transaccion,
        to_char(p.hed_horatrx,'hh24:mi:ss') hora,
        p.ptr_codinterno codigo_producto,
        (p.ptr_brutopos + p.ptr_brutoneg - p.ptr_mdescto) venta_total,
        (p.ptr_brutopos + p.ptr_brutoneg - p.ptr_mdescto) venta_bruta,
        (p.ptr_brutopos + p.ptr_brutoneg - nvl(p.ptr_impuesto,0) - nvl(p.ptr_impuesto_isc,0))  AS venta_neta,
        CASE
          WHEN to_number(p.ptr_codprod)
            IN (SELECT to_number(r.cod_ean)
                  FROM ict2_cod_recargas r)
          THEN  p.ptr_total
            ELSE p.ptr_unidades + p.ptr_peso END venta_unidad
    FROM EXCT2SP.ict2_trxs_vta_bi_group h
        ,ctx_productos_trx p
   WHERE 
     h.hed_pais = p.hed_pais
     AND h.hed_origentrx = p.hed_origentrx
     AND h.hed_local = p.hed_local
     AND h.hed_pos = p.hed_pos
     AND h.hed_numtrx = p.hed_numtrx
     AND h.hed_fechatrx = p.hed_fechatrx
     AND h.hed_horatrx = p.hed_horatrx
     AND h.hed_tipotrx = 'PVT'
     AND h.hed_tipodoc IN ('BLT','TFC','NCR')
     AND h.hed_local = {codigo_local}
     AND h.hed_fcontable in (to_date('{fecha_i}','yyyymmdd'),to_date('{fecha_f}','yyyymmdd'))
     AND h.flg_envvta = 0
    """
    return q_detalle

def listar_locales(obj_ora,query):
    tp_locales=obj_ora.ejecutar_query(query)
    ls_locales=[list(i) for i in tp_locales]
    return ls_locales

def procesar_cab_df(fecha_i,fecha_f,cod_local,obj_ora_s,q_c):
    query_trx_cab=r_q_cabecera(fecha_i,fecha_f,cod_local)
    tp_trx_cab=obj_ora_s.ejecutar_query(query_trx_cab)
    df_cab=pd.DataFrame(data=tp_trx_cab,columns=["FECHA","CODIGO_LOCAL","NUMERO_TERMINAL","NUMERO_TRANSACCION","CODIGO_TIPO_TRANSACCION","CODIGO_TIPO_COMPROBANTE","RPL_FECHA_GRABA","ESTADO_TRANSACCION","FECHA_HORA"])
    df_cab["FECHA"]=df_cab["FECHA"].map(str)
    df_cab["CODIGO_LOCAL"]=df_cab["CODIGO_LOCAL"].map(str)
    df_cab["NUMERO_TERMINAL"]=df_cab["NUMERO_TERMINAL"].map(str)
    df_cab["NUMERO_TRANSACCION"]=df_cab["NUMERO_TRANSACCION"].map(str)
    df_cab["CODIGO_TIPO_TRANSACCION"]=df_cab["CODIGO_TIPO_TRANSACCION"].map(str)
    df_cab["CODIGO_TIPO_COMPROBANTE"]=df_cab["CODIGO_TIPO_COMPROBANTE"].map(str)
    df_cab["RPL_FECHA_GRABA"]=df_cab["RPL_FECHA_GRABA"].map(str)
    df_cab["ESTADO_TRANSACCION"]=df_cab["ESTADO_TRANSACCION"].map(str)
    df_cab["FECHA_HORA"]=df_cab["FECHA_HORA"].map(str)
    q_c.put(df_cab)

def procesar_det_df(fecha_i,fecha_f,cod_local,obj_ora_s,q_d):
    query_trx_det=r_q_detalle(fecha_i,fecha_f,cod_local)
    tp_trx_det=obj_ora_s.ejecutar_query(query_trx_det)
    df_det=pd.DataFrame(data=tp_trx_det,columns=["FECHA","CODIGO_LOCAL","NUMERO_TERMINAL","NUMERO_TRANSACCION","HORA","CODIGO_PRODUCTO","VENTA_TOTAL","VENTA_BRUTA","VENTA_NETA","VENTA_UNIDAD"])
    df_det["FECHA"]=df_det["FECHA"].map(str)
    df_det["CODIGO_LOCAL"]=df_det["CODIGO_LOCAL"].map(str)
    df_det["NUMERO_TERMINAL"]=df_det["NUMERO_TERMINAL"].map(str)
    df_det["NUMERO_TRANSACCION"]=df_det["NUMERO_TRANSACCION"].map(str)
    df_det["HORA"]=df_det["HORA"].map(str)
    df_det["CODIGO_PRODUCTO"]=df_det["CODIGO_PRODUCTO"].map(str)
    df_det["VENTA_TOTAL"]=df_det["VENTA_TOTAL"].map(str)
    df_det["VENTA_BRUTA"]=df_det["VENTA_BRUTA"].map(str)
    df_det["VENTA_NETA"]=df_det["VENTA_NETA"].map(str)
    df_det["VENTA_UNIDAD"]=df_det["VENTA_UNIDAD"].map(str)
    q_d.put(df_det)

def enviar_correo (mensaje):
  try:
    remitente = "over.salazar@spsa.pe"
    destinatario = ['over.salazar@spsa.pe']
    
    mensaje = f"""Estimados por favor revisar el proceso de carga Venta en Linea BI, está cayendo con el siguiente error:<br><h2 style="color:red">{mensaje}</h2>Saludos<br>Over Salazar"""

    email = EmailMessage()
    email["From"] = remitente
    email["To"] = destinatario
    email["Subject"] = "Alerta Venta en Linea BI"
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

def procesar_local(fecha_actual_f_i,fecha_actual_f_f,cod_local):
    try:
        obj_ora_s=ora.oracle(usuario=usuario,contraseña=contraseña,dsn=ip_server_name)
        obj_ora_s.inicializar_oracle()
        obj_ora_s.conectar_oracle()
        query_trx_carga=r_q_trx_carga(fecha_actual_f_i,fecha_actual_f_f,cod_local)
        tp_trx=obj_ora_s.ejecutar_query(query_trx_carga)
        if tp_trx:
            obj_ora_s.query_insert_tupla("insert into EXCT2SP.ict2_trxs_vta_bi_group(hed_pais,hed_origentrx,hed_local,hed_pos,hed_numtrx,hed_fechatrx,hed_horatrx,hed_fcontable,hed_tipotrx,hed_tipodoc,hed_subtipo,fec_registro,hed_anulado) values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13)",tp_trx)
            obj_ora_s.commit()
            print(f"Inserción exitosa en ict2_trxs_vta_bi_group con fechas {fecha_actual_f_i} {fecha_actual_f_f}  y local {cod_local}")
            
            q_c=queue.Queue()
            q_d=queue.Queue()

            threads_l = []

            thread_c = ThreadWithException(target=procesar_cab_df, args=(fecha_actual_f_i,fecha_actual_f_f,cod_local,obj_ora_s,q_c))
            thread_d = ThreadWithException(target=procesar_det_df, args=(fecha_actual_f_i,fecha_actual_f_f,cod_local,obj_ora_s,q_d))
            thread_c.start()
            thread_d.start()
            threads_l.append(thread_c)
            threads_l.append(thread_d)

            for t in threads_l:
                t.join()
        
            for t in threads_l:
                if t.exception:
                    raise ValueError(t.exception)

            df_cab=q_c.get()
            df_det=q_d.get()

            obj_ora_s.cerrar()
            return 1,df_cab,df_det
        else:
            print(f"No hay data para cargar en ict2_trxs_vta_bi_group con fechas {fecha_actual_f_i} {fecha_actual_f_f}  y local {cod_local}")
            return 0,False,False
    except Exception as e:
        try:
            err=f"{e} para el local {cod_local} con fechas {fecha_actual_f_i} {fecha_actual_f_f}"
            print(err)
            enviar_correo (err)
            print(f"Error al procesar la data de los dias {fecha_actual_f_i}  {fecha_actual_f_f} y local {cod_local}")
            fecha_actual_f_s_i=datetime.datetime.strptime(datetime.datetime.strftime(datetime.datetime.strptime(fecha_actual_f_i,"%Y%m%d"),"%d/%m/%Y"),"%d/%m/%Y")
            fecha_actual_f_s_f=datetime.datetime.strptime(datetime.datetime.strftime(datetime.datetime.strptime(fecha_actual_f_f,"%Y%m%d"),"%d/%m/%Y"),"%d/%m/%Y")
            ls_v_fl_error=[]
            ls_a_fl_error=[]
            ls_v_fl_error.append(fecha_actual_f_s_i)
            ls_v_fl_error.append(cod_local)
            ls_v_fl_error.append(0)
            ls_a_fl_error.append(ls_v_fl_error)
            ls_v_fl_error=[]
            ls_v_fl_error.append(fecha_actual_f_s_f)
            ls_v_fl_error.append(cod_local)
            ls_v_fl_error.append(0)
            ls_a_fl_error.append(ls_v_fl_error)
            print(ls_a_fl_error)
            obj_ora_s.query_insert_tupla("insert into EXCT2SP.ICT2_VAL_CARGA_VTA_BI(hed_fcontable,hed_local,est_carga) values (:1,:2,:3)",ls_a_fl_error)
            obj_ora_s.commit()
            return 9999,False,False
        except Exception as e:
            return 9999,False,False
        
def cargar_df_bs_az(df,blob_name):
    output=io.StringIO()
    df.to_csv(output,index=False,sep="|")
    data=output.getvalue().encode("utf-8")
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(data, overwrite=True)
    print(f"archivo {blob_name} subido")

def procesar_pipeline_df_az():
    # Autenticación utilizando Azure CLI
    credential = AzureCliCredential()

    # Crear cliente de Azure Data Factory
    adf_client = DataFactoryManagementClient(credential, subscription_id)

    # Ejecutar pipeline
    run_response = adf_client.pipelines.create_run(
        resource_group_name=resource_group_name,
        factory_name=data_factory_name,
        pipeline_name=pipeline_name
    )

    # Obtener el ID de ejecución del pipeline
    run_id = run_response.run_id
    print(f"Pipeline ejecutado con run ID: {run_id}")

    # Monitorear el estado del pipeline (opcional)
    pipeline_run = adf_client.pipeline_runs.get(
        resource_group_name=resource_group_name,
        factory_name=data_factory_name,
        run_id=run_id
    )

    while pipeline_run.status not in ('Succeeded', 'Failed', 'Cancelled'):
        print(f"Estado actual: {pipeline_run.status}")
        sleep(3)  # Esperar 30 segundos antes de volver a chequear
        pipeline_run = adf_client.pipeline_runs.get(
            resource_group_name=resource_group_name,
            factory_name=data_factory_name,
            run_id=run_id
        )

    print(f"Estado final: {pipeline_run.status}")
    if pipeline_run.status=="Succeeded":
        return 1
    else:
        0

try:

    obj_ora_p=ora.oracle(usuario=usuario,contraseña=contraseña,dsn=ip_server_name)
    obj_ora_p.inicializar_oracle()
    obj_ora_p.conectar_oracle()
    obj_ora_p.ejecutar_query_sin_return("truncate table EXCT2SP.ICT2_VAL_CARGA_VTA_BI")
    
    df_cab_acum=pd.DataFrame(columns=["FECHA","CODIGO_LOCAL","NUMERO_TERMINAL","NUMERO_TRANSACCION","CODIGO_TIPO_TRANSACCION","CODIGO_TIPO_COMPROBANTE","RPL_FECHA_GRABA","ESTADO_TRANSACCION","FECHA_HORA"])
    df_det_acum=pd.DataFrame(columns=["FECHA","CODIGO_LOCAL","NUMERO_TERMINAL","NUMERO_TRANSACCION","HORA","CODIGO_PRODUCTO","VENTA_TOTAL","VENTA_BRUTA","VENTA_NETA","VENTA_UNIDAD"])
    
    ls_a_fl_error=[]    
    ls_loc=listar_locales(obj_ora_p,q_local)
    ls_loc_t=[]

    for i in ls_loc:
        cod_local=i[0]
        ls_loc_t.append(cod_local)

    if flag_carga==2:
        ls_fecha=[]
        ls_fecha.append(fecha_actual_f_i)
        ls_fecha.append(fecha_actual_f_f)
        for a in ls_fecha:
            print(f"inicio de procesamiento en df, dia {a}")
            with concurrent.futures.ThreadPoolExecutor(max_workers=c) as executor:
                resultados = [executor.submit(procesar_local,a,a,i) for i in ls_loc_t]

            for resultado in concurrent.futures.as_completed(resultados):
                f,dfc,dfd=resultado.result()
                if f==1:
                    df_cab_acum=pd.concat([df_cab_acum,dfc])
                    df_det_acum=pd.concat([df_det_acum,dfd])
            print(f"se termino de procesar los df, dia {a}")
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=c) as executor:
            resultados = [executor.submit(procesar_local,fecha_actual_f_i,fecha_actual_f_f,i) for i in ls_loc_t]

        for resultado in concurrent.futures.as_completed(resultados):
            f,dfc,dfd=resultado.result()
            if f==1:
                df_cab_acum=pd.concat([df_cab_acum,dfc])
                df_det_acum=pd.concat([df_det_acum,dfd])


    if df_cab_acum.empty == False and df_det_acum.empty == False:
        print("Si hay datos en los df acum cab y det para cargar al blob storage de azure ")

        threads_df = []
        pbqcab = ThreadWithException(target=cargar_df_bs_az,args=(df_cab_acum,blob_name_cab))
        pbqdet = ThreadWithException(target=cargar_df_bs_az,args=(df_det_acum,blob_name_det))
        pbqcab.start()
        pbqdet.start()
        threads_df.append(pbqcab)
        threads_df.append(pbqdet)

        for t in threads_df:
            t.join()
        
        for t in threads_df:
            if t.exception:
                raise ValueError(t.exception)
        
        f_pipeline=procesar_pipeline_df_az()
        if f_pipeline==1:
         obj_ora_p.ejecutar_query_sin_return(r_q_upd_tabla_carga(fecha_actual_f_i,fecha_actual_f_f))
         obj_ora_p.commit()
        else:
            enviar_correo ("El Pipeline de venta en linea no ha procesado con exito, por favor tu apoyo con la revisión.")
    else:
        print("No hay data en df por lo tanto no se procesa en Azure")
    obj_ora_p.cerrar()
except Exception as e:
    enviar_correo (e)


        

  


    

