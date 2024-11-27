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

usuario= "SXCT2SP"
contraseña= "CT2remiX"
ip_server_name= "10.20.11.11/SPT03"

c=150
    
fecha_actual_f_i=datetime.datetime.strftime(datetime.datetime.strptime('2024-06-16',"%Y-%m-%d"),"%Y%m%d")
fecha_actual_f_f=datetime.datetime.strftime(datetime.datetime.strptime('2024-06-16',"%Y-%m-%d"),"%Y%m%d")
##################################################################################
##################################################################################
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
    -- and l.loc_numero in (195)
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

def r_q_upd_tabla_carga(fecha_i):
    q_upd_tabla_carga=f"""
    update EXCT2SP.ict2_trxs_vtabi h
    set h.flg_envvta=2
    where h.hed_fcontable =to_date({fecha_i},'yyyymmdd')
    and h.flg_envvta=0
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
            ,2
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
                FROM EXCT2SP.ict2_trxs_vtabi i
                WHERE h.hed_pais = i.hed_pais
                AND h.hed_origentrx = i.hed_origentrx
                AND h.hed_local = i.hed_local
                AND h.hed_pos = i.hed_pos
                AND h.hed_numtrx = i.hed_numtrx
                AND h.hed_fechatrx = i.hed_fechatrx
                AND h.hed_horatrx = i.hed_horatrx)
    """
    return q_trx_carga

def listar_locales(obj_ora,query):
    tp_locales=obj_ora.ejecutar_query(query)
    ls_locales=[list(i) for i in tp_locales]
    return ls_locales

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
    obj_ora_s=ora.oracle(usuario=usuario,contraseña=contraseña,dsn=ip_server_name)
    obj_ora_s.inicializar_oracle()
    obj_ora_s.conectar_oracle()
    query_trx_carga=r_q_trx_carga(fecha_actual_f_i,fecha_actual_f_f,cod_local)
    #print(query_trx_carga)
    tp_trx=obj_ora_s.ejecutar_query(query_trx_carga)
    if tp_trx:
        obj_ora_s.query_insert_tupla("insert into EXCT2SP.ict2_trxs_vtabi(hed_pais,hed_origentrx,hed_local,hed_pos,hed_numtrx,hed_fechatrx,hed_horatrx,hed_fcontable,hed_tipotrx,hed_tipodoc,hed_subtipo,FLG_ENVVTA,fec_registro,hed_anulado) values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14)",tp_trx)
        obj_ora_s.commit()
        print(f"Inserción exitosa en ict2_trxs_vtabi con fechas {fecha_actual_f_i} {fecha_actual_f_f}  y local {cod_local}")
        obj_ora_s.cerrar()
    else:
        print(f"No hay data para cargar en ict2_trxs_vtabi con fechas {fecha_actual_f_i} {fecha_actual_f_f}  y local {cod_local}")
        obj_ora_s.cerrar()
    return True
    
try:
    
    obj_ora_p=ora.oracle(usuario=usuario,contraseña=contraseña,dsn=ip_server_name)
    obj_ora_p.inicializar_oracle()
    obj_ora_p.conectar_oracle()
    QAS="""
update EXCT2SP.ICT2_TRXS_VTA_BI_GROUP_DIARIO h
    set h.flg_envvta=1
"""
    obj_ora_p.ejecutar_query(QAS)
    obj_ora_p.commit()

    ls_a_fl_error=[]    
    ls_loc=listar_locales(obj_ora_p,q_local)
    ls_loc_t=[]

    for i in ls_loc:
        cod_local=i[0]
        ls_loc_t.append(cod_local)

    ls_fecha=[]
    ls_fecha.append(fecha_actual_f_i)
    for a in ls_fecha:
        print(f"inicio de procesamiento en ict2_trxs_vtabi, dia {a}")
        #procesar_local(a,a,'195')
        with concurrent.futures.ThreadPoolExecutor(max_workers=c) as executor:
            resultados = [executor.submit(procesar_local,a,a,i) for i in ls_loc_t]

        for resultado in concurrent.futures.as_completed(resultados):
            f=resultado.result()
            if f==True:
                print("exito")
        print(f"se termino de cargar la dataen ict2_trxs_vtabi, dia {a}")
    
    #obj_ora_p.ejecutar_query_sin_return(r_q_upd_tabla_carga(fecha_actual_f_i))
    #obj_ora_p.commit()

    obj_ora_p.cerrar()
except Exception as e:
    enviar_correo (e)


        

  


    

