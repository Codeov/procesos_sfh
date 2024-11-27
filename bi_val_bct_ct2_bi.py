import pandas as pd
from sys import path
import datetime
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery
from est_ora import *
from est_sql import *
import time
import threading as th
from email.message import EmailMessage
from smtplib import SMTP,SMTP_SSL

#credenciales ct2
usuario= "SXCT2SP"
contraseña= "CT2remiX"
ip_server_name= "10.20.11.11/SPT03"

#credenciales bct
ip = '10.20.11.13\sqlspsav2'
bd= 'ASR_CLA'
us= 'adm_spsa'
pw= 'BCT2015sps@'

directory_co=r"D:\instaladores\biblioteca cliente oracle\instantclient_11_2"
ruta_json_pbi= r'D:\python\credenciales biq query\spsa-sistemasbi-powerbi-e0770fbf3fa0.json'

now=datetime.date.today()
year=now.year
month=now.month

month_now = datetime.date(year + int(month/ 12), (month % 12) + 1, 1) 
start_month_cubo = month_now - relativedelta(months=1)
end_month_cubo = now#datetime.date(year + int(month/ 12), (month % 12) + 1, 1) - datetime.timedelta(days=1)

print(start_month_cubo)
print(end_month_cubo)
start_month_ct2=start_month_cubo
end_month_ct2=end_month_cubo

start_month_bct=start_month_cubo
end_month_bct=end_month_cubo

def load_table_tmp_cubo_az(df):
  ins_pbi=bigquery.Client.from_service_account_json(ruta_json_pbi)
  request_pbi=ins_pbi.query("delete `spsa-sistemasbi-powerbi.BI_VALIDADORES.tmp_cubo_az` where true",location="us")
  request_pbi.result()
  struct_ins_pbi = bigquery.LoadJobConfig (
         autodetect=True,
     )
  table_id_pbi='spsa-sistemasbi-powerbi.BI_VALIDADORES.tmp_cubo_az'
  job_ins_pbi = ins_pbi.load_table_from_dataframe(df,table_id_pbi,job_config=struct_ins_pbi)
  while job_ins_pbi.state != 'DONE':
      time.sleep(0.1)
      job_ins_pbi.reload()
  print("se terminó de procesar - "+str(table_id_pbi)+' '+str(job_ins_pbi.result()))

def load_table_tmp_data_ct2(df):
  ins_pbi=bigquery.Client.from_service_account_json(ruta_json_pbi)
  request_pbi=ins_pbi.query("delete `spsa-sistemasbi-powerbi.BI_VALIDADORES.tmp_data_ct2` where true",location="us")
  request_pbi.result()
  struct_ins_pbi = bigquery.LoadJobConfig (
         autodetect=True,
     )
  table_id_pbi='spsa-sistemasbi-powerbi.BI_VALIDADORES.tmp_data_ct2'
  job_ins_pbi = ins_pbi.load_table_from_dataframe(df,table_id_pbi,job_config=struct_ins_pbi)
  while job_ins_pbi.state != 'DONE':
      time.sleep(0.1)
      job_ins_pbi.reload()
  print("se terminó de procesar - "+str(table_id_pbi)+' '+str(job_ins_pbi.result()))

def load_table_tmp_data_bct(df):
  ins_pbi=bigquery.Client.from_service_account_json(ruta_json_pbi)
  request_pbi=ins_pbi.query("delete `spsa-sistemasbi-powerbi.BI_VALIDADORES.tmp_data_bct` where true",location="us")
  request_pbi.result() 
  struct_ins_pbi = bigquery.LoadJobConfig (
         autodetect=True,
     )
  table_id_pbi='spsa-sistemasbi-powerbi.BI_VALIDADORES.tmp_data_bct'
  job_ins_pbi = ins_pbi.load_table_from_dataframe(df,table_id_pbi,job_config=struct_ins_pbi)
  while job_ins_pbi.state != 'DONE':
      time.sleep(0.1)
      job_ins_pbi.reload()
  print("se terminó de procesar - "+str(table_id_pbi)+' '+str(job_ins_pbi.result()))

def data_cupo_az (start_month,end_month):
  path.append(r'C:\Program Files\Microsoft Office\root\vfs\ProgramFilesX64\Microsoft.NET\ADOMD.NET\130')
  from pyadomd import Pyadomd
  connection_string=r'Data Source=asazure://aspaaseastus2.asazure.windows.net/spanlsrvprd01:rw;Initial Catalog=SPSA_Cubo_Venta;User ID=admdatamartsp@intercorpretail.pe;Password=D4tXui7D/*-.5o0-\Q;Persist Security Info=True;Impersonation Level=Impersonate'
  dff=pd.DataFrame(data=[],columns=["codigo_sap","venta_neta","venta_vale","costo","fecha"])
  while start_month <= end_month :
    con=Pyadomd(connection_string)
    con.open()
    fecha_format=datetime.datetime.strftime(start_month,"%d/%m/%Y")
    day=start_month.weekday()
    if day==0:
     day="Lunes"
    elif day==1 :
      day="Martes"
    elif day==2 :
      day="Miércoles"
    elif day==3 :
      day="Jueves"
    elif day==4 :
      day="Viernes"
    elif day==5 :
      day="Sábado"
    elif day==6 :
      day="Domingo"
    fecha_request=(str(day)+" "+str(fecha_format))
    dax_query= """
        select 
        {
          [Measures].[Venta Neta],   
          [Measures].[Venta Vale],
          [Measures].[Costo]
        } on columns,
        non empty
        {
        [dim_local].[Codigo Local].[Codigo Local]
        } on rows
        from [Modelo]
        where [dim_tiempo].[Calendario Mensual].[Dia].&["""+f"""{fecha_request}"""+"""]"""
     
    result=con.cursor().execute(dax_query)
    df=pd.DataFrame(result.fetchone(),columns=["codigo_sap","venta_neta","venta_vale","costo"])
    df['fecha']=str(start_month)
    df['codigo_sap'] = df["codigo_sap"].map(str)
    df['venta_neta'] = df["venta_neta"].map(str)
    df['venta_vale'] = df["venta_vale"].map(str)
    df['costo'] = df["costo"].map(str)
    df['fecha'] = df["fecha"].map(str)
    dff=pd.concat([dff,df])
    print(fecha_request)
    start_month=start_month+datetime.timedelta(days=1)
    con.close()
  return dff

def query_ct2 (fecha):
  q_ct2=f"""
  SELECT
  to_char(h.hed_fcontable,'YYYY-MM-DD') fecha,
  --h.hed_fcontable fecha,
  h.hed_tipodoc tipo_doc,
  h.hed_local codigo_alterno,
  SUM(h.hed_brutopos + h.hed_brutoneg - nvl(h.hed_impuesto,0)- nvl(h.hed_impuesto_isc,0)) venta_neta
  FROM ctx_header_trx h
  WHERE h.hed_tipotrx = 'PVT'
  AND h.hed_fcontable = to_date('{fecha}','YYYYMMDD')
  and h.hed_local < 5000
  and h.hed_local not in (20,21,919)
  AND h.hed_tipodoc IN ('BLT','TFC','NCR')
  AND h.hed_anulado = 'N' 
  GROUP BY h.hed_fcontable,h.hed_tipodoc,h.hed_local
  """
  return q_ct2

def query_bct(fecha):
  q_bct=f"""
  select substring(cast(a.budate as varchar(10)),1,4)+'-'+substring(cast(a.budate as varchar(10)),5,2)+'-'+substring(cast(a.budate as varchar(10)),7,2) fecha
  ,branchId codigo_alterno,sum(amount-IGVTotalAmount-ISCTotalAmount) venta_neta 
  from trxheader  a with (nolock)
  where a.budate  = '{fecha}'
  and a.branchid  <5000
  and a.branchid not in (20,21,919)
  and a.amount<>0
  and a.idTransactionType in (10,56,2)
  and a.electronicSeries is not null
  group by a.budate,branchId
  """
  return q_bct

def data_ct2(start_month_ct2,end_month_ct2):
  oracledb.init_oracle_client(directory_co) 
  extension_oracle=oracle(usuario,contraseña,ip_server_name)
  extension_oracle.conectar_oracle()
  dff_ct2=pd.DataFrame(data=[],columns=["fecha","tipo_doc","codigo_alterno","venta_neta"])

  while start_month_ct2<=end_month_ct2:
    var_fecha_ct2=datetime.datetime.strftime(start_month_ct2,'%Y%m%d')
    df_ct2=extension_oracle.ejecutar_query(query_ct2(var_fecha_ct2))
    dfv_ct2=pd.DataFrame(data=df_ct2,columns=["fecha","tipo_doc","codigo_alterno","venta_neta"])
    dfv_ct2['fecha'] = dfv_ct2["fecha"].map(str)
    dfv_ct2['tipo_doc'] = dfv_ct2["tipo_doc"].map(str)
    dfv_ct2['codigo_alterno'] = dfv_ct2["codigo_alterno"].map(str)
    dfv_ct2['venta_neta'] = dfv_ct2["venta_neta"].map(str)
    dff_ct2=pd.concat([dff_ct2,dfv_ct2])
    print(f"fecha ct2 {var_fecha_ct2}")
    start_month_ct2=start_month_ct2+datetime.timedelta(days=1)

  extension_oracle.cerrar()
  return dff_ct2

def data_bct(start_month_bct,end_month_bct):
  extension_sql=SQLServer(ip,bd,us,pw)
  extension_sql.connect_to_sql_server()
  dff_bct=pd.DataFrame(data=[],columns=["fecha","codigo_alterno","venta_neta"])
  while start_month_bct<=end_month_bct:
    var_fecha_bct=datetime.datetime.strftime(start_month_bct,'%Y%m%d')
    df_bct=extension_sql.query_return(query_bct(var_fecha_bct))
    dfv_bct=pd.DataFrame(data=df_bct,columns=["fecha","codigo_alterno","venta_neta"])
    dfv_bct['fecha']=dfv_bct['fecha'].map(str)
    dfv_bct['codigo_alterno']=dfv_bct['codigo_alterno'].map(str)
    dfv_bct['venta_neta']=dfv_bct['venta_neta'].map(str)
    dff_bct=pd.concat([dff_bct,dfv_bct])
    print(f"fecha bct {var_fecha_bct}")
    start_month_bct=start_month_bct+datetime.timedelta(days=1)
  extension_sql.cerrar()
  return dff_bct

def carga_cubo_az():
  try:
    load_table_tmp_cubo_az(data_cupo_az(start_month_cubo,end_month_cubo))
  except Exception as e:
    print(e)
    raise Exception("Falló en la carga Cubo Azure")
  
def carga_data_ct2():
  try:
    load_table_tmp_data_ct2(data_ct2(start_month_ct2,end_month_ct2))
  except Exception as e:
    print(e)
    raise Exception("Falló en la carga Data CT2")
  
def carga_data_bct():
  try:
    load_table_tmp_data_bct(data_bct(start_month_bct,end_month_bct))
  except Exception as e:
    print(e)
    raise Exception("Falló en la carga Data BCT")
  

def sp_procesar_dif_bct_ct2_bi():
  try:
    ins_pbi=bigquery.Client.from_service_account_json(ruta_json_pbi)
    request_pbi=ins_pbi.query("call `spsa-sistemasbi-powerbi.BI_VALIDADORES.sp_procesar_dif_bct_ct2_bi` ()",location="us")
    request_pbi.result()
    print(" Se terminó de procesar las diferencias de BCT , CT2 y BI en Big Query")
  except Exception as e:
    print(e)
    raise Exception ("Error al procesar las diferencias de BCT , CT2 y BI")

def df_tabla_val (flag_val):
  query_dif_bct_ct2_bi="""
   select a.*
  from `spsa-sistemasbi-powerbi.BI_VALIDADORES.dif_venta_neta_bct_bi_ct2` a
  where a.Diferencia_BCT_CT2 not between -6000 and 6000
  or a.Diferencia_CT2_BI not between -5 and 5
  order by 1 asc
  """
  query_dif_bct_ct2="""
  select a.fecha,a.Venta_Neta_BCT,a.Venta_Neta_CT2,a.Diferencia_BCT_CT2
  from `spsa-sistemasbi-powerbi.BI_VALIDADORES.dif_venta_neta_bct_bi_ct2` a
  where a.Diferencia_BCT_CT2 not between -6000 and 6000
  order by 1 asc
  """
  query_dif_ct2_bi="""
  select a.fecha,a.Venta_Neta_CT2,a.Venta_Neta_BI,a.Diferencia_CT2_BI
  from `spsa-sistemasbi-powerbi.BI_VALIDADORES.dif_venta_neta_bct_bi_ct2` a
  where a.Diferencia_CT2_BI not between -5 and 5
  order by 1 asc
  """
  query_dif_tv_cubo_az="""
  select a.* from
  `spsa-sistemasbi-powerbi.BI_VALIDADORES.dif_venta_neta_tv_cubo_az` a
  where diferencia not between -1 and 1 
  order by 1 asc
  """
  req_clt=bigquery.Client.from_service_account_json(ruta_json_pbi)
  if flag_val=="ct2_bi":
    request_bi_ct2=req_clt.query(query_dif_ct2_bi,location="us")
    request_bi_ct2.result()
    df_bi_ct2=request_bi_ct2.to_dataframe()
    return df_bi_ct2
  elif flag_val=="bct_ct2":
    request_bct_ct2=req_clt.query(query_dif_bct_ct2,location="us")
    request_bct_ct2.result()
    df_bct_ct2=request_bct_ct2.to_dataframe()
    return df_bct_ct2
  elif flag_val=="tv_cubo_az" :
    request_tv_cubo_az=req_clt.query(query_dif_tv_cubo_az,location="us")
    request_tv_cubo_az.result()
    df_tv_cubo_az=request_tv_cubo_az.to_dataframe()
    return df_tv_cubo_az
  elif flag_val=="bct_ct2_bi":
    request_bct_ct2_bi=req_clt.query(query_dif_bct_ct2_bi,location="us")
    request_bct_ct2_bi.result()
    df_request_bct_ct2_bi=request_bct_ct2_bi.to_dataframe()
    return df_request_bct_ct2_bi
  else :
    raise Exception ("Parametros de validación Incorrectos - validar request df")

def val_data_bct_ct2_bi():
 df_bct_ct2=df_tabla_val("bct_ct2")
 df_ct2_bi=df_tabla_val("ct2_bi")
 df_bct_ct2_bi=df_tabla_val("bct_ct2_bi")
 if df_bct_ct2_bi.empty!=True: #df_bct_ct2.empty!=True and df_ct2_bi.empty!=True:
    flag_val_p="bct_ct2_bi"
    return flag_val_p,df_bct_ct2_bi
 elif df_bct_ct2.empty!=True and df_ct2_bi.empty==True:
    flag_val_p="bct_ct2"
    return flag_val_p,df_bct_ct2
 elif df_bct_ct2.empty==True and df_ct2_bi.empty!=True:
    flag_val_p="ct2_bi"
    return flag_val_p,df_ct2_bi
 elif df_bct_ct2_bi.empty==True: #df_bct_ct2.empty==True and df_ct2_bi.empty==True:
    flag_val_p="not_bct_ct2_bi"
    return flag_val_p,df_bct_ct2_bi
 else :
    raise Exception ("Parametros de validación Incorrectos - validar data en df-p")

def val_data_tv_az():
  df_tv_az=df_tabla_val("tv_cubo_az")
  if df_tv_az.empty!=True:
    flag_val_s="tv_az"
    return flag_val_s,df_tv_az
  elif df_tv_az.empty==True:
    flag_val_s="not_tv_az"
    return flag_val_s,df_tv_az
  else :
    raise Exception ("Parametros de validación Incorrectos - validar data en df-s")
  
def val_load_html(flag_val_p,df_p,flag_val_s,df_s,hora):
  if df_p.empty!=True:
   df_p=df_p.to_html(index=False,col_space=150,justify='center',border=3).replace('<td>', '<td align="right">')
  if df_s.empty!=True:
   df_s=df_s.to_html(index=False,col_space=150,justify='center',border=3).replace('<td>', '<td align="right">')
  
  primer_subt_bct_ct2_bi=f"""
  <h3>
                <center>
                  <hr>Diferencias BCT-CT2-BI<hr/>
                </center>
              </h3>
  """
  primer_subt_bct_ct2=f"""
  <h3>
                <center>
                  <hr>Diferencias BCT-CT2<hr/>
                </center>
              </h3>
  """
  primer_subt_ct2_bi=f"""
  <h3>
                <center>
                  <hr>Diferencias CT2-BI<hr/>
                </center>
              </h3>
  """
  titulo_left="""
              <h1 style="color:rgb(238, 19, 19)">Cuadratura de Venta BI!!!</h1>
              """
  hour_of_request=f"""
                  <h4 style="text-align: right;">Request : {hora}</h4>
                  """
  primer_block=f"""
              <center>
                <div class="row">
                  {df_p}
                </div>
              </center>
              <h5 style="text-align: right;">Nota: No incluye Venta Vale</h5>
              """
  second_block=f"""
              <h3>
                <center>
                  <hr>Diferencias Ticket Venta & Cubo de Venta Power BI<hr/>
                </center>
              </h3>
              <center>
                <div class="row">
                  {df_s}
                </div>
              <center>
              <h5 style="text-align: right;">Nota: Sí incluye Venta Vale</h5>
              """
  aviso=f"""
              <p>Estimados su apoyo con la coordinacion y reproceso urgente...</p>
               """
  footer=f"""
              <center>
                <footer><p><h4>Sistemas BI 2024© - BST</h4></p></footer>
              </center>
  """
  
  if flag_val_p=="bct_ct2_bi" and flag_val_s=="tv_az":
    html=f"""
          <html>
            <head>
            </head>
          <body>
           {titulo_left}
           {hour_of_request}
           {primer_subt_bct_ct2_bi}
           {primer_block}
           {second_block}
           {aviso}
           {footer}
          </body>
          """
    print(html)
    return html
  elif  flag_val_p=="bct_ct2" and flag_val_s=="tv_az":
    html=f"""
          <html>
            <head>
            </head>
          <body>
           {titulo_left}
           {hour_of_request}
           {primer_subt_bct_ct2}
           {primer_block}
           {second_block}
           {aviso}
           {footer}
          </body>
          """
    print(html)
    return html
  elif  flag_val_p=="ct2_bi" and flag_val_s=="tv_az":
    html=f"""
          <html>
            <head>
            </head>
          <body>
           {titulo_left}
           {hour_of_request}
           {primer_subt_ct2_bi}
           {primer_block}
           {second_block}
           {aviso}
           {footer}
          </body>
          """
    print(html)
    return html
  elif flag_val_p=="bct_ct2_bi" and flag_val_s=="not_tv_az":
    html=f"""
          <html>
            <head>
            </head>
          <body>
           {titulo_left}
           {hour_of_request}
           {primer_subt_bct_ct2_bi}
           {primer_block}
           {aviso}
           {footer}
          </body>
          """
    print(html)
    return html
  elif  flag_val_p=="bct_ct2" and flag_val_s=="not_tv_az":
    html=f"""
          <html>
            <head>
            </head>
          <body>
           {titulo_left}
           {hour_of_request}
           {primer_subt_bct_ct2}
           {primer_block}
           {footer}
          </body>
          """
    print(html)
    return html
  elif  flag_val_p=="ct2_bi" and flag_val_s=="not_tv_az":
    html=f"""
          <html>
            <head>
            </head>
          <body>
           {titulo_left}
           {hour_of_request}
           {primer_subt_ct2_bi}
           {primer_block}
           {aviso}
           {footer}
          </body>
          """
    print(html)
    return html
  elif  flag_val_p=="not_bct_ct2_bi" and flag_val_s=="tv_az":
    html=f"""
          <html>
            <head>
            </head>
          <body>
           {titulo_left}
           {hour_of_request}
           {second_block}
           {aviso}
           {footer}
          </body>
          """
    print(html)
    return html
  elif  flag_val_p=="not_bct_ct2_bi" and flag_val_s=="not_tv_az":
    return None
  else:
    raise Exception ("Parametros de validación Incorrectos - validar y procesar HTML")
  
def enviar_correo (html):
  try:
    remitente = "over.salazar@spsa.pe"
    destinatario = ['jose.carmona-pry@spsa.pe','marlon.cabrera@spsa.pe','juan.medina@spsa.pe','elvis.velasquez@spsa.pe','beatriz.vasquez@spsa.pe','avanza-magalli.vera@spsa.pe','creantis-bryan.palomino@spsa.pe','avanza-soportebi01@spsa.pe','creantis-jose.lamela@spsa.pe','jenny.aguirre@spsa.pe','ana.nunez@spsa.pe','piero.ibanez@spsa.pe','roberto.sologuren@spsa.pe']
    #destinatario = ['over.salazar@spsa.pe','jose.carmona-pry@spsa.pe','marlon.cabrera@spsa.pe','creantis-jose.lamela@spsa.pe','creantis-bryan.palomino@spsa.pe','avanza-soportebi01@spsa.pe']
    #destinatario = ['over.salazar@spsa.pe']
    
    print(destinatario)
    mensaje = html

    email = EmailMessage()
    email["From"] = remitente
    email["To"] = destinatario
    email["Subject"] = "Diferencias de Venta BI"
    email.set_content(mensaje, subtype="html")

    smtp = SMTP(host="smtp.office365.com",port="587")
    smtp.starttls()
    smtp.login(remitente, "kgsndqcvltqhlslq")
    smtp.sendmail(remitente, destinatario, email.as_string())
    smtp.quit()
    smtp.close()
  except Exception as e:
    print(e)
    raise Exception("Error en el envío de correo")

def hora_actual():
  hora_actual=datetime.datetime.now()
  año=str(hora_actual.year)
  mes=str(hora_actual.month)
  if len(mes)==1:
   mes='0'+mes
  dia=str(hora_actual.day)
  if len(dia)==1:
   dia='0'+dia
  hora=str(hora_actual.hour)
  if len(hora)==1:
   hora='0'+hora
  minuto=str(hora_actual.minute)
  if len(minuto)==1:
   minuto='0'+minuto

  hora_actual_f=(str(dia)+"/"+str(mes)+"/"+str(año)+" "+str(hora)+":"+str(minuto))
  return hora_actual_f



hilo_cubo_az=th.Thread(target=carga_cubo_az)
hilo_data_ct2=th.Thread(target=carga_data_ct2)
hilo_data_bct=th.Thread(target=carga_data_bct)
hilo_cubo_az.start()
hilo_data_ct2.start()
hilo_data_bct.start()
hilo_cubo_az.join()
hilo_data_ct2.join()
hilo_data_bct.join()
sp_procesar_dif_bct_ct2_bi()

hora_actual=hora_actual()

flag_val_p,df_p=val_data_bct_ct2_bi()
flag_val_s,df_s=val_data_tv_az()
html=val_load_html(flag_val_p,df_p,flag_val_s,df_s,hora_actual)

if html !=None:
  enviar_correo(html)
  print("Correo Enviado...")
else:
  print("No se envia correo...")



