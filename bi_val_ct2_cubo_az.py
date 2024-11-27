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

#credenciales bi_45
ip = '10.20.17.45'
bd= 'BI_STG'
us= 'sa'
pw= 'agilito2030'

directory_co=r"D:\instaladores\biblioteca cliente oracle\instantclient_11_2"
ruta_json_pbi= r'D:\python\credenciales biq query\spsa-sistemasbi-powerbi-e0770fbf3fa0.json'

now=datetime.date.today()
year=now.year
month=now.month

month_now = datetime.date(year + int(month/ 12), (month % 12) + 0, 1) 
start_month_cubo = month_now - relativedelta(months=0)
end_month_cubo = now#datetime.date(year + int(month/ 12), (month % 12) + 1, 1) - datetime.timedelta(days=1)

start_month_ct2=start_month_cubo
end_month_ct2=end_month_cubo

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
         #write_disposition='WRITE_TRUNCATE'
     )

  table_id_pbi='spsa-sistemasbi-powerbi.BI_VALIDADORES.tmp_data_ct2'

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
  AND h.hed_anulado = 'N' 
  GROUP BY h.hed_fcontable,h.hed_tipodoc,h.hed_local
  """
  return q_ct2

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
    print(var_fecha_ct2)
    start_month_ct2=start_month_ct2+datetime.timedelta(days=1)

  extension_oracle.cerrar()
  return dff_ct2

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

def sp_procesar_dif_ct2_bi():
  try:
    ins_pbi=bigquery.Client.from_service_account_json(ruta_json_pbi)
    request_pbi=ins_pbi.query("call `spsa-sistemasbi-powerbi.BI_VALIDADORES.sp_procesar_dif_ct2_bi` ()",location="us")
    request_pbi.result()
    print(" Se terminó de procesar las diferencias de CT2 y BI en Big Query")
  except Exception as e:
    print(e)
    raise Exception ("Error al procesar las diferencias de CT2 y BI")

def df_tabla_val (flag_val):
  query_dif_ct2_bi="""
  select a.* from
  `spsa-sistemasbi-powerbi.BI_VALIDADORES.dif_venta_neta_bi_ct2` a
  where diferencia <> 0 --not between -1 and 1
  order by 1 asc
  """
  query_dif_tv_cubo_az="""
  select a.* from
  `spsa-sistemasbi-powerbi.BI_VALIDADORES.dif_venta_neta_tv_cubo_az` a
  where diferencia <> 0--not between -1 and 1
  order by 1 asc
  """
  req_clt=bigquery.Client.from_service_account_json(ruta_json_pbi)
  if flag_val=="bi_ct2":
    request_bi_ct2=req_clt.query(query_dif_ct2_bi,location="us")
    request_bi_ct2.result()
    df_bi_ct2=request_bi_ct2.to_dataframe()
    return df_bi_ct2
  elif flag_val=="tv_cubo_az" :
    request_tv_cubo_az=req_clt.query(query_dif_tv_cubo_az,location="us")
    request_tv_cubo_az.result()
    df_tv_cubo_az=request_tv_cubo_az.to_dataframe()
    return df_tv_cubo_az
  else :
    raise Exception ("Parametros de validación Incorrectos")

def load_html_table_dif_bi_ct2():
  df = df_tabla_val("bi_ct2")
  if df.empty==True:
    print ("No hay data de diferencias en ct2 y bi")
    return None
  else :
    print("Si hay data de diferencias en ct2 y bi")
    html_dif_bi_ct2=df.to_html(index=False,col_space=150,justify='center',border=3).replace('<td>', '<td align="right">')
    return html_dif_bi_ct2
  
def load_html_table_dif_tv_cubo_az():
  df = df_tabla_val("tv_cubo_az")
  if df.empty==True:
    print ("No hay data de diferencias en ticket venta y cubo az")
    return None
  else :
    print("Si hay data de diferencias en ticket venta y cubo az")
    html_dif_tv_cubo_az=df.to_html(index=False,col_space=150,justify='center',border=3).replace('<td>', '<td align="right">')
    #print(html_dif_tv_cubo_az)
    return html_dif_tv_cubo_az
  
def procesar_html(tabla_bi_ct2,tabla_tv_cubo_az,tipo_body):
  if tipo_body=="full":
    html=f"""
    <html>
        <head>
        </head>
        <body>
            <h1 style="color:rgb(238, 19, 19)">Alerta Venta BI!!!
            </h1>
            <h3><center>
                <hr>Diferencias CT2 & BI 
                <hr/>
            </center></h3>
    <center>
      <div class="row">
      {tabla_bi_ct2}
    </div>
    </center>
    
    <h5 style="text-align: right;">Nota: No incluye Venta Vale</h1>
            <h3><center>
                <hr>Diferencias Ticket Venta & Cubo de Venta Power BI
                <hr/>
            </center></h3>
  <center>
  <div class="row">      
  {tabla_tv_cubo_az}
  </div>
  </center>
            <h5 style="text-align: right;">Nota: Sí incluye Venta Vale</h1>
            <h3>Estimado Equipo de Reproceso su coordinación urgente, por favor reprocesar y bajar estas diferencias....</h3>
        <body>
    <html>
    """
    return html
  elif tipo_body=="bi_ct2":
    html=f"""
            <html>
            <head>
            </head>
            <body>
                <h1 style="color:rgb(238, 19, 19)">Alerta Venta BI!!!</h1>
                <h3><center>
                    <hr>Diferencias CT2 & BI
                    <hr/>
                </center>
            </h3>
        <center>
          <div class="row">
          {tabla_bi_ct2}
        </div>
        </center>

        <h5 style="text-align: right;">Nota: No incluye Venta Vale</h1>
        <h3>Estimados su coordinación urgente, por favor reprocesar y bajar estas diferencias....</h3>
        <body>
        <html>
      """
    return html
  elif tipo_body=="tv_cubo_az":
    html=f"""
        <html>
          <head>
          </head>
          <body>
              <h1 style="color:rgb(238, 19, 19)">Alerta Venta BI!!!</h1>
              <h3>  
            <center>
              <hr>Diferencias Ticket Venta & Cubo de Venta Power BI<hr/>
            </center>
          </h3>
    <center>
      <div class="row">      
      {tabla_tv_cubo_az}
      </div>
    </center>
              <h5 style="text-align: right;">Nota: Sí incluye Venta Vale</h1>
              <h3>Estimados su coordinación urgente, por favor reprocesar y bajar estas diferencias....</h3>
          <body>
      <html>
    """
    return html
  else:
    return None

def enviar_correo (html):
  try:
    remitente = "over.salazar@spsa.pe"
    #destinatario = ['oversalazar1997@gmail.com','jose.carmona-pry@spsa.pe','marlon.cabrera@spsa.pe','juan.medina@spsa.pe','soportect2@spsa.pe','elvis.velasquez@spsa.pe','osalazart@certus.edu.pe','beatriz.vasquez@spsa.pe','avanza-magalli.vera@spsa.pe']
    #destinatario = ['oversalazar1997@gmail.com','jose.carmona-pry@spsa.pe','marlon.cabrera@spsa.pe','creantis-jose.lamela@spsa.pe']
    destinatario = ['oversalazar1997@gmail.com']
    
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
  
#hilo_cubo_az=th.Thread(target=carga_cubo_az)
#hilo_data_ct2=th.Thread(target=carga_data_ct2)
#hilo_cubo_az.start()
#hilo_data_ct2.start()
#hilo_cubo_az.join()
#hilo_data_ct2.join()
#sp_procesar_dif_ct2_bi()

data_dif_bi_ct2=load_html_table_dif_bi_ct2()
data_dif_tv_cubo_az=load_html_table_dif_tv_cubo_az()

if data_dif_bi_ct2!=None and data_dif_tv_cubo_az!= None:
  tipo_body="full"
  html=procesar_html(data_dif_bi_ct2,data_dif_tv_cubo_az,tipo_body)
  enviar_correo(html)
  print(html)
elif data_dif_bi_ct2!=None:
  tipo_body="bi_ct2"
  html=procesar_html(data_dif_bi_ct2,data_dif_tv_cubo_az,tipo_body)
  enviar_correo(html)
  print(html)
elif data_dif_tv_cubo_az!=None:
  tipo_body="tv_cubo_az"
  html=procesar_html(data_dif_bi_ct2,data_dif_tv_cubo_az,tipo_body)
  enviar_correo(html)
  print(html)
else:
  print("No se envía el correo")



  


    




