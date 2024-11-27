from est_sql import *
import pandas as pd
from smtplib import SMTP,SMTP_SSL
from email.message import EmailMessage

ip = '10.20.1.5'
bd= 'Retail_DW'
us= 'operador'
pw= 'operador'

def ret_tipo_query(tipo_query):
    query_err_scm_driver="""
    select distinct a.id_proceso,a.nombre,a.fecha,a.descripcion
    from [dbo].[err_scm_driver] a
    """
    query_err_scm_agr_metrica="""
    select a.fecha,a.cod_local,a.driver,metrica 
    from  [dbo].[err_scm_agr_metrica] a
    """
    if tipo_query=="err_scm_driver":
        return query_err_scm_driver
    elif tipo_query=="err_scm_agr_metrica":
        return query_err_scm_agr_metrica
    else:
        raise ("parametros de entrada incorrectos - ret_tipo_query") 

def request_sql (query):
 try:
    obj_sql=SQLServer(ip,bd,us,pw)
    obj_sql.connect_to_sql_server()
    ls=obj_sql.query_return(query)
    obj_sql.cerrar()
    return ls
 except Exception as e:
    raise(f"error en el request +{query}")

def validar_data_lista(ls,name_ls):
    if name_ls=="ls_err_scm_driver":
        if ls:
            flag_ls_err_scm_driver=True
            return flag_ls_err_scm_driver
        else:
            flag_ls_err_scm_driver=False
            return flag_ls_err_scm_driver
    elif name_ls=="ls_err_scm_agr_metrica":
        if ls:
            flag_ls_err_scm_agr_metrica=True
            return flag_ls_err_scm_agr_metrica
        else:
            flag_ls_err_scm_agr_metrica=False
            return flag_ls_err_scm_agr_metrica
    else:
        raise("parametros incorrectos - validar_data_lista")

def enviar_correo (html):
  try:
    remitente = "over.salazar@spsa.pe"
    destinatario = ['over.salazar@spsa.pe','creantis-bryan.palomino@spsa.pe','creantis-jose.lamela@spsa.pe']
    mensaje = html

    email = EmailMessage()
    email["From"] = remitente
    email["To"] = destinatario
    email["Subject"] = "Alerta SCM"
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
  
def procesar_html(tabla_html,tipo_flag):
     titulo_left="""
              <h1 style="color:rgb(238, 19, 19)">Informe de Driver Faltante - SCM!!!</h1>
              """
     primer_block=f"""
              <h3>
                <center>
                  <hr>Detalle de Proceso-Driver Faltante <hr/>
                </center>
              </h3>
              <center>
                <div class="row">
                  {tabla_html}
                </div>
              </center>
              """
     second_block=f"""
              <h3>
                <center>
                  <hr>Locales con Driver-Métrica Cero<hr/>
                </center>
              </h3>
              <center>
                <div class="row">
                  {tabla_html}
                </div>
              <center>
              """
     aviso=f"""
             <p>Estimados su apoyo con la revisión de los input SCM...</p>
            """
     footer=f"""
              <center>
                <footer><p><h4>Sistemas BI 2024© - BST</h4></p></footer>
              </center>
              """
     if tipo_flag=="flag_ls_err_scm_driver":
         html=f"""
          {titulo_left}
          {primer_block}
          {aviso}
          {footer}
         """
         return html
     elif tipo_flag=="flag_ls_err_scm_agr_metrica":
          html=f"""
          {titulo_left}
          {second_block}
          {aviso}
          {footer}
         """
          return html
     else :
         raise("parametros incorrectos - procesar_html")
    

ls_err_scm_driver=request_sql(ret_tipo_query("err_scm_driver"))
ls_err_scm_agr_metrica=request_sql(ret_tipo_query("err_scm_agr_metrica"))

flag_ls_err_scm_driver=validar_data_lista(ls_err_scm_driver,"ls_err_scm_driver")
flag_ls_err_scm_agr_metrica=validar_data_lista(ls_err_scm_agr_metrica,"ls_err_scm_agr_metrica")

if flag_ls_err_scm_driver==True:
    df_err_scm_driver=pd.DataFrame(data=ls_err_scm_driver,columns=["id_proceso","nombre","fecha","descripcion"])
    df_err_scm_driver['id_proceso']=df_err_scm_driver['id_proceso'].map(str)
    df_err_scm_driver['nombre']=df_err_scm_driver['nombre'].map(str)
    df_err_scm_driver['fecha']=df_err_scm_driver['fecha'].map(str)
    df_err_scm_driver['descripcion']=df_err_scm_driver['descripcion'].map(str)
    html_err_scm_driver=df_err_scm_driver.to_html(index=False,col_space=150,justify='center',border=3).replace('<td>', '<td align="right">')
    html=procesar_html(html_err_scm_driver,"flag_ls_err_scm_driver")
    enviar_correo(html)
    print("Correo Enviado")
elif flag_ls_err_scm_agr_metrica==True:
    df_err_scm_agr_metrica=pd.DataFrame(data=ls_err_scm_agr_metrica,columns=["fecha","cod_local","driver","metrica"])
    df_err_scm_agr_metrica['fecha']=df_err_scm_agr_metrica['fecha'].map(str)
    df_err_scm_agr_metrica['cod_local']=df_err_scm_agr_metrica['cod_local'].map(str)
    df_err_scm_agr_metrica['driver']=df_err_scm_agr_metrica['driver'].map(str)
    df_err_scm_agr_metrica['metrica']=df_err_scm_agr_metrica['metrica'].map(str)
    html_err_scm_agr_metrica=df_err_scm_agr_metrica.to_html(index=False,col_space=150,justify='center',border=3).replace('<td>', '<td align="right">')
    html=procesar_html(html_err_scm_agr_metrica,"flag_ls_err_scm_agr_metrica")
    enviar_correo(html)
    print("Correo Enviado")
else:
    print("No se Envia Correo")






   
   
   






