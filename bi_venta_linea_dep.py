
import est_ora as ora
from email.message import EmailMessage
from smtplib import SMTP

usuario= "SXCT2SP"
contraseña= "CT2remiX"
ip_server_name= "10.20.11.11/SPT03"

q_delete="""
    delete 
    FROM EXCT2SP.ict2_trxs_vta_bi_group i
    where i.hed_fcontable not in 
    (
    trunc(sysdate)-1,
    trunc(sysdate)
    )
"""
def enviar_correo (mensaje):
  try:
    remitente = "over.salazar@spsa.pe"
    destinatario = ['over.salazar@spsa.pe']
    
    mensaje = f"""Estimados por favor revisar el proceso depuración Venta en Linea, está cayendo con siguiente el error:<br><h2 style="color:red">{mensaje}</h2>Saludos<br>Over Salazar"""

    email = EmailMessage()
    email["From"] = remitente
    email["To"] = destinatario
    email["Subject"] = "Alerta Depuración Venta en Linea BI"
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
  
def depurar_tabla_envio_trx():
    try:
        obj_ora=ora.oracle(usuario=usuario,contraseña=contraseña,dsn=ip_server_name)
        obj_ora.inicializar_oracle()
        obj_ora.conectar_oracle()
        obj_ora.ejecutar_query_sin_return(q_delete)
        obj_ora.commit()
        return True
    except Exception as e:
        print(e)
        enviar_correo (e)

depurar_tabla_envio_trx()




