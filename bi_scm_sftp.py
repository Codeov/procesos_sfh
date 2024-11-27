import pysftp
import os

ip="34.134.251.183"
us="qa_spsa"
pw="R&XfDeDXQ9se!2"

nombre_file="prueba.csv"
ruta_origen=r"D:/BI/big_query/links.txt"
ruta_destino_p=r"/spsa_data/scheduler/historiaspsa/"
ruta_destino_d=ruta_destino_p+nombre_file
print(ruta_destino_d)


def upload_file_to_sftp(local_file_path, remote_file_path_p,remote_file_path_d, sftp_host, sftp_username, sftp_password):
    try:
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None  

        with pysftp.Connection(host=sftp_host, username=sftp_username, password=sftp_password, cnopts=cnopts) as sftp:
            # Cargar el archivo
            val_exists=sftp.exists(remote_file_path_p)
            if val_exists:
                sftp.put(localpath=local_file_path,remotepath=remote_file_path_d)
                print(f'Archivo {local_file_path} cargado exitosamente a {remote_file_path_p}')
    except Exception as e:
        print(f'Ocurrió un error: {e}')
        raise(f"error en la subida de archivo al sftp ,{e}")

local_file_path = ruta_origen
remote_file_path_p =ruta_destino_p
remote_file_path_d =ruta_destino_d
sftp_host =ip
sftp_username = us
sftp_password = pw

print(local_file_path)
print(remote_file_path_d)
upload_file_to_sftp(local_file_path, remote_file_path_p,remote_file_path_d,sftp_host,sftp_username, sftp_password)


