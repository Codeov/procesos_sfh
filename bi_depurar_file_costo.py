
from google.cloud import storage
from google.cloud.storage import Blob
from datetime import datetime,timedelta
import os 

file_json = "E:/bst/bi_venta/ejecutables/file_input/sistemas-bi-7a46b3894448.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS']=file_json
files_origen_actual =[]
files_destino_var=[]
files_destino_actual=[]
file_name= 'costo.gz'
file_name_mxt= 'costo_'
directorio_storage_origen='mjcn/costo/archivo_diario/'
directorio_storage_destino='mjcn/costo/archivo_diario_procesado/'
name_bucket='spsa'
numero_files=90
getdate=str((datetime.today()+timedelta(days=0)).strftime('%Y%m%d'))
destino_blob=directorio_storage_destino+file_name_mxt+getdate+'.gz'

blob_search=os.path.join(directorio_storage_origen,file_name)
storage_client=storage.Client()
origen_bucket = storage_client.get_bucket(name_bucket)
destino_bucket=storage_client.get_bucket(name_bucket)

######listar y copiar archivos######
blobs_origen = origen_bucket.list_blobs(prefix=directorio_storage_origen)
for blobs_org in blobs_origen:
    files_origen_actual.append(blobs_org.name)
blob="".join(list(filter(lambda x: x==blob_search,files_origen_actual)))
estado= 'false'
while blob and estado== 'false':
    print("archivo encontrado...")
    source_blob=origen_bucket.blob(blob)
    origen_bucket.copy_blob(source_blob,destino_bucket,destino_blob)
    estado='true'

######lógica de eliminacion######
blobs_destino = destino_bucket.list_blobs(prefix=directorio_storage_destino)
for blobs_dst in blobs_destino:
    files_destino_actual.append(blobs_dst.name)

for i in range(0,numero_files):
 if i==0 :
  num=0
 else:
  num='-'+str(i)
 getdate=str((datetime.today()+timedelta(days=int(num))).strftime('%Y%m%d'))
 destino_blob=directorio_storage_destino+file_name_mxt+getdate+'.gz'
 files_destino_var.append(destino_blob)

print(files_destino_actual)
print(files_destino_var)

for val_file in files_destino_actual :
   val_blob="".join(list(filter(lambda x: x==val_file,files_destino_var)))
   val_flag='true'
   if val_blob and val_flag=='true':
    val_flag='false'
   else:
     if val_file==directorio_storage_destino:
       print("principal")
     else:
      print("el archivo a eliminar es "+str(val_file))
      blob_delete = destino_bucket.list_blobs(prefix=val_file)
      for blob in blob_delete:
       blob.delete()









