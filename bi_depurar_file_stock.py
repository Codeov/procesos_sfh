
from google.cloud import storage
from google.cloud.storage import Blob
from google.cloud import bigquery as bq
from datetime import datetime,timedelta
import os 

file_json = "D:\python\credenciales biq query/sistemas-bi-7a46b3894448.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS']=file_json

files_destino_var=[]
files_destino_actual=[]
file_name= 'Stock_Diario.csv.gz'
file_name_mxt= 'Stock_Diario.csv_'
directorio_storage_origen='mjcn/stock/archivo_diario/'
directorio_storage_destino='mjcn/stock/archivo_diario_procesado/'
name_bucket='spsa'
numero_files=3

blob_search=os.path.join(directorio_storage_origen,file_name)
storage_client=storage.Client()
origen_bucket = storage_client.get_bucket(name_bucket)
destino_bucket=storage_client.get_bucket(name_bucket)

query_fecha_stock="""
select distinct fecha_proceso
from `sistemas-bi.SPSA_STAGE.stock_diario`
--where 1=2
order by 1 desc
limit 1
"""
def obtener_fecha_stock(file_json,query_fecha_stock):
  try:
    oBq=bq.Client.from_service_account_json(file_json)
    reqBq=oBq.query(query_fecha_stock)
    reqBq.result()
    df=reqBq.to_dataframe()
    if df.empty==False:
     fecha="".join(df.values[0])
     fecha=datetime.strptime(fecha,"%Y-%m-%d")
     fecha=datetime.strftime(fecha,"%Y%m%d")
     print(fecha)
    else:
      fecha=datetime.now()
      fecha=fecha+timedelta(days=-1)
      fecha=datetime.strftime(fecha,"%Y%m%d")
      print(fecha)
    return fecha
  except Exception as e:
    raise Exception("Error en la extraccion de fecha")


def validar_file_diario(directorio_storage_origen,blob_search):
  files_origen_actual =[]
  blobs_origen = origen_bucket.list_blobs(prefix=directorio_storage_origen)
  for blobs_org in blobs_origen:
      files_origen_actual.append(blobs_org.name)
  blob="".join(list(filter(lambda x: x==blob_search,files_origen_actual)))
  if blob:
    return True,blob
  else:
    blob="vacio"
    return None,blob
  
flag_val_file_diario,blob=validar_file_diario(directorio_storage_origen,blob_search)

if flag_val_file_diario==True:
  source_blob=origen_bucket.blob(blob)
  fecha=obtener_fecha_stock(file_json,query_fecha_stock)
  destino_blob=directorio_storage_destino+file_name_mxt+fecha+'.gz'
  print(destino_blob)
  origen_bucket.copy_blob(source_blob,destino_bucket,destino_blob)
else:
  print("el archivo no se encuentra")


######lógica de eliminacion######
blobs_destino = destino_bucket.list_blobs(prefix=directorio_storage_destino)
for blobs_dst in blobs_destino:
    files_destino_actual.append(blobs_dst.name)

for i in range(1,numero_files+1):
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











