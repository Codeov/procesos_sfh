try:
    from google.cloud import storage
    from google.cloud.storage import Blob
    import os
except Exception as e:
    print("Error :{} ".format(e))

    
path=os.path.join('D:\python\credenciales biq query','sistemas-bi-7a46b3894448.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS']=path

storage_client= storage.Client(path)

bucket=storage_client.get_bucket('spsa')

blob=Blob('bst/costo_reproceso/costo_rep.csv',bucket)

blob.upload_from_filename('D:/BI/objetivos/costo/archivo_diario/costo_rep.csv')

print('subido')