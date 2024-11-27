from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# Variables de configuración
connection_string = "DefaultEndpointsProtocol=https;AccountName=lkdatossp;AccountKey=zjC5uH8oWtl3zBTbegMHQ76/s9gqfwiic27sqyXc9Nbs8TfMRcUaUu4yf7hvO/J1RSbMbFo859W2XilrRuhi4Q==;EndpointSuffix=core.windows.net"
container_name = "blddatossp"
blob_name = "venta/linea/cabecera.csv"
file_path = "D:/BI/objetivos/costo/archivo_diario/costo_rep.csv"

# Crear el cliente de servicio de blob
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Crear el cliente del contenedor
container_client = blob_service_client.get_container_client(container_name)

# Crear el cliente del blob
blob_client = container_client.get_blob_client(blob_name)

# Subir el archivo CSV
with open(file_path, "rb") as data:
    blob_client.upload_blob(data)

print(f"El archivo {file_path} ha sido subido como {blob_name} al contenedor {container_name}.")
