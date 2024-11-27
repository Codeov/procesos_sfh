
import os
from azure.identity import AzureCliCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from time import sleep

# Verificar el PATH en Python
print(os.environ['PATH'])
# Verificar si 'az' está disponible
os.system('az --version')

# Reemplaza estos valores con los de tu suscripción y Data Factory
subscription_id = '8df11707-3cc8-4f7f-a784-2ccbceee31b1'
resource_group_name = 'GRDatosSP'
data_factory_name = 'DFDatosSP'
pipeline_name = 'pipe_sql_dim_local'
parameters = {}  # Diccionario con los parámetros necesarios para tu pipeline, si los hay


# Autenticación utilizando Azure CLI
credential = AzureCliCredential()

# Crear cliente de Azure Data Factory
adf_client = DataFactoryManagementClient(credential, subscription_id)

# Ejecutar pipeline
run_response = adf_client.pipelines.create_run(
    resource_group_name=resource_group_name,
    factory_name=data_factory_name,
    pipeline_name=pipeline_name
)

# Obtener el ID de ejecución del pipeline
run_id = run_response.run_id
print(f"Pipeline ejecutado con run ID: {run_id}")

# Monitorear el estado del pipeline (opcional)
pipeline_run = adf_client.pipeline_runs.get(
    resource_group_name=resource_group_name,
    factory_name=data_factory_name,
    run_id=run_id
)

while pipeline_run.status not in ('Succeeded', 'Failed', 'Cancelled'):
    print(f"Estado actual: {pipeline_run.status}")
    sleep(3)  # Esperar 30 segundos antes de volver a chequear
    pipeline_run = adf_client.pipeline_runs.get(
        resource_group_name=resource_group_name,
        factory_name=data_factory_name,
        run_id=run_id
    )

print(f"Estado final: {pipeline_run.status}")
