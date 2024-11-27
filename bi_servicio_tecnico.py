from google.cloud import bigquery
import time 
import pandas as pd

file_json_ist= r'D:\python\credenciales biq query\pe-intercorpretail-servicio_tecnico.json'
file_json_pb= r'D:\python\credenciales biq query\spsa-sistemasbi-powerbi-e0770fbf3fa0.json'


query_ist="""
select * from `pe-intercorpretail-cld-001.tracking_service_external_dataset.technical_service_summary`
"""
def df (query) :
    request_i=bigquery.Client.from_service_account_json(file_json_ist)
    query_job=request_i.query(query)

    while query_job.state != 'DONE':
        time.sleep(3)
        query_job.reload()

    if query_job.state== 'DONE':
     df=query_job.to_dataframe()
     #df.to_csv("D:\python\st.csv")
     return df

def ins_data_promo_gcp():
 request_p= bigquery.Client.from_service_account_json(file_json_pb)
 delet=request_p.query("delete `spsa-sistemasbi-powerbi.Servicio_Tecnico.servicio_tecnico_stage` where true",location="us")
 delet.result()

 job_config_cab = bigquery.LoadJobConfig (
        autodetect=True,
        write_disposition='WRITE_TRUNCATE'
    )
 target_table_id='spsa-sistemasbi-powerbi.Servicio_Tecnico.servicio_tecnico_stage'
 job_cab = request_p.load_table_from_dataframe(df(query_ist),target_table_id,job_config=job_config_cab)
 while job_cab.state != 'DONE':
    time.sleep(1)
    job_cab.reload()
    print("se terminó de procesar - "+str(target_table_id)+' '+str(job_cab.result()))

try:
    ins_data_promo_gcp()
except Exception as e :
   print(e)
   raise Exception ("error...")
  


 


