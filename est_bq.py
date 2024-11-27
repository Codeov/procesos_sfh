from google.cloud import bigquery 
import time

class bigq:
    def __init__(self,json):
        self.json=json
    
    def clear_table(self,project,dataset,table):
        obq=bigquery.Client.from_service_account_json(self.json)
        qcl=f"truncate table `{project}.{dataset}.{table}`"
        rq =obq.query(qcl)
        rq.result()
    
    def exec_query_sin_param(self,query):
        obq=bigquery.Client.from_service_account_json(self.json)
        rq =obq.query(query)
        rq.result()

    def ins_table(self,project,dataset,table,df):
         obq= bigquery.Client.from_service_account_json(self.json)
         job_config_cab = bigquery.LoadJobConfig(
                autodetect=True
            )
         target_table_id=f'{project}.{dataset}.{table}' 
         job_ins = obq.load_table_from_dataframe(df,target_table_id,job_config=job_config_cab)
         while job_ins.state != 'DONE':
            time.sleep(1)
            job_ins.reload()
         print(f"se terminó de procesar la data en {target_table_id} - "+str(job_ins.result()))
    
    def ins_table_param_segun_input(self,project,dataset,table,df):
         obq= bigquery.Client.from_service_account_json(self.json)
         # Generar el esquema basado en las columnas del DataFrame
         #schema = [bigquery.SchemaField(column, "STRING") for column in df.columns]

         job_config_cab = bigquery.LoadJobConfig(
            #schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
         
         target_table_id=f'{project}.{dataset}.{table}' 
         job_ins = obq.load_table_from_dataframe(df,target_table_id,job_config=job_config_cab)
         while job_ins.state != 'DONE':
            time.sleep(1)
            job_ins.reload()
         print(f"se terminó de procesar la data en {target_table_id} - "+str(job_ins.result()))

    def consultar_table(self,query):
        o_bq=bigquery.Client.from_service_account_json(self.json)
        request= o_bq.query(query)
        request.result()
        pdb=request.to_dataframe()
        return pdb    

