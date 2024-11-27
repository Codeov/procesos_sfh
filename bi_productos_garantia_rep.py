import oracledb 
import pandas as pd
import datetime
from google.cloud import bigquery as bq

#credenciales ct2
usuario= "SXCT2SP"
contraseña= "CT2remiX"
ip_server_name= "10.20.11.11/SPT03"
directory_co=r"D:\instaladores\biblioteca cliente oracle\instantclient_11_2"
file_json = r"D:\python\credenciales biq query\sistemas-bi-7a46b3894448.json"

fecha_inicial= '2024-09-01'
f_fecha_inicial= datetime.datetime.strptime(fecha_inicial,'%Y-%m-%d')
fecha_final='2024-10-29'
f_fecha_final=datetime.datetime.strptime(fecha_final,'%Y-%m-%d')

class oracle :
    def __init__(self,usuario,contraseña,dsn):
        self.usuario=usuario
        self.contraseña= contraseña
        self.dsn=dsn

    def conectar_oracle(self):
        try:
            self.cnxo=oracledb.connect(user=self.usuario,password=self.contraseña,dsn=self.dsn)
            print("conexion exitosa to oracle")
            return self.cnxo
        except Exception as e:
            print(e)
            return None
    def ejecutar_query(self,query):
        try:
            cursor=self.cnxo.cursor()
            cursor.execute(query)
            self.data=cursor.fetchall()
            #self.data = list(itertools.chain.from_iterable(cursor))
            if self.data:
             return(self.data)
            else:
             return None
        except Exception as e:
            print(e)
            return None
        
    def ejecutar_query_cab(self,query):
        try:
            cursor=self.cnxo.cursor()
            cursor.execute(query)
            self.cab = [col[0] for col in cursor.description]
            return(self.cab)
        except Exception as e:
            print(e)
            return None
        
    def cerrar(self):
        try:
            self.close= self.cnxo.close()
            print("conexion to oracle cerrada")
            return self.close
        except Exception as e:
            print(e)
            return None

def query_ct2_ticket_gar(fecha) :
 query =f"""
    select /*+RULE*/ 
    distinct to_char(a.hed_fechatrx,'yyyy-mm-dd') fecha,a.hed_local,a.hed_pos,a.hed_numtrx,a.ptr_corrprod,nullif(b.cod_bar,'') ptr_codprod,b.prd_lvl_number ptr_sku,
    a.gar_corrgarantia,
    nullif(c.cod_bar,'') gar_codgarantia,c.prd_lvl_number gar_sku                                                                                                                      
    from ctx_garantias_trx a
    left join
    (
       select distinct a.cod_bar,a.cod_bar_upc,a.prd_lvl_number from ifhprdmst_ct a  
    ) b
    
    on
    cast(a.ptr_codprod as numeric)=cast(b.cod_bar_upc as numeric)
    left join
    (
       select distinct a.cod_bar,a.cod_bar_upc,a.prd_lvl_number from ifhprdmst_ct a  
    ) c
    
    on
    cast(a.gar_codgarantia as numeric)=cast(c.cod_bar_upc as numeric)

    where 
    to_char(a.hed_fechatrx, 'DD/MM/YYYY')='{fecha}' 
 """
 return query

def valor_reemplazo(col):
    if col.dtype == 'float64' or col.dtype == 'int64':
        return 0  
    elif col.dtype == 'object':
        return ''  
    elif col.dtype == 'datetime64[ns]':
        return pd.Timestamp('1900-01-01')
    else:
        return None 
    
def cargar_data_ticket_gar_gcp(fecha):
   ct2_ticket_gar=oracle(usuario,contraseña,ip_server_name)
   ct2_ticket_gar.conectar_oracle()
   ticket_gar=ct2_ticket_gar.ejecutar_query(query_ct2_ticket_gar(fecha))
   if ticket_gar !=None:
      f=pd.DataFrame(data=ticket_gar,columns=['fecha','hed_local','hed_pos','hed_numtrx','ptr_corrprod','ptr_codprod','ptr_sku','gar_corrgarantia','gar_codgarantia','gar_sku'])
      for col in f.columns:
            f[col] = f[col].fillna(valor_reemplazo(f[col]))
      f['fecha']=f['fecha'].map(str)
      f['hed_local']=f['hed_local'].map(str)
      f['hed_pos']=f['hed_pos'].map(str)
      f['hed_numtrx']=f['hed_numtrx'].map(str)
      f['ptr_corrprod']=f['ptr_corrprod'].map(str)
      f['ptr_codprod']=f['ptr_codprod'].map(str)
      f['ptr_sku']=f['ptr_sku'].map(str)
      f['gar_corrgarantia']=f['gar_corrgarantia'].map(str)
      f['gar_codgarantia']=f['gar_codgarantia'].map(str)
      f['gar_sku']=f['gar_sku'].map(str)
      table='tmp_ticket_gar'
      ct2_ticket_gar.cerrar()
      return f
def ins_garantia_ticket(df,table,ini,fin):
   try:
      req_tg=bq.Client.from_service_account_json(file_json)
      resp_del_tg=req_tg.query(f"""delete `sistemas-bi.SPSA.{table}` where true""")
      resp_del_tg.result()
      print("cargando tmp...")
      resp_ins_tg=req_tg.load_table_from_dataframe(df,f"""sistemas-bi.SPSA.{table}""")
      resp_ins_tg.result()
      print("tmp procesada")
      print("cargando sp...")
      resp_ins_sp=req_tg.query(f"""call `sistemas-bi.SPSA.ins_fact_ticket_vta_gar_rep`('{ini}', '{fin}') """)
      resp_ins_sp.result()
      print("sp procesado")
   except Exception as e:
      raise print(e)
oracledb.init_oracle_client(directory_co)   
dfa=pd.DataFrame(data=[],columns=['fecha','hed_local','hed_pos','hed_numtrx','ptr_corrprod','ptr_codprod','gar_codgarantia'])
while f_fecha_inicial <=f_fecha_final:
    r_fecha_inicial=datetime.datetime.strftime(f_fecha_inicial,'%d/%m/%Y')
    dfa=pd.concat([dfa,cargar_data_ticket_gar_gcp(r_fecha_inicial)])
    f_fecha_inicial=f_fecha_inicial+datetime.timedelta(days=1)

table='tmp_ticket_gar'
ins_garantia_ticket(dfa,table,fecha_inicial,fecha_final)
 
 