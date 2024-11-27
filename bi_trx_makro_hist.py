import datetime as dt
from google.cloud import bigquery as bq
import os


fecha_inicial='2022-01-01'
fecha_final='2024-03-11'
fecha_inicial=dt.datetime.strptime(fecha_inicial,"%Y-%m-%d")
fecha_final=dt.datetime.strptime(fecha_final,"%Y-%m-%d")

file_json_sbi=r'D:\python\credenciales biq query\sistemas-bi-7a46b3894448.json'
ruta_destino=r'D:\BI\proyectos\SCM_Hist\archivos_makro'

def consultar_data_mk(fecha):
    obj_bq=bq.Client.from_service_account_json(file_json_sbi)
    query_bq=f"""
    
    with ticket as 
    (
    select distinct a.fecha,a.codigo_local,b.nombre,a.numero_terminal,
        a.numero_transaccion,a.codigo_producto,a.id_ticket,a.hora_transaccion,
            from `sistemas-bi.SPSA.ticket_venta` a
            inner join `sistemas-bi.SPSA.dim_local` b
            on a.codigo_local=b.codigo_local
            where 
            b.formato= 'CASH & CARRY'
            and a.fecha between '{fecha}' and '{fecha}'
        --and a.codigo_local= '522'
            and a.codigo_tipo_venta=1
    )
    select concat(codigo_local,"-",nombre) Tienda,Seccion,Fecha,
    Hora,Driver,Valor from
    (
    select 
        a.fecha,a.codigo_local,a.nombre,a.nombre_division seccion,
        case when right(substring(cast(a.hora_transaccion as string),1,5),2) between '00' and '15' 
            then  concat(left(substring(cast(a.hora_transaccion as string),1,5),2),':00-',left(substring(cast(a.hora_transaccion as string),1,5),2),':15') 

            when right(substring(cast(a.hora_transaccion as string),1,5),2) between '16' and '30' 
            then  concat(left(substring(cast(a.hora_transaccion as string),1,5),2),':16-',left(substring(cast(a.hora_transaccion as string),1,5),2),':30') 

            when right(substring(cast(a.hora_transaccion as string),1,5),2) between '31' and '45' 
            then  concat(left(substring(cast(a.hora_transaccion as string),1,5),2),':31-',left(substring(cast(a.hora_transaccion as string),1,5),2),':45') 

            when right(substring(cast(a.hora_transaccion as string),1,5),2) between '46' and '59' 
            then  concat(left(substring(cast(a.hora_transaccion as string),1,5),2),':46-',left(substring(cast(a.hora_transaccion as string),1,5),2),':59') 
            end
        hora,
        count(distinct a.id_ticket) cantidad_trx,
        count(distinct a.codigo_producto) cantidad_items
        from
        (
            select a.fecha,a.codigo_local,a.nombre,a.numero_terminal,
        a.numero_transaccion,a.codigo_producto,b.nombre_division,a.id_ticket,a.hora_transaccion from ticket a
        inner join sistemas-bi.SPSA.dim_producto b 
        on a.codigo_producto=b.codigo_producto
        ) 
        a
        group by a.fecha,a.codigo_local,a.nombre,a.nombre_division,
        case when right(substring(cast(a.hora_transaccion as string),1,5),2) between '00' and '15' 
            then  concat(left(substring(cast(a.hora_transaccion as string),1,5),2),':00-',left(substring(cast(a.hora_transaccion as string),1,5),2),':15') 
            when right(substring(cast(a.hora_transaccion as string),1,5),2) between '16' and '30' 
            then  concat(left(substring(cast(a.hora_transaccion as string),1,5),2),':16-',left(substring(cast(a.hora_transaccion as string),1,5),2),':30') 
            when right(substring(cast(a.hora_transaccion as string),1,5),2) between '31' and '45' 
            then  concat(left(substring(cast(a.hora_transaccion as string),1,5),2),':31-',left(substring(cast(a.hora_transaccion as string),1,5),2),':45') 
            when right(substring(cast(a.hora_transaccion as string),1,5),2) between '46' and '59' 
            then  concat(left(substring(cast(a.hora_transaccion as string),1,5),2),':46-',left(substring(cast(a.hora_transaccion as string),1,5),2),':59') 
            end
    ) 
    unpivot (valor for driver in (cantidad_trx,cantidad_items)) 
    """
    req_bq=obj_bq.query(query_bq)
    req_bq.result()
    df=req_bq.to_dataframe()
    df
    return df


while fecha_inicial<=fecha_final:
    fecha_inicial_p=dt.datetime.strftime(fecha_inicial,"%Y-%m-%d")
    df=consultar_data_mk(fecha_inicial_p)
    name="Makro_Hist_"+(dt.datetime.strftime(fecha_inicial,"%Y%m%d"))+".csv"
    if df.empty==False:
        ruta_destino_rename=os.path.join(ruta_destino,name)
        df.to_csv(ruta_destino_rename,index=False,sep="|",encoding='UTF-16')
        print("Archivo "+name+" procesado...")
    else:
         print("No hay data para el archivo "+name+"...")
    fecha_inicial=fecha_inicial+dt.timedelta(days=1)

