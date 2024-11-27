
from est_ora import oracle
from est_sql import *
import datetime
import pandas as pd
import time
import logging
import concurrent.futures

from google.cloud import bigquery

#credenciales ct2
usuario= "SXCT2SP"
contraseña= "CT2remiX"
ip_server_name= "10.20.11.11/SPT03"
directory_co=r"D:\instaladores\biblioteca cliente oracle\instantclient_11_2"

file_json_sbi=r'D:\python\credenciales biq query\sistemas-bi-7a46b3894448.json'

thread=9

fecha_ini= '2024-02-01'
fecha_fin='2024-02-12'
fecha_format_ini_1=datetime.datetime.strptime(fecha_ini,'%Y-%m-%d')
fecha_format_fin_1=datetime.datetime.strptime(fecha_fin,'%Y-%m-%d')

loc_rec= """
select distinct loc_numero
from irs_locales   where 
 loc_activo='S'
and cad_numero in (12) --3
union all
select distinct loc_numero
from irs_locales   where 
 loc_activo='S'
and cad_numero in (2) 
and loc_numero <5000
and loc_descripcion like '%JOKR%MK%'
"""

def query_ct2_recaudaciones (fecha,local):
    query_recaudaciones= f"""
     SELECT /*+RULE*/  to_char(h.hed_fcontable,'YYYY-MM-DD') Fecha_Proceso,
        i.loc_numeropmm Codigo_Local,
        h.hed_pos Numero_Terminal,
        h.hed_numtrx Numero_Transaccion,
        NVL(h.hed_vendedor,'0') Codigo_Vendedor,
        to_char(h.hed_fechatrx,'YYYY-MM-DD') Fecha_Compra,
        h.hed_tipotrx Codigo_Tipo_Transaccion,
        h.hed_tipodoc Codigo_Tipo_Doc_Trx,
        to_char(h.hed_horatrx,'hh24:mi:ss') Hora_Transaccion,
        'V' Estado_Transaccion,
        (h.hed_brutopos + h.hed_brutoneg) Venta_Bruta,
        '0' Monto_Ajuste,
        (h.hed_brutopos + h.hed_brutoneg -
        NVL((SELECT SUM(s.dpr_monto)
               FROM ctx_dsctos_prod_trx s
              WHERE h.hed_pais = s.hed_pais
                AND h.hed_origentrx = s.hed_origentrx
                AND h.hed_local = s.hed_local
                AND h.hed_pos = s.hed_pos
                AND h.hed_numtrx = s.hed_numtrx
                AND h.hed_fechatrx = s.hed_fechatrx
                AND h.hed_horatrx = s.hed_horatrx),0)) Venta_con_Descuento,
        (h.hed_brutopos + h.hed_brutoneg - NVL(h.hed_impuesto,0) - nvl(h.hed_impuesto_isc,0)) Venta_Neta,
        NVL((SELECT SUM(s.dpr_monto)
               FROM ctx_dsctos_prod_trx s
              WHERE h.hed_pais = s.hed_pais
                AND h.hed_origentrx = s.hed_origentrx
                AND h.hed_local = s.hed_local
                AND h.hed_pos = s.hed_pos
                AND h.hed_numtrx = s.hed_numtrx
                AND h.hed_fechatrx = s.hed_fechatrx
                AND h.hed_horatrx = s.hed_horatrx),0) Monto_Descuento_Item,
        h.hed_redondeo Monto_Redondeo,
        h.hed_donacion Monto_Donacion,
        h.hed_cajero Codigo_Cajero,
        h.hed_tipocambio Tipo_Cambio,
        DECODE(h.hed_tipodoc,'TFC','1','BLT','3','NCR','7') Codigo_Tipo_Comprobante,
        CASE WHEN h.hed_tipoventa = 1 THEN h.hed_ndocashop ELSE h.hed_rutdoc END Ruc_Factura, 
        h.hed_serie Serie_Transaccion,
        h.hed_motivo Codigo_Razon_Devolucion,
        h.hed_autoriza Codigo_Supervisor,
        CASE WHEN (SELECT COUNT(1) FROM ctx_garantias_trx g
          WHERE h.hed_pais = g.hed_pais
            AND h.hed_origentrx = g.hed_origentrx
            AND h.hed_local = g.hed_local
            AND h.hed_pos = g.hed_pos
            AND h.hed_numtrx = g.hed_numtrx
            AND h.hed_fechatrx = g.hed_fechatrx
            AND h.hed_horatrx = g.hed_horatrx) > 0 THEN '1' ELSE '0' END Flag_Supergarantia,
        f.fid_tiporeg Codigo_Tipo_Documento,
        DECODE(NVL((SELECT a.cod_interno
           FROM puc_personas a
          WHERE lpad(f.fid_nrodoc,8,'0') = a.num_docum_ide
            AND a.tip_docum_ide = 141),0),0,nvl(f.fid_nrodoc,0),0) Numero_Documento_Cliente,
        to_char(f.fid_nrotarj) Numero_Tarjeta_Cliente,
        h.hed_impuesto Monto_IGV,
        nvl(h.hed_impuesto_isc,0) Monto_ISC,
        (SELECT NVL(SUM(t.impp_montoimp),0)
           FROM ctx_impsto_dsctos_prod_trx t
          WHERE h.hed_pais = t.hed_pais
            AND h.hed_origentrx = t.hed_origentrx
            AND h.hed_local = t.hed_local
            AND h.hed_pos = t.hed_pos
            AND h.hed_numtrx = t.hed_numtrx
            AND h.hed_fechatrx = t.hed_fechatrx
            AND h.hed_horatrx = t.hed_horatrx
            AND t.impp_codimp IN ('GM','GF')) Monto_IGV_Gasto,
            
        '0' Monto_Costo_Total,
        CASE WHEN LENGTH(h.hed_cnegop)>1 THEN '1' ELSE '0' END Flag_Centro_Negocio,
        '0' Numero_Centro_Negocio_O,
        '0' Numero_Ticket,
        h.hed_numdocdel Numero_Boleta_Electronica,
        h.hed_entretrx Tiempo_Entre_Transaccion,
        h.hed_codcar Codigo_Car,
        h.hed_rutdoc Numero_Documento_Car,
        CASE WHEN (SELECT COUNT(1) FROM ctx_dsctos_prod_trx ds
          WHERE h.hed_pais = ds.hed_pais
            AND h.hed_origentrx = ds.hed_origentrx
            AND h.hed_local = ds.hed_local
            AND h.hed_pos = ds.hed_pos
            AND h.hed_numtrx = ds.hed_numtrx
            AND h.hed_fechatrx = ds.hed_fechatrx
            AND h.hed_horatrx = ds.hed_horatrx
            AND ds.dpr_coddcto IN ('411149',
                                '411154',
                                '411163',
                                '411179',
                                '411180',
                                '9835225',
                                '9835194',
                                '15308433',
                                '15377082',
                                '15670595',
                                '15670700',
                                '15670701',
                                '15670703',
                                 '15670819')
            ) > 0 THEN '1' ELSE '0' END Flag_Descuento_Colaborador,
        (SELECT CASE WHEN SUM(NVL(p.des_numero,0)) > 0 THEN '1' ELSE '0' END
           FROM ctx_productos_trx p
          WHERE h.hed_pais = p.hed_pais
            AND h.hed_origentrx = p.hed_origentrx
            AND h.hed_local = p.hed_local
            AND h.hed_pos = p.hed_pos
            AND h.hed_numtrx = p.hed_numtrx
            AND h.hed_fechatrx = p.hed_fechatrx
            AND h.hed_horatrx = p.hed_horatrx) Flag_Despacho,
        to_char(h.hed_fechatrx_origen,'YYYY-MM-DD') Fecha_Proceso_Ori,
        h.hed_local_origen Codigo_Local_Ori,
        NVL(h.hed_pos_origen,'0') Numero_Terminal_Ori,
        NVL(h.hed_numtrx_origen,'0') Numero_Transaccion_Ori,
        f.fid_nrodoc Numero_Documento_Cliente_LOY,
        h.hed_cnegop Numero_Centro_Negocio, 
        h.hed_subtipo Codigo_Subtipo_Trx,
        h.hed_numtar Numero_Tarjeta_Oh,
        NVL((SELECT a.cod_interno
           FROM puc_personas a
          WHERE lpad(f.fid_nrodoc,8,'0') = a.num_docum_ide
            AND a.tip_docum_ide = 141),0) Codigo_Interno,
        h.hed_timbmpago Tiempo_Cobro,
        h.hed_timbprod Tiempo_Escaneo,
        h.Hed_Rcgcodigo Codigo_Recarga,
        h.hed_rcgphone Numero_Recarga, 
        h.hed_giro Numero_Documento_Cliente_LOY_B 
    FROM ctx_header_trx h
        ,irs_locales    i
        ,ctx_fidelizacion_trx f
  WHERE i.loc_numero = h.hed_local
    AND h.hed_pais = f.hed_pais(+)
    AND h.hed_origentrx = f.hed_origentrx(+)
    AND h.hed_local = f.hed_local(+)
    AND h.hed_pos = f.hed_pos(+)
    AND h.hed_numtrx = f.hed_numtrx(+)
    AND h.hed_fechatrx = f.hed_fechatrx(+)
    AND h.hed_horatrx = f.hed_horatrx(+)
    AND h.hed_tipotrx IN ('AV','REC')
    AND h.hed_anulado = 'N'
    --AND h.hed_local = 61
    AND h.hed_fcontable = to_date('{fecha}','yyyymmdd')
    and h.hed_local={local}
    """
    return query_recaudaciones

executor=concurrent.futures.ThreadPoolExecutor(max_workers=thread)

def super_task(fecha,local,obj_ora):
    time.sleep(1)
    ls_rec=obj_ora.ejecutar_query(query_ct2_recaudaciones(fecha,local))
    return ls_rec

def request_recaudaciones_ct2(fecha_format_ini_f,ora):
    res = []
    fin = []
    x = []
    ls_loc=(list(row) for row in (ora.ejecutar_query(loc_rec)))
    print(ls_loc)
    for j in ls_loc:
        local=(j[0])
        #print(local)
        x.append(executor.submit(super_task,fecha_format_ini_f,local,ora))
    print(x)
    for r in concurrent.futures.as_completed(x):
          (r.result())
          if r.result() != None: 
            fin += r.result()
            res=(fin)

    df=pd.DataFrame(data=res,columns=["Fecha_Proceso", "Codigo_Local", "Numero_Terminal", "Numero_Transaccion", "Codigo_Vendedor", "Fecha_Compra", "Codigo_Tipo_Transaccion", "Codigo_Tipo_Doc_Trx", "Hora_Transaccion", "Estado_Transaccion", "Venta_Bruta", "Monto_Ajuste", "Venta_con_Descuento", "Venta_Neta", "Monto_Descuento_Item", "Monto_Redondeo", "Monto_Donacion", "Codigo_Cajero", "Tipo_Cambio", "Codigo_Tipo_Comprobante", "Ruc_Factura", "Serie_Transaccion", "Codigo_Razon_Devolucion", "Codigo_Supervisor", "Flag_Supergarantia", "Codigo_Tipo_Documento", "Numero_Documento_Cliente", "Numero_Tarjeta_Cliente", "Monto_IGV", "Monto_ISC", "Monto_IGV_Gasto", "Monto_Costo_Total", "Flag_Centro_Negocio", "Numero_Centro_Negocio_O", "Numero_Ticket", "Numero_Boleta_Electronica", "Tiempo_Entre_Transaccion", "Codigo_Car", "Numero_Documento_Car", "Flag_Descuento_Colaborador", "Flag_Despacho", "Fecha_Proceso_Ori", "Codigo_Local_Ori", "Numero_Terminal_Ori", "Numero_Transaccion_Ori", "Numero_Documento_Cliente_LOY", "Numero_Centro_Negocio", "Codigo_Subtipo_Trx", "Numero_Tarjeta_Oh", "Codigo_Interno", "Tiempo_Cobro", "Tiempo_Escaneo", "Codigo_Recarga", "Numero_Recarga", "Numero_Documento_Cliente_LOY_B"])
    df['Fecha_Proceso']=df['Fecha_Proceso'].map(str)
    df['Codigo_Local']=df['Codigo_Local'].map(str)
    df['Numero_Terminal']=df['Numero_Terminal'].map(str)
    df['Numero_Transaccion']=df['Numero_Transaccion'].map(str)
    df['Codigo_Vendedor']=df['Codigo_Vendedor'].map(str)
    df['Fecha_Compra']=df['Fecha_Compra'].map(str)
    df['Codigo_Tipo_Transaccion']=df['Codigo_Tipo_Transaccion'].map(str)
    df['Codigo_Tipo_Doc_Trx']=df['Codigo_Tipo_Doc_Trx'].map(str)
    df['Hora_Transaccion']=df['Hora_Transaccion'].map(str)
    df['Estado_Transaccion']=df['Estado_Transaccion'].map(str)
    df['Venta_Bruta']=df['Venta_Bruta'].map(str)
    df['Monto_Ajuste']=df['Monto_Ajuste'].map(str)
    df['Venta_con_Descuento']=df['Venta_con_Descuento'].map(str)
    df['Venta_Neta']=df['Venta_Neta'].map(str)
    df['Monto_Descuento_Item']=df['Monto_Descuento_Item'].map(str)
    df['Monto_Redondeo']=df['Monto_Redondeo'].map(str)
    df['Monto_Donacion']=df['Monto_Donacion'].map(str)
    df['Codigo_Cajero']=df['Codigo_Cajero'].map(str)
    df['Tipo_Cambio']=df['Tipo_Cambio'].map(str)
    df['Codigo_Tipo_Comprobante']=df['Codigo_Tipo_Comprobante'].map(str)
    df['Ruc_Factura']=df['Ruc_Factura'].map(str)
    df['Serie_Transaccion']=df['Serie_Transaccion'].map(str)
    df['Codigo_Razon_Devolucion']=df['Codigo_Razon_Devolucion'].map(str)
    df['Codigo_Supervisor']=df['Codigo_Supervisor'].map(str)
    df['Flag_Supergarantia']=df['Flag_Supergarantia'].map(str)
    df['Codigo_Tipo_Documento']=df['Codigo_Tipo_Documento'].map(str)
    df['Numero_Documento_Cliente']=df['Numero_Documento_Cliente'].map(str)
    df['Numero_Tarjeta_Cliente']=df['Numero_Tarjeta_Cliente'].map(str)
    df['Monto_IGV']=df['Monto_IGV'].map(str)
    df['Monto_ISC']=df['Monto_ISC'].map(str)
    df['Monto_IGV_Gasto']=df['Monto_IGV_Gasto'].map(str)
    df['Monto_Costo_Total']=df['Monto_Costo_Total'].map(str)
    df['Flag_Centro_Negocio']=df['Flag_Centro_Negocio'].map(str)
    df['Numero_Centro_Negocio_O']=df['Numero_Centro_Negocio_O'].map(str)
    df['Numero_Ticket']=df['Numero_Ticket'].map(str)
    df['Numero_Boleta_Electronica']=df['Numero_Boleta_Electronica'].map(str)
    df['Tiempo_Entre_Transaccion']=df['Tiempo_Entre_Transaccion'].map(str)
    df['Codigo_Car']=df['Codigo_Car'].map(str)
    df['Numero_Documento_Car']=df['Numero_Documento_Car'].map(str)
    df['Flag_Descuento_Colaborador']=df['Flag_Descuento_Colaborador'].map(str)
    df['Flag_Despacho']=df['Flag_Despacho'].map(str)
    df['Fecha_Proceso_Ori']=df['Fecha_Proceso_Ori'].map(str)
    df['Codigo_Local_Ori']=df['Codigo_Local_Ori'].map(str)
    df['Numero_Terminal_Ori']=df['Numero_Terminal_Ori'].map(str)
    df['Numero_Transaccion_Ori']=df['Numero_Transaccion_Ori'].map(str)
    df['Numero_Documento_Cliente_LOY']=df['Numero_Documento_Cliente_LOY'].map(str)
    df['Numero_Centro_Negocio']=df['Numero_Centro_Negocio'].map(str)
    df['Codigo_Subtipo_Trx']=df['Codigo_Subtipo_Trx'].map(str)
    df['Numero_Tarjeta_Oh']=df['Numero_Tarjeta_Oh'].map(str)
    df['Codigo_Interno']=df['Codigo_Interno'].map(str)
    df['Tiempo_Cobro']=df['Tiempo_Cobro'].map(str)
    df['Tiempo_Escaneo']=df['Tiempo_Escaneo'].map(str)
    df['Codigo_Recarga']=df['Codigo_Recarga'].map(str)
    df['Numero_Recarga']=df['Numero_Recarga'].map(str)
    df['Numero_Documento_Cliente_LOY_B']=df['Numero_Documento_Cliente_LOY_B'].map(str)


    return df

def conectar_bq(file_json_sbi):
    lg_bq=bigquery.Client.from_service_account_json(file_json_sbi)
    return lg_bq

def clear_table_bq(tabla,project,data_set,file_json_sbi):
    lg_bq=conectar_bq(file_json_sbi)
    query=f"""
      truncate table `{project}.{data_set}.{tabla}`
    """
    request_bq=lg_bq.query(query)
    request_bq.result()

def cargar_table_bq(obj_bq,df):
     print("cargando tmp...")
     resp_ins_tg=obj_bq.load_table_from_dataframe(df,f"""sistemas-bi.SPSA.tmp_ticket_recaudaciones_ct2""")
     resp_ins_tg.result()
     print("tmp procesado...")
     print("cargando sp...")
     resp_ins_fact=obj_bq.query(f"""call `sistemas-bi.SPSA.ins_fact_trx_recaudaciones`()""")
     resp_ins_fact.result()
     print("sp procesado...")
     
    
  
def procesar_recaudaciones_bq(fecha_format_ini_1,ora):
        fecha_format_ini_f=datetime.datetime.strftime(fecha_format_ini_1,'%Y%m%d')
        df_recaudaciones=request_recaudaciones_ct2(fecha_format_ini_f,ora)
        if df_recaudaciones.empty!=True:
            obj_bq=conectar_bq(file_json_sbi)
            cargar_table_bq(obj_bq,df_recaudaciones)

                 
ora=oracle(usuario,contraseña,ip_server_name)
ora.inicializar_oracle()
ora.conectar_oracle()

clear_table_bq("tmp_ticket_recaudaciones_ct2","sistemas-bi","SPSA",file_json_sbi)   

while fecha_format_ini_1<=fecha_format_fin_1:
    procesar_recaudaciones_bq(fecha_format_ini_1,ora)
    
    fecha_format_ini_1=fecha_format_ini_1+datetime.timedelta(days=1)
    fecha_var=datetime.datetime.strftime(fecha_format_ini_1,'%Y-%m-%d')
    print ("se termino de procesar el dia "+str(fecha_var)+"...")

ora.cerrar()




