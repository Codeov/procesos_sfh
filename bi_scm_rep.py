
from est_ora import oracle
from est_sql import *
import datetime
import pandas as pd
from google.cloud import bigquery

#credenciales ct2
usuario= "SXCT2SP"
contraseña= "CT2remiX"
ip_server_name= "10.20.11.11/SPT03"
directory_co=r"D:\instaladores\biblioteca cliente oracle\instantclient_11_2"

#credenciales 18
ip = '10.20.1.5'
bd= 'Retail_DW'
us= 'operador'
pw= 'operador'

file_json_sbi=r'D:\python\credenciales biq query\sistemas-bi-7a46b3894448.json'

fecha_ini= '2023-11-24'
fecha_fin='2023-12-04'
fecha_format_ini_1=datetime.datetime.strptime(fecha_ini,'%Y-%m-%d')
fecha_format_fin_1=datetime.datetime.strptime(fecha_fin,'%Y-%m-%d')
print(fecha_format_ini_1)

def query_recargas_bq(fecha_inicial,fecha_final) :
    bq_rec_cel= f"""
    select fecha,a.codigo_local,numero_terminal,numero_transaccion,codigo_tipo_trx,'RECARGA' codigo_sub_tipo_trx,
    hora_transaccion,id_ticket,codigo_recarga,numero_recarga,round(venta_neta,4) venta_neta
    from `sistemas-bi.SPSA.ticket_venta` A 
    inner join  `sistemas-bi.SPSA.dim_local` b on A.codigo_local = b.codigo_local
    where fecha between '{fecha_inicial}' and '{fecha_final}'
    and b.formato in ('PLAZA VEA','PLAZA VEA SUPER','PLAZA VEA EXPRESS','VIVANDA')
    and codigo_recarga is not null 
    and numero_recarga is not null ;
    """
    return bq_rec_cel


def query_ct2_recaudaciones (fecha):
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
    AND h.hed_local = 1593
    AND h.hed_fcontable = to_date('{fecha}','yyyymmdd')
    """
    return query_recaudaciones

def request_recaudaciones_ct2(query):
    ora=oracle(usuario,contraseña,ip_server_name)
    ora.inicializar_oracle()
    ora.conectar_oracle()
    ls_rec=ora.ejecutar_query(query)
    if ls_rec!=None:
        df=pd.DataFrame(data=ls_rec,columns=["Fecha_Proceso", "Codigo_Local", "Numero_Terminal", "Numero_Transaccion", "Codigo_Vendedor", "Fecha_Compra", "Codigo_Tipo_Transaccion", "Codigo_Tipo_Doc_Trx", "Hora_Transaccion", "Estado_Transaccion", "Venta_Bruta", "Monto_Ajuste", "Venta_con_Descuento", "Venta_Neta", "Monto_Descuento_Item", "Monto_Redondeo", "Monto_Donacion", "Codigo_Cajero", "Tipo_Cambio", "Codigo_Tipo_Comprobante", "Ruc_Factura", "Serie_Transaccion", "Codigo_Razon_Devolucion", "Codigo_Supervisor", "Flag_Supergarantia", "Codigo_Tipo_Documento", "Numero_Documento_Cliente", "Numero_Tarjeta_Cliente", "Monto_IGV", "Monto_ISC", "Monto_IGV_Gasto", "Monto_Costo_Total", "Flag_Centro_Negocio", "Numero_Centro_Negocio_O", "Numero_Ticket", "Numero_Boleta_Electronica", "Tiempo_Entre_Transaccion", "Codigo_Car", "Numero_Documento_Car", "Flag_Descuento_Colaborador", "Flag_Despacho", "Fecha_Proceso_Ori", "Codigo_Local_Ori", "Numero_Terminal_Ori", "Numero_Transaccion_Ori", "Numero_Documento_Cliente_LOY", "Numero_Centro_Negocio", "Codigo_Subtipo_Trx", "Numero_Tarjeta_Oh", "Codigo_Interno", "Tiempo_Cobro", "Tiempo_Escaneo", "Codigo_Recarga", "Numero_Recarga", "Numero_Documento_Cliente_LOY_B"])
        ls=df.values.tolist()
        return ls
    ora.cerrar()

def ins_scm_tmp_ticket_recaudaciones_ct2(fecha_format_ini_1,obj_sql):

        fecha_format_ini_f=datetime.datetime.strftime(fecha_format_ini_1,'%Y%m%d')
        ls_recaudaciones=request_recaudaciones_ct2(query_ct2_recaudaciones(fecha_format_ini_f))
        obj_sql.query_insert_tupla("insert into scm_tmp_ticket_recaudaciones_ct2 values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",ls_recaudaciones)
        obj_sql.commit()

def request_recargas_bq(json,query):
    bq_recargas=bigquery.Client.from_service_account_json(json)
    request_recargas=bq_recargas.query(query)
    request_recargas.result()
    df_recargas=pd.DataFrame(data=(request_recargas.to_dataframe()))
    return df_recargas.values.tolist()

sql_stg=SQLServer(ip,bd,us,pw)
sql_stg.connect_to_sql_server()
sql_stg.query("truncate table scm_tmp_ticket_recaudaciones_ct2")
sql_stg.query("truncate table scm_tmp_recargas_cel")
sql_stg.query ("truncate table scm_fact_recaudaciones_operaciones")
sql_stg.query ("truncate table scm_tmp_trx_serv_cfr")
sql_stg.query ("truncate table scm_data_economax_operaciones")
sql_stg.query ("truncate table scm_trx_medio_pago_item")
sql_stg.query ("truncate table scm_trx_dely_cso_abta_avg")

#scm_trx_medio_pago_item

while fecha_format_ini_1<=fecha_format_fin_1:
    ins_scm_tmp_ticket_recaudaciones_ct2(fecha_format_ini_1,sql_stg)
    fecha_var=datetime.datetime.strftime(fecha_format_ini_1,'%Y-%m-%d')
    ls_recargas=request_recargas_bq(file_json_sbi,query_recargas_bq(fecha_var,fecha_var))
    sql_stg.query_insert_tupla("insert into scm_tmp_recargas_cel values (?,?,?,?,?,?,?,?,?,?,?)",ls_recargas)
    sql_stg.commit()
    sql_stg.query(f"exec ins_scm_fact_recaudaciones_operaciones '{fecha_var}'")
    sql_stg.commit()
    sql_stg.query(f"exec ins_scm_tmp_trx_serv_cfr '{fecha_var}'")
    sql_stg.commit()
    #sql_stg.query(f"exec ins_scm_data_economax_operaciones '{fecha_var}'")
    #sql_stg.commit()
    #sql_stg.query(f"exec ins_Pronostico_Volumen_Kronos '{fecha_var}'")
    #sql_stg.commit()
    #sql_stg.query(f"exec ins_scm_trx_medio_pago_item '{fecha_var}'")
    #sql_stg.commit()
    #sql_stg.query(f"exec ins_scm_trx_dely_cso_abta_avg '{fecha_var}'")
    #sql_stg.commit()
    fecha_format_ini_1=fecha_format_ini_1+datetime.timedelta(days=1)
    print ("se termino de procesar el dia "+str(fecha_var)+"...")

sql_stg.cerrar()

#val_input_data_scm



