import datetime
import est_ora as ora
import est_bq as bq
import pandas as pd
import concurrent.futures
import threading
from email.message import EmailMessage
from smtplib import SMTP
import queue
import pytz

pd.set_option('future.no_silent_downcasting', True)

usuario= "SXCT2SP"
contraseña= "CT2remiX"
ip_server_name= "10.20.11.11/SPT03"

c=100

file_json = r"D:\python\credenciales biq query\vta_linea_sistemas-bi-438c564c407c.json"

zona_horaria_peru = pytz.timezone('America/Lima')
hora_actual=datetime.datetime.now(zona_horaria_peru).time()

formato_hora = "%H:%M:%S"
hora_1_1 = "00:00:00"
hora_1_1_f = datetime.datetime.strptime(hora_1_1, formato_hora).time()
hora_1_2 = "05:00:00"
hora_1_2_f = datetime.datetime.strptime(hora_1_2, formato_hora).time()

if hora_actual>=hora_1_1_f and hora_actual<=hora_1_2_f:
    fecha_actual_i=datetime.datetime.now()+datetime.timedelta(days=-1)
    fecha_actual_f=datetime.datetime.now()+datetime.timedelta(days=-1)
    fecha_actual_f_i=datetime.datetime.strftime(fecha_actual_i,"%Y%m%d")
    fecha_actual_f_f=datetime.datetime.strftime(fecha_actual_f,"%Y%m%d")
else:
    fecha_actual_i=datetime.datetime.now()
    fecha_actual_f=datetime.datetime.now()
    fecha_actual_f_i=datetime.datetime.strftime(fecha_actual_i,"%Y%m%d")
    fecha_actual_f_f=datetime.datetime.strftime(fecha_actual_f,"%Y%m%d")


print(f"Fecha de Carga {fecha_actual_f_i}")
print(f"Fecha de Carga {fecha_actual_f_f}")

q_local="""
SELECT distinct l.loc_numero    AS cod_suc_pos
    FROM irs_locales       l
        ,mae_proceso_local p
        ,irs_cadenas i
   WHERE l.loc_numero = p.cod_local
   and l.cad_numero=i.cad_numero
     AND p.cod_proceso in (13,29,22)
     AND l.loc_activo = 'S'
     AND p.tip_estado = 'A'
     and l.loc_numero <=5000
     and i.cad_numero not in (14,5,11)
     --and l.loc_numero in (195,264,37,81,2114,64)
     --and l.loc_numero in (195)
     order by 1 
"""

class ThreadWithException(threading.Thread):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.exception = None

    def run(self):
        try:
            if self._target:
                self._target(*self._args, **self._kwargs)
        except Exception as e:
            self.exception = e

def r_q_upd_tabla_carga(fecha_i,fecha_f):
    q_upd_tabla_carga=f"""
    update EXCT2SP.ICT2_TRXS_VTA_BI_GROUP_DIARIO h
    set h.flg_envvta=1
    where h.hed_fcontable in (to_date({fecha_i},'yyyymmdd'),to_date({fecha_f},'yyyymmdd'))
    and h.flg_envvta=0
    and exists
                (SELECT 1
                    FROM 
                    (
                        select distinct a.hed_pais,a.hed_origentrx,a.hed_local,a.hed_pos,a.hed_numtrx,a.hed_fechatrx,a.hed_horatrx
                        from  EXCT2SP.ICT2_TRXS_VTA_BI_GROUP_DIARIO a
                        left join EXCT2SP.ICT2_VAL_CARGA_VTA_BI_DIARIO b
                        on a.hed_fcontable=b.hed_fcontable and a.hed_local=b.hed_local
                        WHERE a.flg_envvta = 0
                        AND a.hed_fcontable in (to_date({fecha_i},'yyyymmdd'),to_date({fecha_f},'yyyymmdd'))
                        and b.hed_fcontable is null
                    )i
                    WHERE h.hed_pais = i.hed_pais
                    AND h.hed_origentrx = i.hed_origentrx
                    AND h.hed_local = i.hed_local
                    AND h.hed_pos = i.hed_pos
                    AND h.hed_numtrx = i.hed_numtrx
                    AND h.hed_fechatrx = i.hed_fechatrx
                    AND h.hed_horatrx = i.hed_horatrx
                )
    """
    return q_upd_tabla_carga

def r_q_trx_carga(fecha_i,fecha_f,codigo_local):
    q_trx_carga=f"""
    SELECT /*+ index(h) */ h.hed_pais
            ,h.hed_origentrx
            ,h.hed_local
            ,h.hed_pos
            ,h.hed_numtrx
            ,h.hed_fechatrx
            ,h.hed_horatrx
            ,h.hed_fcontable
            ,h.hed_tipotrx
            ,h.hed_tipodoc
            ,h.hed_subtipo
            ,SYSDATE
            ,h.hed_anulado
        FROM ctx_header_trx h
            ,mae_transaccion m
        WHERE h.hed_fcontable in (to_date('{fecha_i}','yyyymmdd'),to_date('{fecha_f}','yyyymmdd'))
        AND h.hed_local = {codigo_local}
        AND h.hed_tipotrx = m.mae_tipotrx
        AND h.hed_tipodoc = m.mae_tipodoc
        AND m.mae_tipotrx = 'PVT'
        AND m.mae_estadobi = 1
        AND h.hed_anulado = 'N'
        AND NOT EXISTS
            (SELECT 1
                FROM EXCT2SP.ICT2_TRXS_VTA_BI_GROUP_DIARIO i
                WHERE h.hed_pais = i.hed_pais
                AND h.hed_origentrx = i.hed_origentrx
                AND h.hed_local = i.hed_local
                AND h.hed_pos = i.hed_pos
                AND h.hed_numtrx = i.hed_numtrx
                AND h.hed_fechatrx = i.hed_fechatrx
                AND h.hed_horatrx = i.hed_horatrx)
    """
    return q_trx_carga

def r_q_cabecera(fecha_i,fecha_f,codigo_local):
    q_cabecera=f"""
SELECT /*+ index(h) */ to_char(h.hed_fcontable,'YYYY-MM-DD') AS Fecha_Proceso,
        h.hed_local  AS Codigo_Local,
        h.hed_pos  AS Numero_Terminal,
        h.hed_numtrx  AS Numero_Transaccion,
        NVL(h.hed_vendedor,'0') AS Codigo_Vendedor,
        to_char(h.hed_fechatrx,'YYYY-MM-DD') AS Fecha_Compra,
        h.hed_tipotrx  AS Codigo_Tipo_Transaccion,
        h.hed_tipodoc  AS Codigo_Tipo_Documento,
        to_char(h.hed_horatrx,'hh24:mi:ss') AS Hora_Transaccion,
        'V' AS Estado_Transaccion,
        (h.hed_brutopos + h.hed_brutoneg) AS Venta_Bruta,
        '0' AS Monto_Ajuste,
        (h.hed_brutopos + h.hed_brutoneg -
        NVL((SELECT SUM(s.dpr_monto)
               FROM ctx_dsctos_prod_trx s
              WHERE h.hed_pais = s.hed_pais
                AND h.hed_origentrx = s.hed_origentrx
                AND h.hed_local = s.hed_local
                AND h.hed_pos = s.hed_pos
                AND h.hed_numtrx = s.hed_numtrx
                AND h.hed_fechatrx = s.hed_fechatrx
                AND h.hed_horatrx = s.hed_horatrx),0)) AS Venta_con_Descuento,
        (h.hed_brutopos + h.hed_brutoneg - NVL(h.hed_impuesto,0) - nvl(h.hed_impuesto_isc,0)) AS Venta_Neta,
        NVL((SELECT SUM(s.dpr_monto)
               FROM ctx_dsctos_prod_trx s
              WHERE h.hed_pais = s.hed_pais
                AND h.hed_origentrx = s.hed_origentrx
                AND h.hed_local = s.hed_local
                AND h.hed_pos = s.hed_pos
                AND h.hed_numtrx = s.hed_numtrx
                AND h.hed_fechatrx = s.hed_fechatrx
                AND h.hed_horatrx = s.hed_horatrx),0) AS Monto_Descuento_Item,
        h.hed_redondeo AS Monto_Redondeo,
        h.hed_donacion AS Monto_Donacion,
        h.hed_cajero AS Codigo_Cajero,
        h.hed_tipocambio AS Tipo_Cambio,
        DECODE(h.hed_tipodoc,'TFC','1','BLT','3','NCR','7') AS Codigo_Tipo_Comprobante,
        CASE WHEN h.hed_tipoventa = 1 THEN h.hed_ndocashop ELSE h.hed_rutdoc END  AS Ruc_Factura,
        h.hed_serie AS Serie_Transaccion,
        h.hed_motivo AS Codigo_Razon_Devolucion,
        h.hed_autoriza AS Codigo_Supervisor,
        CASE WHEN (SELECT COUNT(1) FROM ctx_garantias_trx g
          WHERE h.hed_pais = g.hed_pais
            AND h.hed_origentrx = g.hed_origentrx
            AND h.hed_local = g.hed_local
            AND h.hed_pos = g.hed_pos
            AND h.hed_numtrx = g.hed_numtrx
            AND h.hed_fechatrx = g.hed_fechatrx
            AND h.hed_horatrx = g.hed_horatrx) > 0 THEN '1' ELSE '0' END AS Flag_Supergarantia,
        f.fid_tiporeg AS Fid_Tiporeg,
        DECODE(NVL((SELECT a.cod_interno
           FROM puc_personas a
          WHERE lpad(f.fid_nrodoc,8,'0') = a.num_docum_ide
            AND a.tip_docum_ide = 141),0),0,nvl(f.fid_nrodoc,0),0)AS Numero_Documento_Cliente,
        to_char(f.fid_nrotarj) AS Numero_Tarjeta_Cliente,
        h.hed_impuesto AS Monto_IGV,
        nvl(h.hed_impuesto_isc,0) AS Monto_ISC,
        (SELECT NVL(SUM(t.impp_montoimp),0)
           FROM ctx_impsto_dsctos_prod_trx t
          WHERE h.hed_pais = t.hed_pais
            AND h.hed_origentrx = t.hed_origentrx
            AND h.hed_local = t.hed_local
            AND h.hed_pos = t.hed_pos
            AND h.hed_numtrx = t.hed_numtrx
            AND h.hed_fechatrx = t.hed_fechatrx
            AND h.hed_horatrx = t.hed_horatrx
            AND t.impp_codimp IN ('GM','GF')) AS Monto_IGV_Gasto,
        '0'AS Monto_Costo_Total,
        CASE WHEN LENGTH(h.hed_cnegop)>1 THEN '1' ELSE '0' END AS Flag_Centro_Negocio,
        '0' AS Numero_Centro_Negocio_O,
        '0'AS Numero_Ticket,
        h.hed_numdocdel AS Numero_Boleta_Electronica,
        h.hed_entretrx AS Tiempo_Entre_Transaccion,
        h.hed_codcar AS Codigo_Car,
        h.hed_rutdoc AS Numero_Documento_Car,
        CASE WHEN (SELECT COUNT(1) FROM ctx_dsctos_prod_trx ds
          WHERE h.hed_pais = ds.hed_pais
            AND h.hed_origentrx = ds.hed_origentrx
            AND h.hed_local = ds.hed_local
            AND h.hed_pos = ds.hed_pos
            AND h.hed_numtrx = ds.hed_numtrx
            AND h.hed_fechatrx = ds.hed_fechatrx
            AND h.hed_horatrx = ds.hed_horatrx
            AND ds.dpr_coddcto in ('411149',
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
       '15670819'

)) > 0 THEN '1' ELSE '0' END AS Flag_Descuento_Colaborador,
        (SELECT CASE WHEN SUM(NVL(p.des_numero,0)) > 0 THEN '1' ELSE '0' END
           FROM ctx_productos_trx p
          WHERE h.hed_pais = p.hed_pais
            AND h.hed_origentrx = p.hed_origentrx
            AND h.hed_local = p.hed_local
            AND h.hed_pos = p.hed_pos
            AND h.hed_numtrx = p.hed_numtrx
            AND h.hed_fechatrx = p.hed_fechatrx
            AND h.hed_horatrx = p.hed_horatrx) AS Flag_Despacho,
        to_char(h.hed_fechatrx_origen,'YYYY-MM-DD') AS Fecha_Proceso_Ori,
        h.hed_local_origen AS Codigo_Local_Ori,
        NVL(h.hed_pos_origen,'0') AS Numero_Terminal_Ori,
        NVL(h.hed_numtrx_origen,'0') AS Numero_Transaccion_Ori,
        f.fid_nrodoc AS Numero_Documento_Cliente_LOY,
        h.hed_cnegop AS Numero_Centro_Negocio,
        b.hed_subtipo,
        h.hed_numtar AS Numero_Tarjeta,
        NVL((SELECT a.cod_interno
           FROM puc_personas a
          WHERE lpad(f.fid_nrodoc,8,'0') = a.num_docum_ide
            AND a.tip_docum_ide = 141),0) num_docum_ide,
        h.hed_timbmpago,
        h.hed_timbprod,
        h.Hed_Rcgcodigo AS Codigo_Recarga,
        h.hed_rcgphone AS Numero_Recarga,
        h.hed_giro AS Numero_Documento_Cliente_LOY_B,
        h.HED_TIPOVENTA  AS Agora_Shop,
        h.HED_TDOCCLTE ,
        h.HED_NOMCLTE ,
        h.HED_APECLTE ,
        h.hed_ecompedido AS Pedido_Ecommerce
    FROM ctx_header_trx h
        ,EXCT2SP.ICT2_TRXS_VTA_BI_GROUP_DIARIO b
        ,ctx_fidelizacion_trx f
  WHERE h.hed_pais = b.hed_pais
    AND h.hed_origentrx = b.hed_origentrx
    AND h.hed_local = b.hed_local
    AND h.hed_pos = b.hed_pos
    AND h.hed_numtrx = b.hed_numtrx
    AND h.hed_fechatrx = b.hed_fechatrx
    AND h.hed_horatrx = b.hed_horatrx
    AND h.hed_pais = f.hed_pais(+)
    AND h.hed_origentrx = f.hed_origentrx(+)
    AND h.hed_local = f.hed_local(+)
    AND h.hed_pos = f.hed_pos(+)
    AND h.hed_numtrx = f.hed_numtrx(+)
    AND h.hed_fechatrx = f.hed_fechatrx(+)
    AND h.hed_horatrx = f.hed_horatrx(+)
    AND b.hed_tipotrx IN ('PVT')
    AND b.hed_tipodoc IN ('BLT','TFC','NCR')
    AND b.hed_anulado = 'N'
    AND b.hed_local = {codigo_local}
    AND b.hed_fcontable in (to_date('{fecha_i}','yyyymmdd'),to_date('{fecha_f}','yyyymmdd'))
    AND b.flg_envvta = 0
    """
    return q_cabecera

def r_q_detalle(fecha_i,fecha_f,codigo_local):
    q_detalle=f"""
    SELECT /*+ index(p) */ to_char(p.ptr_fcontable,'YYYY-MM-DD') Fecha_Proceso,
        p.hed_local   Codigo_Local,
        p.hed_pos   Numero_Terminal,
        p.hed_numtrx    Numero_Transaccion,
        nvl(p.ptr_vendedor,0) Codigo_Vendedor,
        p.ptr_corrprod  Numero_Correlativo,
        p.ptr_codinterno Codigo_Producto,
        p.ptr_codprod     Codigo_EAN,
        CASE
          WHEN to_number(p.ptr_codprod)
            IN (SELECT to_number(r.cod_ean)
                  FROM ict2_cod_recargas r)
          THEN  p.ptr_total
            ELSE p.ptr_unidades + p.ptr_peso END Venta_Unidad,
        (p.ptr_brutopos + p.ptr_brutoneg - p.ptr_mdescto) Venta_Bruta,
        (p.ptr_brutopos + p.ptr_brutoneg) Venta_Con_Descuento,
        p.ptr_mdescto    Monto_Dscto_Producto,
        '0'   Monto_Dscto_Proporcional,
        p.ptr_impuesto  Monto_IGV,
        nvl(p.ptr_impuesto_isc,'0')  Monto_ISC,
        (p.ptr_brutopos + p.ptr_brutoneg - nvl(p.ptr_impuesto,0) - nvl(p.ptr_impuesto_isc,0)) Venta_Neta,
        nvl(p.ptr_cprecioaut,'0') Codigo_Supervisor_C_Precio,
        nvl(p.ptr_cpreciomot,'0') Codigo_Motivo,
        p.ptr_afecto Flag_Afecto,
        p.ptr_impuesto Valor_Igv,
        (SELECT NVL(SUM(t.impp_montoimp),0)
           FROM ctx_impsto_dsctos_prod_trx t
          WHERE h.hed_pais = t.hed_pais
            AND h.hed_origentrx = t.hed_origentrx
            AND h.hed_local = t.hed_local
            AND h.hed_pos = t.hed_pos
            AND h.hed_numtrx = t.hed_numtrx
            AND h.hed_fechatrx = t.hed_fechatrx
            AND h.hed_horatrx = t.hed_horatrx
            AND t.impp_codimp IN ('GM','GF','GA')
            AND p.ptr_corrprod = t.ptr_corrprod ) Monto_Igv_Dscto_Gasto,
        (SELECT NVL(SUM(t.impp_montoimp),0)
           FROM ctx_impsto_dsctos_prod_trx t
          WHERE p.hed_pais = t.hed_pais
            AND p.hed_origentrx = t.hed_origentrx
            AND p.hed_local = t.hed_local
            AND p.hed_pos = t.hed_pos
            AND p.hed_numtrx = t.hed_numtrx
            AND p.hed_fechatrx = t.hed_fechatrx
            AND p.hed_horatrx = t.hed_horatrx
            AND t.impp_codimp IN ('MT','MP','GM','GF','GA')
            AND p.ptr_corrprod = t.ptr_corrprod) Monto_Igv_Dscto,
        '0' Monto_Precio_Reg,
        '0' Monto_Precio_Vig,
        '0' Monto_Costo_Promedio,
        p.ptr_cprecionvo Valor_Precio_Cambio,
        p.ptr_prorrateo Monto_Dscto_Prd_Cambioprecio,
        p.ptr_preciovta Monto_Precio_Unitario,
        '0' Monto_Ajuste_Trx,
        (SELECT nvl(SUM(d.dpr_monto2),0)
            FROM ctx_dsctos_prod_trx d
           WHERE p.hed_pais = d.hed_pais
             AND p.hed_origentrx = d.hed_origentrx
             AND p.hed_local = d.hed_local
             AND p.hed_pos = d.hed_pos
             AND p.hed_numtrx = d.hed_numtrx
             AND p.hed_fechatrx = d.hed_fechatrx
             AND p.hed_horatrx = d.hed_horatrx
             AND p.ptr_corrprod = d.ptr_corrprod ) Monto_Dscto_Margen_Ticket,
        (SELECT nvl(SUM(d.dpr_monto1),0)
            FROM ctx_dsctos_prod_trx d
           WHERE p.hed_pais = d.hed_pais
             AND p.hed_origentrx = d.hed_origentrx
             AND p.hed_local = d.hed_local
             AND p.hed_pos = d.hed_pos
             AND p.hed_numtrx = d.hed_numtrx
             AND p.hed_fechatrx = d.hed_fechatrx
             AND p.hed_horatrx = d.hed_horatrx
             AND p.ptr_corrprod = d.ptr_corrprod) Monto_Dscto_Margen_Producto,
        (SELECT nvl(SUM(d.dpr_monto3),0)
            FROM ctx_dsctos_prod_trx d
           WHERE p.hed_pais = d.hed_pais
             AND p.hed_origentrx = d.hed_origentrx
             AND p.hed_local = d.hed_local
             AND p.hed_pos = d.hed_pos
             AND p.hed_numtrx = d.hed_numtrx
             AND p.hed_fechatrx = d.hed_fechatrx
             AND p.hed_horatrx = d.hed_horatrx
             AND p.ptr_corrprod = d.ptr_corrprod) Monto_Dscto_Gasto_Financiero,
         (SELECT nvl(SUM(d.dpr_monto4),0)
            FROM ctx_dsctos_prod_trx d
           WHERE p.hed_pais = d.hed_pais
             AND p.hed_origentrx = d.hed_origentrx
             AND p.hed_local = d.hed_local
             AND p.hed_pos = d.hed_pos
             AND p.hed_numtrx = d.hed_numtrx
             AND p.hed_fechatrx = d.hed_fechatrx
             AND p.hed_horatrx = d.hed_horatrx
             AND p.ptr_corrprod = d.ptr_corrprod) Monto_Dscto_Gasto_Marketing,
         (SELECT NVL(SUM(t.impp_montoimp),0)
           FROM ctx_impsto_dsctos_prod_trx t
          WHERE p.hed_pais = t.hed_pais
            AND p.hed_origentrx = t.hed_origentrx
            AND p.hed_local = t.hed_local
            AND p.hed_pos = t.hed_pos
            AND p.hed_numtrx = t.hed_numtrx
            AND p.hed_fechatrx = t.hed_fechatrx
            AND p.hed_horatrx = t.hed_horatrx
            AND t.impp_codimp = 'MT'
            AND p.ptr_corrprod = t.ptr_corrprod) GV_Margen_Ticket,
         (SELECT NVL(SUM(t.impp_montoimp),0)
           FROM ctx_impsto_dsctos_prod_trx t
          WHERE p.hed_pais = t.hed_pais
            AND p.hed_origentrx = t.hed_origentrx
            AND p.hed_local = t.hed_local
            AND p.hed_pos = t.hed_pos
            AND p.hed_numtrx = t.hed_numtrx
            AND p.hed_fechatrx = t.hed_fechatrx
            AND p.hed_horatrx = t.hed_horatrx
            AND t.impp_codimp = 'MP'
            AND p.ptr_corrprod = t.ptr_corrprod) IGV_Margen_Producto,
         (SELECT NVL(SUM(t.impp_montoimp),0)
           FROM ctx_impsto_dsctos_prod_trx t
          WHERE p.hed_pais = t.hed_pais
            AND p.hed_origentrx = t.hed_origentrx
            AND p.hed_local = t.hed_local
            AND p.hed_pos = t.hed_pos
            AND p.hed_numtrx = t.hed_numtrx
            AND p.hed_fechatrx = t.hed_fechatrx
            AND p.hed_horatrx = t.hed_horatrx
            AND t.impp_codimp = 'GF'
            AND p.ptr_corrprod = t.ptr_corrprod ) IGV__Gasto_Financiero,
        (SELECT NVL(SUM(t.impp_montoimp),0)
           FROM ctx_impsto_dsctos_prod_trx t
          WHERE p.hed_pais = t.hed_pais
            AND p.hed_origentrx = t.hed_origentrx
            AND p.hed_local = t.hed_local
            AND p.hed_pos = t.hed_pos
            AND p.hed_numtrx = t.hed_numtrx
            AND p.hed_fechatrx = t.hed_fechatrx
            AND p.hed_horatrx = t.hed_horatrx
            AND t.impp_codimp = 'GM'
            AND p.ptr_corrprod = t.ptr_corrprod ) IGV_Gasto_Marketing,
         CASE
           WHEN (NVL(p.des_numero,0)) > 0
             THEN '1' ELSE '0' END Flag_Despacho,
         (SELECT dp.des_nombre
             FROM ctx_despachos_trx dp
            WHERE p.hed_pais = dp.hed_pais
              AND p.hed_origentrx = dp.hed_origentrx
              AND p.hed_local = dp.hed_local
              AND p.hed_pos = dp.hed_pos
              AND p.hed_numtrx = dp.hed_numtrx
              AND p.hed_fechatrx = dp.hed_fechatrx
              AND p.hed_horatrx = dp.hed_horatrx
              AND p.des_numero = dp.des_numero ) Nom_Cliente_dd,
        p.des_numero Num_NV,
        '0' Num_nv_Ori,
        p.ptr_ingreso  AS Flag_ingreso
    FROM EXCT2SP.ICT2_TRXS_VTA_BI_GROUP_DIARIO h
        ,ctx_productos_trx p
   WHERE  h.hed_pais = p.hed_pais
     AND h.hed_origentrx = p.hed_origentrx
     AND h.hed_local = p.hed_local
     AND h.hed_pos = p.hed_pos
     AND h.hed_numtrx = p.hed_numtrx
     AND h.hed_fechatrx = p.hed_fechatrx
     AND h.hed_horatrx = p.hed_horatrx
     AND h.hed_tipotrx = 'PVT'
     AND h.hed_tipodoc IN ('BLT','TFC','NCR')
     AND h.hed_local = {codigo_local}
    AND h.hed_fcontable in (to_date('{fecha_i}','yyyymmdd'),to_date('{fecha_f}','yyyymmdd'))
    AND h.flg_envvta = 0
    """
    return q_detalle


def r_q_fp_externo(fecha_i,fecha_f,codigo_local):

    q_fp_externo=f"""
--forma pago externo
  SELECT to_char(h.hed_fcontable,'YYYY-MM-DD') AS Fecha_Proceso,
        l.loc_numeropmm  AS Codigo_Local,
        h.hed_pos  AS Numero_Terminal,
        h.hed_numtrx  AS Numero_Transaccion,
        d.pag_corrpago  AS Correlativo,
        d.pag_tipopago  AS Codigo_Pago,
        d.pag_tipopago  AS Codigo_Tipo_Pago,
        d.pag_bmagnetica  AS Numero_Cuenta,
        d.pag_tipocuota  AS Tipo_Cuota,
        d.pag_nrocuotas  AS Numero_Cuota,
        d.pag_monto  AS Importe_Pago_Bruto,
        NVL(d.pag_rutcli,'0')  AS Numero_RUC,
        '0'  AS Fecha_Expiracion,
        d.pag_codautor  AS Codigo_Autorizacion,
        '0'  AS Numero_Cupon,
        d.pag_codmoneda  AS Codigo_Moneda,
        d.pag_flgbanda  AS Marca_Banda,
        d.pag_flgbatch  AS Marca_Batch,
        d.pag_online  AS Codigo_En_Linea,
        d.pag_tdocid  AS Codigo_Tipo_Documento,
        d.pag_cargo  AS Codigo_Cargo,
        d.pag_numvouch  AS Codigo_Referencia,
        d.pag_nrounico AS ID_VISA,
        d.pag_fonocli  AS Numero_Documento,
        '0'  AS Numero_Voucher,
        d.pag_recargo  AS Monto_Recargo,
        d.pag_supervisor  AS Codigo_Supervisor,
        d.pag_tipocredito  AS Tipo_Credito,
        d.pag_numcta AS linea--  AS Numero_Tarjeta_Mask
        FROM EXCT2SP.ICT2_TRXS_VTA_BI_GROUP_DIARIO h
            ,irs_locales l
            ,ctx_pagos_trx d
        WHERE l.loc_numero = h.hed_local
          AND h.hed_pais = d.hed_pais
          AND h.hed_origentrx = d.hed_origentrx
          AND h.hed_local = d.hed_local
          AND h.hed_pos = d.hed_pos
          AND h.hed_numtrx = d.hed_numtrx
          AND h.hed_fechatrx = d.hed_fechatrx
          AND h.hed_horatrx = d.hed_horatrx
          --AND h.hed_tipotrx = 'PVT'
          --AND h.hed_tipodoc IN ('BLT','TFC','NCR')
          AND h.hed_local = {codigo_local}
          AND h.hed_fcontable in (to_date('{fecha_i}','yyyymmdd'),to_date('{fecha_f}','yyyymmdd'))
          AND h.flg_envvta = 0
          AND d.pag_tipopago IN 
          (SELECT CFG_VALORPARAM FROM EXCT2SP.ict2_configuraciones WHERE CFG_INTERFAZ_ID = 'ITF_BI' AND CFG_NOMPARAM = 'PAGO_EXT')
          --AND 1=2
    """
    return q_fp_externo

def r_q_fp_interno(fecha_i,fecha_f,codigo_local):
    
    q_fp_interno=f"""
--forma pago interno
SELECT to_char(h.hed_fcontable,'YYYY-MM-DD') AS Fecha_Proceso,
        l.loc_numeropmm AS Codigo_Local,
        h.hed_pos AS Numero_Terminal,
        h.hed_numtrx AS Numero_Transaccion,
        d.pag_corrpago AS Correlativo,
        d.pag_tipopago AS Codigo_Pago,
        d.pag_tipopago AS Codigo_Tipo_Pago,
        d.pag_bmagnetica AS Numero_Cuenta,
        d.pag_monto AS Importe_Pago_Bruto,
        d.pag_fonocli AS Numero_RUC,
        d.pag_flgbanda AS Marca_Banda,
        d.pag_flgtitular AS Marca_Banda_Magnetica,
        d.pag_userid AS Id_Usuario,
        d.pag_flgrepacta AS Marca_Repactacion,
        d.pag_estado AS Estado,
        d.pag_flginteres AS Marca_Interes,
        d.pag_codautor AS Codigo_Autorizacion,
        d.pag_codmoneda AS Codigo_Moneda,
        '0' AS Banda_Magnetica,
        d.pag_online AS Codigo_En_Linea,
        d.pag_tdocid AS Codigo_Tipo_Documento,
        NVL(d.pag_rutcli,'0') AS Numero_Documento,
        d.pag_tipocredito AS Tipo_Credito,
        d.pag_montocuota AS Valor_Cuota,
        d.pag_numvouch AS linea-- AS Codigo_Referencia
    FROM EXCT2SP.ICT2_TRXS_VTA_BI_GROUP_DIARIO h
        ,irs_locales    l
        ,ctx_pagos_trx  d
   WHERE l.loc_numero = h.hed_local
     AND h.hed_pais = d.hed_pais
     AND h.hed_origentrx = d.hed_origentrx
     AND h.hed_local = d.hed_local
     AND h.hed_pos = d.hed_pos
     AND h.hed_numtrx = d.hed_numtrx
     AND h.hed_fechatrx = d.hed_fechatrx
     AND h.hed_horatrx = d.hed_horatrx
     AND h.hed_tipotrx IN ('AV','PVT','REC')
     AND h.hed_anulado = 'N'
     --AND h.hed_tipotrx = 'PVT'
     --AND h.hed_tipodoc IN ('BLT','TFC','NCR')
     AND h.hed_local = {codigo_local}
     AND h.hed_fcontable in (to_date('{fecha_i}','yyyymmdd'),to_date('{fecha_f}','yyyymmdd'))
     AND h.flg_envvta = 0
     AND d.pag_tipopago IN (16)
    """
    return q_fp_interno

def r_q_fp_efectivo(fecha_i,fecha_f,codigo_local):
    
    q_fp_efectivo=f"""
--forma pago efectivo
  SELECT to_char(h.hed_fcontable,'YYYY-MM-DD') AS Fecha_Proceso,
           l.loc_numeropmm AS Codigo_Local,
           h.hed_pos AS Numero_Terminal,
           h.hed_numtrx AS Numero_Transaccion,
           d.pag_corrpago AS Correlativo,
           d.pag_tipopago AS Codigo_Pago,
           d.pag_tipopago AS Codigo_Tipo_Pago,
           d.pag_codmoneda AS Codigo_Moneda,
           CASE
             WHEN d.pag_tipopago='30'
               THEN
                 ((d.pag_monto*d.pag_tipocambio) - d.pag_vuelto)
                 ELSE
                   (d.pag_monto - d.pag_vuelto) END AS Importe_Pago_Bruto,
           d.pag_vuelto AS Monto_Vuelto,
           d.pag_redondeo AS Monto_Redondeo,
           d.pag_donacion AS linea-- AS Monto_Donacion
      FROM EXCT2SP.ICT2_TRXS_VTA_BI_GROUP_DIARIO h
          ,irs_locales    l
          ,ctx_pagos_trx  d
     WHERE l.loc_numero = h.hed_local
       AND h.hed_pais = d.hed_pais
       AND h.hed_origentrx = d.hed_origentrx
       AND h.hed_local = d.hed_local
       AND h.hed_pos = d.hed_pos
       AND h.hed_numtrx = d.hed_numtrx
       AND h.hed_fechatrx = d.hed_fechatrx
       AND h.hed_horatrx = d.hed_horatrx
       AND h.hed_tipotrx IN ('AV','PVT','REC')
         AND h.hed_anulado = 'N'
       --</CRB-200717>
       --AND h.hed_tipotrx = 'PVT'
       --AND h.hed_tipodoc IN ('BLT','TFC','NCR')
       AND h.hed_local = {codigo_local}
       AND h.hed_fcontable in (to_date('{fecha_i}','yyyymmdd'),to_date('{fecha_f}','yyyymmdd'))
       AND h.flg_envvta = 0
       AND d.pag_tipopago IN (1,30)
    """
    return q_fp_efectivo

def r_q_fp_vale(fecha_i,fecha_f,codigo_local):
    
    q_fp_vale=f"""
    
--forma pago vale
SELECT to_char(h.hed_fcontable,'YYYY-MM-DD') AS Fecha_Proceso,
      l.loc_numeropmm AS Codigo_Local,
      h.hed_pos AS Numero_Terminal,
      h.hed_numtrx AS Numero_Transaccion,
      d.pag_corrpago AS Correlativo,
      d.pag_tipopago AS Codigo_Pago,
      d.pag_tipopago AS Codigo_Tipo_Pago,
      d.pag_fonocli AS Numero_RUC,
      d.pag_numcta AS Numero_Vale,
      d.pag_numserie AS Numero_Serie_Vale,
      d.pag_supervisor AS Codigo_Supervisor,
      d.pag_fechacred AS Fecha_Vale_Credito,
      DECODE(d.pag_tipopago,'02','NC','VL') AS Codigo_Tipo_Vale,
      h.hed_fechatrx_origen AS Fecha_Proceso_NC,
      h.hed_local_origen AS Codigo_Local_NC,
      h.hed_pos_origen AS Numero_Terminal_NC,
      h.hed_numtrx_origen AS Numero_Transaccion_NC,
      d.pag_monto AS linea -- AS Importe_Pago_Bruto
  FROM ctx_header_trx h
      ,EXCT2SP.ICT2_TRXS_VTA_BI_GROUP_DIARIO t
      ,irs_locales     l
      ,ctx_pagos_trx   d
 WHERE l.loc_numero = h.hed_local
   AND h.hed_pais = d.hed_pais
   AND h.hed_origentrx = d.hed_origentrx
   AND h.hed_local = d.hed_local
   AND h.hed_pos = d.hed_pos
   AND h.hed_numtrx = d.hed_numtrx
   AND h.hed_fechatrx = d.hed_fechatrx
   AND h.hed_horatrx = d.hed_horatrx
   AND h.hed_pais = t.hed_pais
   AND h.hed_origentrx = t.hed_origentrx
   AND h.hed_local = t.hed_local
   AND h.hed_pos = t.hed_pos
   AND h.hed_numtrx = t.hed_numtrx
   AND h.hed_fechatrx = t.hed_fechatrx
   AND h.hed_horatrx = t.hed_horatrx
   AND h.hed_tipotrx IN ('AV','PVT','REC')
   AND h.hed_anulado = 'N'
   --AND h.hed_tipotrx = 'PVT'
   --AND h.hed_tipodoc IN ('BLT','TFC','NCR')
   AND h.hed_local = {codigo_local}
   AND h.hed_fcontable in (to_date('{fecha_i}','yyyymmdd'),to_date('{fecha_f}','yyyymmdd'))
   AND t.flg_envvta = 0
   AND d.pag_tipopago IN (2,3,5,6)
    """
    return q_fp_vale

def r_q_promocion(fecha_i,fecha_f,codigo_local):
    
    r_q_promocion=f"""
    SELECT to_char(h.hed_fcontable,'YYYY-MM-DD')  AS Fecha_Proceso,
         l.loc_numeropmm  AS Codigo_Local,
         h.hed_pos  AS Numero_Terminal,
         h.hed_numtrx  AS Numero_Tansaccion,
         d.ptr_corrprod  AS Correlativo,
         d.dpr_coddcto  AS Codigo_Promocion,
         d.dpr_monto2  AS Monto_Dscto_Margen_Ticket,
         d.dpr_monto1f  AS Monto_Dscto_Margen_Producto,
         d.dpr_monto3  AS Monto_Dscto_Gasto_Financiero,
         d.dpr_monto4  AS Monto_Dscto_Gasto_Marketing,
         (SELECT NVL(SUM(t.impp_montoimp),0)
           FROM ctx_impsto_dsctos_prod_trx t
          WHERE h.hed_pais = t.hed_pais
            AND h.hed_origentrx = t.hed_origentrx
            AND h.hed_local = t.hed_local
            AND h.hed_pos = t.hed_pos
            AND h.hed_numtrx = t.hed_numtrx
            AND h.hed_fechatrx = t.hed_fechatrx
            AND h.hed_horatrx = t.hed_horatrx
            AND t.ptr_corrprod = d.ptr_corrprod
            AND t.dpr_corrdcto = d.dpr_corrdcto
            AND t.impp_codimp = 'MT' )  AS IGV_Dscto_MT,
         (SELECT NVL(SUM(t.impp_montoimp),0)
           FROM ctx_impsto_dsctos_prod_trx t
          WHERE h.hed_pais = t.hed_pais
            AND h.hed_origentrx = t.hed_origentrx
            AND h.hed_local = t.hed_local
            AND h.hed_pos = t.hed_pos
            AND h.hed_numtrx = t.hed_numtrx
            AND h.hed_fechatrx = t.hed_fechatrx
            AND h.hed_horatrx = t.hed_horatrx
            AND t.ptr_corrprod = d.ptr_corrprod
            AND t.dpr_corrdcto = d.dpr_corrdcto
            AND t.impp_codimp = 'MP' )  AS IGV_Dscto_MP,
         (SELECT NVL(SUM(t.impp_montoimp),0)
           FROM ctx_impsto_dsctos_prod_trx t
          WHERE h.hed_pais = t.hed_pais
            AND h.hed_origentrx = t.hed_origentrx
            AND h.hed_local = t.hed_local
            AND h.hed_pos = t.hed_pos
            AND h.hed_numtrx = t.hed_numtrx
            AND h.hed_fechatrx = t.hed_fechatrx
            AND h.hed_horatrx = t.hed_horatrx
            AND t.ptr_corrprod = d.ptr_corrprod
            AND t.dpr_corrdcto = d.dpr_corrdcto
            AND t.impp_codimp = 'GF' )  AS IGV_Dscto_GF,
         (SELECT NVL(SUM(t.impp_montoimp),0)
           FROM ctx_impsto_dsctos_prod_trx t
          WHERE h.hed_pais = t.hed_pais
            AND h.hed_origentrx = t.hed_origentrx
            AND h.hed_local = t.hed_local
            AND h.hed_pos = t.hed_pos
            AND h.hed_numtrx = t.hed_numtrx
            AND h.hed_fechatrx = t.hed_fechatrx
            AND h.hed_horatrx = t.hed_horatrx
            AND t.ptr_corrprod = d.ptr_corrprod
            AND t.dpr_corrdcto = d.dpr_corrdcto
            AND t.impp_codimp = 'GM' )  AS IGV_Dscto_GM,
         NVL(d.dpr_usuario1,0)  AS Cantidad_Descuento,
         d.dpr_monto  AS Monto_Descuento,
         (d.dpr_monto3 + d.dpr_monto4)  AS Monto_Descuento_Gasto,
         '0'  AS Flag_PV_PL,
         '0'  AS Tipo_PV_PL,
         '0'  AS Cantidad_Peso,
         '0' AS Numero_Cupon,
         d.ptr_codprod  AS codigo_producto,
         d.dpr_monto5  AS Monto_Dscto_Gasto_Agora ,
        (SELECT NVL(SUM(t.impp_montoimp),0)
           FROM ctx_impsto_dsctos_prod_trx t
          WHERE h.hed_pais = t.hed_pais
            AND h.hed_origentrx = t.hed_origentrx
            AND h.hed_local = t.hed_local
            AND h.hed_pos = t.hed_pos
            AND h.hed_numtrx = t.hed_numtrx
            AND h.hed_fechatrx = t.hed_fechatrx
            AND h.hed_horatrx = t.hed_horatrx
            AND t.ptr_corrprod = d.ptr_corrprod
            AND t.dpr_corrdcto = d.dpr_corrdcto
            AND t.impp_codimp = 'GA' )  
            AS linea 
     FROM EXCT2SP.ICT2_TRXS_VTA_BI_GROUP_DIARIO h
         ,irs_locales l
         ,ctx_dsctos_prod_trx d
    WHERE l.loc_numero = h.hed_local
      AND h.hed_pais = d.hed_pais
      AND h.hed_origentrx = d.hed_origentrx
      AND h.hed_local = d.hed_local
      AND h.hed_pos = d.hed_pos
      AND h.hed_numtrx = d.hed_numtrx
      AND h.hed_fechatrx = d.hed_fechatrx
      AND h.hed_horatrx = d.hed_horatrx
      AND h.hed_tipotrx = 'PVT'
      AND h.hed_tipodoc IN ('BLT','TFC','NCR')
      AND h.hed_local = {codigo_local}
      AND h.hed_fcontable in (to_date('{fecha_i}','yyyymmdd'),to_date('{fecha_f}','yyyymmdd'))
      AND h.flg_envvta = 0
    """

    return r_q_promocion


def listar_locales(obj_ora,query):
    tp_locales=obj_ora.ejecutar_query(query)
    ls_locales=[list(i) for i in tp_locales]
    return ls_locales

def procesar_cab_df(fecha_i,fecha_f,cod_local,obj_ora_s,q_c):
    query_trx_cab=r_q_cabecera(fecha_i,fecha_f,cod_local)
    tp_trx_cab=obj_ora_s.ejecutar_query(query_trx_cab)
    df_cab=pd.DataFrame(data=tp_trx_cab,columns=["FECHA_PROCESO","CODIGO_LOCAL","NUMERO_TERMINAL","NUMERO_TRANSACCION","CODIGO_VENDEDOR","FECHA_COMPRA","CODIGO_TIPO_TRANSACCION","CODIGO_TIPO_DOCUMENTO","HORA_TRANSACCION","ESTADO_TRANSACCION","VENTA_BRUTA","MONTO_AJUSTE","VENTA_CON_DESCUENTO","VENTA_NETA","MONTO_DESCUENTO_ITEM","MONTO_REDONDEO","MONTO_DONACION","CODIGO_CAJERO","TIPO_CAMBIO","CODIGO_TIPO_COMPROBANTE","RUC_FACTURA","SERIE_TRANSACCION","CODIGO_RAZON_DEVOLUCION","CODIGO_SUPERVISOR","FLAG_SUPERGARANTIA","FID_TIPOREG","NUMERO_DOCUMENTO_CLIENTE","NUMERO_TARJETA_CLIENTE","MONTO_IGV","MONTO_ISC","MONTO_IGV_GASTO","MONTO_COSTO_TOTAL","FLAG_CENTRO_NEGOCIO","NUMERO_CENTRO_NEGOCIO_O","NUMERO_TICKET","NUMERO_BOLETA_ELECTRONICA","TIEMPO_ENTRE_TRANSACCION","CODIGO_CAR","NUMERO_DOCUMENTO_CAR","FLAG_DESCUENTO_COLABORADOR","FLAG_DESPACHO","FECHA_PROCESO_ORI","CODIGO_LOCAL_ORI","NUMERO_TERMINAL_ORI","NUMERO_TRANSACCION_ORI","NUMERO_DOCUMENTO_CLIENTE_LOY","NUMERO_CENTRO_NEGOCIO","HED_SUBTIPO","NUMERO_TARJETA","NUM_DOCUM_IDE","HED_TIMBMPAGO","HED_TIMBPROD","CODIGO_RECARGA","NUMERO_RECARGA","NUMERO_DOCUMENTO_CLIENTE_LOY_B","AGORA_SHOP","HED_TDOCCLTE","HED_NOMCLTE","HED_APECLTE","PEDIDO_ECOMMERCE"
])
    if df_cab.empty!=True:
        for col in df_cab.columns:
            df_cab[col] = df_cab[col].fillna(valor_reemplazo(df_cab[col]))
        df_cab["FECHA_PROCESO"]=df_cab["FECHA_PROCESO"].map(str)
        df_cab["CODIGO_LOCAL"]=df_cab["CODIGO_LOCAL"].map(str)
        df_cab["NUMERO_TERMINAL"]=df_cab["NUMERO_TERMINAL"].map(str)
        df_cab["NUMERO_TRANSACCION"]=df_cab["NUMERO_TRANSACCION"].map(str)
        df_cab["CODIGO_VENDEDOR"]=df_cab["CODIGO_VENDEDOR"].map(str)
        df_cab["FECHA_COMPRA"]=df_cab["FECHA_COMPRA"].map(str)
        df_cab["CODIGO_TIPO_TRANSACCION"]=df_cab["CODIGO_TIPO_TRANSACCION"].map(str)
        df_cab["CODIGO_TIPO_DOCUMENTO"]=df_cab["CODIGO_TIPO_DOCUMENTO"].map(str)
        df_cab["HORA_TRANSACCION"]=df_cab["HORA_TRANSACCION"].map(str)
        df_cab["ESTADO_TRANSACCION"]=df_cab["ESTADO_TRANSACCION"].map(str)
        df_cab["VENTA_BRUTA"]=df_cab["VENTA_BRUTA"].map(str)
        df_cab["MONTO_AJUSTE"]=df_cab["MONTO_AJUSTE"].map(str)
        df_cab["VENTA_CON_DESCUENTO"]=df_cab["VENTA_CON_DESCUENTO"].map(str)
        df_cab["VENTA_NETA"]=df_cab["VENTA_NETA"].map(str)
        df_cab["MONTO_DESCUENTO_ITEM"]=df_cab["MONTO_DESCUENTO_ITEM"].map(str)
        df_cab["MONTO_REDONDEO"]=df_cab["MONTO_REDONDEO"].map(str)
        df_cab["MONTO_DONACION"]=df_cab["MONTO_DONACION"].map(str)
        df_cab["CODIGO_CAJERO"]=df_cab["CODIGO_CAJERO"].map(str)
        df_cab["TIPO_CAMBIO"]=df_cab["TIPO_CAMBIO"].map(str)
        df_cab["CODIGO_TIPO_COMPROBANTE"]=df_cab["CODIGO_TIPO_COMPROBANTE"].map(str)
        df_cab["RUC_FACTURA"]=df_cab["RUC_FACTURA"].map(str)
        df_cab["SERIE_TRANSACCION"]=df_cab["SERIE_TRANSACCION"].map(str)
        df_cab["CODIGO_RAZON_DEVOLUCION"]=df_cab["CODIGO_RAZON_DEVOLUCION"].map(str)
        df_cab["CODIGO_SUPERVISOR"]=df_cab["CODIGO_SUPERVISOR"].map(str)
        df_cab["FLAG_SUPERGARANTIA"]=df_cab["FLAG_SUPERGARANTIA"].map(str)
        df_cab["FID_TIPOREG"]=df_cab["FID_TIPOREG"].map(str)
        df_cab["NUMERO_DOCUMENTO_CLIENTE"]=df_cab["NUMERO_DOCUMENTO_CLIENTE"].map(str)
        df_cab["NUMERO_TARJETA_CLIENTE"]=df_cab["NUMERO_TARJETA_CLIENTE"].map(str)
        df_cab["MONTO_IGV"]=df_cab["MONTO_IGV"].map(str)
        df_cab["MONTO_ISC"]=df_cab["MONTO_ISC"].map(str)
        df_cab["MONTO_IGV_GASTO"]=df_cab["MONTO_IGV_GASTO"].map(str)
        df_cab["MONTO_COSTO_TOTAL"]=df_cab["MONTO_COSTO_TOTAL"].map(str)
        df_cab["FLAG_CENTRO_NEGOCIO"]=df_cab["FLAG_CENTRO_NEGOCIO"].map(str)
        df_cab["NUMERO_CENTRO_NEGOCIO_O"]=df_cab["NUMERO_CENTRO_NEGOCIO_O"].map(str)
        df_cab["NUMERO_TICKET"]=df_cab["NUMERO_TICKET"].map(str)
        df_cab["NUMERO_BOLETA_ELECTRONICA"]=df_cab["NUMERO_BOLETA_ELECTRONICA"].map(str)
        df_cab["TIEMPO_ENTRE_TRANSACCION"]=df_cab["TIEMPO_ENTRE_TRANSACCION"].map(str)
        df_cab["CODIGO_CAR"]=df_cab["CODIGO_CAR"].map(str)
        df_cab["NUMERO_DOCUMENTO_CAR"]=df_cab["NUMERO_DOCUMENTO_CAR"].map(str)
        df_cab["FLAG_DESCUENTO_COLABORADOR"]=df_cab["FLAG_DESCUENTO_COLABORADOR"].map(str)
        df_cab["FLAG_DESPACHO"]=df_cab["FLAG_DESPACHO"].map(str)
        df_cab["FECHA_PROCESO_ORI"]=df_cab["FECHA_PROCESO_ORI"].map(str)
        df_cab["CODIGO_LOCAL_ORI"]=df_cab["CODIGO_LOCAL_ORI"].map(str)
        df_cab["NUMERO_TERMINAL_ORI"]=df_cab["NUMERO_TERMINAL_ORI"].map(str)
        df_cab["NUMERO_TRANSACCION_ORI"]=df_cab["NUMERO_TRANSACCION_ORI"].map(str)
        df_cab["NUMERO_DOCUMENTO_CLIENTE_LOY"]=df_cab["NUMERO_DOCUMENTO_CLIENTE_LOY"].map(str)
        df_cab["NUMERO_CENTRO_NEGOCIO"]=df_cab["NUMERO_CENTRO_NEGOCIO"].map(str)
        df_cab["HED_SUBTIPO"]=df_cab["HED_SUBTIPO"].map(str)
        df_cab["NUMERO_TARJETA"]=df_cab["NUMERO_TARJETA"].map(str)
        df_cab["NUM_DOCUM_IDE"]=df_cab["NUM_DOCUM_IDE"].map(str)
        df_cab["HED_TIMBMPAGO"]=df_cab["HED_TIMBMPAGO"].map(str)
        df_cab["HED_TIMBPROD"]=df_cab["HED_TIMBPROD"].map(str)
        df_cab["CODIGO_RECARGA"]=df_cab["CODIGO_RECARGA"].map(str)
        df_cab["NUMERO_RECARGA"]=df_cab["NUMERO_RECARGA"].map(str)
        df_cab["NUMERO_DOCUMENTO_CLIENTE_LOY_B"]=df_cab["NUMERO_DOCUMENTO_CLIENTE_LOY_B"].map(str)
        df_cab["AGORA_SHOP"]=df_cab["AGORA_SHOP"].map(str)
        df_cab["HED_TDOCCLTE"]=df_cab["HED_TDOCCLTE"].map(str)
        df_cab["HED_NOMCLTE"]=df_cab["HED_NOMCLTE"].map(str)
        df_cab["HED_APECLTE"]=df_cab["HED_APECLTE"].map(str)
        df_cab["PEDIDO_ECOMMERCE"]=df_cab["PEDIDO_ECOMMERCE"].map(str)
        q_c.put(df_cab)
    else:
        return True

def procesar_det_df(fecha_i,fecha_f,cod_local,obj_ora_s,q_d):
    query_trx_det=r_q_detalle(fecha_i,fecha_f,cod_local)
    tp_trx_det=obj_ora_s.ejecutar_query(query_trx_det)
    df_det=pd.DataFrame(data=tp_trx_det,columns=["FECHA_PROCESO","CODIGO_LOCAL","NUMERO_TERMINAL","NUMERO_TRANSACCION","CODIGO_VENDEDOR","NUMERO_CORRELATIVO","CODIGO_PRODUCTO","CODIGO_EAN","VENTA_UNIDAD","VENTA_BRUTA","VENTA_CON_DESCUENTO","MONTO_DSCTO_PRODUCTO","MONTO_DSCTO_PROPORCIONAL","MONTO_IGV","MONTO_ISC","VENTA_NETA","CODIGO_SUPERVISOR_C_PRECIO","CODIGO_MOTIVO","FLAG_AFECTO","VALOR_IGV","MONTO_IGV_DSCTO_GASTO","MONTO_IGV_DSCTO","MONTO_PRECIO_REG","MONTO_PRECIO_VIG","MONTO_COSTO_PROMEDIO","VALOR_PRECIO_CAMBIO","MONTO_DSCTO_PRD_CAMBIOPRECIO","MONTO_PRECIO_UNITARIO","MONTO_AJUSTE_TRX","MONTO_DSCTO_MARGEN_TICKET","MONTO_DSCTO_MARGEN_PRODUCTO","MONTO_DSCTO_GASTO_FINANCIERO","MONTO_DSCTO_GASTO_MARKETING","GV_MARGEN_TICKET","IGV_MARGEN_PRODUCTO","IGV__GASTO_FINANCIERO","IGV_GASTO_MARKETING","FLAG_DESPACHO","NOM_CLIENTE_DD","NUM_NV","NUM_NV_ORI","FLAG_INGRESO"
])
    if df_det.empty!=True:
        for col in df_det.columns:
            df_det[col] = df_det[col].fillna(valor_reemplazo(df_det[col]))
        df_det["FECHA_PROCESO"]=df_det["FECHA_PROCESO"].map(str)
        df_det["CODIGO_LOCAL"]=df_det["CODIGO_LOCAL"].map(str)
        df_det["NUMERO_TERMINAL"]=df_det["NUMERO_TERMINAL"].map(str)
        df_det["NUMERO_TRANSACCION"]=df_det["NUMERO_TRANSACCION"].map(str)
        df_det["CODIGO_VENDEDOR"]=df_det["CODIGO_VENDEDOR"].map(str)
        df_det["NUMERO_CORRELATIVO"]=df_det["NUMERO_CORRELATIVO"].map(str)
        df_det["CODIGO_PRODUCTO"]=df_det["CODIGO_PRODUCTO"].map(str)
        df_det["CODIGO_EAN"]=df_det["CODIGO_EAN"].map(str)
        df_det["VENTA_UNIDAD"]=df_det["VENTA_UNIDAD"].map(str)
        df_det["VENTA_BRUTA"]=df_det["VENTA_BRUTA"].map(str)
        df_det["VENTA_CON_DESCUENTO"]=df_det["VENTA_CON_DESCUENTO"].map(str)
        df_det["MONTO_DSCTO_PRODUCTO"]=df_det["MONTO_DSCTO_PRODUCTO"].map(str)
        df_det["MONTO_DSCTO_PROPORCIONAL"]=df_det["MONTO_DSCTO_PROPORCIONAL"].map(str)
        df_det["MONTO_IGV"]=df_det["MONTO_IGV"].map(str)
        df_det["MONTO_ISC"]=df_det["MONTO_ISC"].map(str)
        df_det["VENTA_NETA"]=df_det["VENTA_NETA"].map(str)
        df_det["CODIGO_SUPERVISOR_C_PRECIO"]=df_det["CODIGO_SUPERVISOR_C_PRECIO"].map(str)
        df_det["CODIGO_MOTIVO"]=df_det["CODIGO_MOTIVO"].map(str)
        df_det["FLAG_AFECTO"]=df_det["FLAG_AFECTO"].map(str)
        df_det["VALOR_IGV"]=df_det["VALOR_IGV"].map(str)
        df_det["MONTO_IGV_DSCTO_GASTO"]=df_det["MONTO_IGV_DSCTO_GASTO"].map(str)
        df_det["MONTO_IGV_DSCTO"]=df_det["MONTO_IGV_DSCTO"].map(str)
        df_det["MONTO_PRECIO_REG"]=df_det["MONTO_PRECIO_REG"].map(str)
        df_det["MONTO_PRECIO_VIG"]=df_det["MONTO_PRECIO_VIG"].map(str)
        df_det["MONTO_COSTO_PROMEDIO"]=df_det["MONTO_COSTO_PROMEDIO"].map(str)
        df_det["VALOR_PRECIO_CAMBIO"]=df_det["VALOR_PRECIO_CAMBIO"].map(str)
        df_det["MONTO_DSCTO_PRD_CAMBIOPRECIO"]=df_det["MONTO_DSCTO_PRD_CAMBIOPRECIO"].map(str)
        df_det["MONTO_PRECIO_UNITARIO"]=df_det["MONTO_PRECIO_UNITARIO"].map(str)
        df_det["MONTO_AJUSTE_TRX"]=df_det["MONTO_AJUSTE_TRX"].map(str)
        df_det["MONTO_DSCTO_MARGEN_TICKET"]=df_det["MONTO_DSCTO_MARGEN_TICKET"].map(str)
        df_det["MONTO_DSCTO_MARGEN_PRODUCTO"]=df_det["MONTO_DSCTO_MARGEN_PRODUCTO"].map(str)
        df_det["MONTO_DSCTO_GASTO_FINANCIERO"]=df_det["MONTO_DSCTO_GASTO_FINANCIERO"].map(str)
        df_det["MONTO_DSCTO_GASTO_MARKETING"]=df_det["MONTO_DSCTO_GASTO_MARKETING"].map(str)
        df_det["GV_MARGEN_TICKET"]=df_det["GV_MARGEN_TICKET"].map(str)
        df_det["IGV_MARGEN_PRODUCTO"]=df_det["IGV_MARGEN_PRODUCTO"].map(str)
        df_det["IGV__GASTO_FINANCIERO"]=df_det["IGV__GASTO_FINANCIERO"].map(str)
        df_det["IGV_GASTO_MARKETING"]=df_det["IGV_GASTO_MARKETING"].map(str)
        df_det["FLAG_DESPACHO"]=df_det["FLAG_DESPACHO"].map(str)
        df_det["NOM_CLIENTE_DD"]=df_det["NOM_CLIENTE_DD"].map(str)
        df_det["NUM_NV"]=df_det["NUM_NV"].map(str)
        df_det["NUM_NV_ORI"]=df_det["NUM_NV_ORI"].map(str)
        df_det["FLAG_INGRESO"]=df_det["FLAG_INGRESO"].map(str)

        q_d.put(df_det)
    else:
        return True

def procesar_fpexterno_df(fecha_i,fecha_f,cod_local,obj_ora_s,q_fpe):
    query_trx_fpe=r_q_fp_externo(fecha_i,fecha_f,cod_local)
    tp_trx_fpe=obj_ora_s.ejecutar_query(query_trx_fpe)
    df_fpe=pd.DataFrame(data=tp_trx_fpe,columns=["FECHA_PROCESO","CODIGO_LOCAL","NUMERO_TERMINAL","NUMERO_TRANSACCION","CORRELATIVO","CODIGO_PAGO","CODIGO_TIPO_PAGO","NUMERO_CUENTA","TIPO_CUOTA","NUMERO_CUOTA","IMPORTE_PAGO_BRUTO","NUMERO_RUC","FECHA_EXPIRACION","CODIGO_AUTORIZACION","NUMERO_CUPON","CODIGO_MONEDA","MARCA_BANDA","MARCA_BATCH","CODIGO_EN_LINEA","CODIGO_TIPO_DOCUMENTO","CODIGO_CARGO","CODIGO_REFERENCIA","ID_VISA","NUMERO_DOCUMENTO","NUMERO_VOUCHER","MONTO_RECARGO","CODIGO_SUPERVISOR","TIPO_CREDITO","LINEA"
])  
    
    if df_fpe.empty!=True:
        for col in df_fpe.columns:
            df_fpe[col] = df_fpe[col].fillna(valor_reemplazo(df_fpe[col]))
        df_fpe["FECHA_PROCESO"]=df_fpe["FECHA_PROCESO"].map(str)
        df_fpe["CODIGO_LOCAL"]=df_fpe["CODIGO_LOCAL"].map(str)
        df_fpe["NUMERO_TERMINAL"]=df_fpe["NUMERO_TERMINAL"].map(str)
        df_fpe["NUMERO_TRANSACCION"]=df_fpe["NUMERO_TRANSACCION"].map(str)
        df_fpe["CORRELATIVO"]=df_fpe["CORRELATIVO"].map(str)
        df_fpe["CODIGO_PAGO"]=df_fpe["CODIGO_PAGO"].map(str)
        df_fpe["CODIGO_TIPO_PAGO"]=df_fpe["CODIGO_TIPO_PAGO"].map(str)
        df_fpe["NUMERO_CUENTA"]=df_fpe["NUMERO_CUENTA"].map(str)
        df_fpe["TIPO_CUOTA"]=df_fpe["TIPO_CUOTA"].map(str)
        df_fpe["NUMERO_CUOTA"]=df_fpe["NUMERO_CUOTA"].map(str)
        df_fpe["IMPORTE_PAGO_BRUTO"]=df_fpe["IMPORTE_PAGO_BRUTO"].map(str)
        df_fpe["NUMERO_RUC"]=df_fpe["NUMERO_RUC"].map(str)
        df_fpe["FECHA_EXPIRACION"]=df_fpe["FECHA_EXPIRACION"].map(str)
        df_fpe["CODIGO_AUTORIZACION"]=df_fpe["CODIGO_AUTORIZACION"].map(str)
        df_fpe["NUMERO_CUPON"]=df_fpe["NUMERO_CUPON"].map(str)
        df_fpe["CODIGO_MONEDA"]=df_fpe["CODIGO_MONEDA"].map(str)
        df_fpe["MARCA_BANDA"]=df_fpe["MARCA_BANDA"].map(str)
        df_fpe["MARCA_BATCH"]=df_fpe["MARCA_BATCH"].map(str)
        df_fpe["CODIGO_EN_LINEA"]=df_fpe["CODIGO_EN_LINEA"].map(str)
        df_fpe["CODIGO_TIPO_DOCUMENTO"]=df_fpe["CODIGO_TIPO_DOCUMENTO"].map(str)
        df_fpe["CODIGO_CARGO"]=df_fpe["CODIGO_CARGO"].map(str)
        df_fpe["CODIGO_REFERENCIA"]=df_fpe["CODIGO_REFERENCIA"].map(str)
        df_fpe["ID_VISA"]=df_fpe["ID_VISA"].map(str)
        df_fpe["NUMERO_DOCUMENTO"]=df_fpe["NUMERO_DOCUMENTO"].map(str)
        df_fpe["NUMERO_VOUCHER"]=df_fpe["NUMERO_VOUCHER"].map(str)
        df_fpe["MONTO_RECARGO"]=df_fpe["MONTO_RECARGO"].map(str)
        df_fpe["CODIGO_SUPERVISOR"]=df_fpe["CODIGO_SUPERVISOR"].map(str)
        df_fpe["TIPO_CREDITO"]=df_fpe["TIPO_CREDITO"].map(str)
        df_fpe["LINEA"]=df_fpe["LINEA"].map(str)
        q_fpe.put(df_fpe)
    else:
        return True
    
def procesar_promocion_df(fecha_i,fecha_f,cod_local,obj_ora_s,q_promo):
    query_trx_promocion=r_q_promocion(fecha_i,fecha_f,cod_local)
    tp_trx_promo=obj_ora_s.ejecutar_query(query_trx_promocion)
    df_promo=pd.DataFrame(data=tp_trx_promo,columns=["FECHA_PROCESO","CODIGO_LOCAL","NUMERO_TERMINAL","NUMERO_TANSACCION","CORRELATIVO","CODIGO_PROMOCION","MONTO_DSCTO_MARGEN_TICKET","MONTO_DSCTO_MARGEN_PRODUCTO","MONTO_DSCTO_GASTO_FINANCIERO","MONTO_DSCTO_GASTO_MARKETING","IGV_DSCTO_MT","IGV_DSCTO_MP","IGV_DSCTO_GF","IGV_DSCTO_GM","CANTIDAD_DESCUENTO","MONTO_DESCUENTO","MONTO_DESCUENTO_GASTO","FLAG_PV_PL","TIPO_PV_PL","CANTIDAD_PESO","NUMERO_CUPON","CODIGO_PRODUCTO","MONTO_DSCTO_GASTO_AGORA","LINEA"
])  
    
    if df_promo.empty!=True:
        for col in df_promo.columns:
            df_promo[col] = df_promo[col].fillna(valor_reemplazo(df_promo[col]))
            df_promo["FECHA_PROCESO"]=df_promo["FECHA_PROCESO"].map(str)
            df_promo["CODIGO_LOCAL"]=df_promo["CODIGO_LOCAL"].map(str)
            df_promo["NUMERO_TERMINAL"]=df_promo["NUMERO_TERMINAL"].map(str)
            df_promo["NUMERO_TANSACCION"]=df_promo["NUMERO_TANSACCION"].map(str)
            df_promo["CORRELATIVO"]=df_promo["CORRELATIVO"].map(str)
            df_promo["CODIGO_PROMOCION"]=df_promo["CODIGO_PROMOCION"].map(str)
            df_promo["MONTO_DSCTO_MARGEN_TICKET"]=df_promo["MONTO_DSCTO_MARGEN_TICKET"].map(str)
            df_promo["MONTO_DSCTO_MARGEN_PRODUCTO"]=df_promo["MONTO_DSCTO_MARGEN_PRODUCTO"].map(str)
            df_promo["MONTO_DSCTO_GASTO_FINANCIERO"]=df_promo["MONTO_DSCTO_GASTO_FINANCIERO"].map(str)
            df_promo["MONTO_DSCTO_GASTO_MARKETING"]=df_promo["MONTO_DSCTO_GASTO_MARKETING"].map(str)
            df_promo["IGV_DSCTO_MT"]=df_promo["IGV_DSCTO_MT"].map(str)
            df_promo["IGV_DSCTO_MP"]=df_promo["IGV_DSCTO_MP"].map(str)
            df_promo["IGV_DSCTO_GF"]=df_promo["IGV_DSCTO_GF"].map(str)
            df_promo["IGV_DSCTO_GM"]=df_promo["IGV_DSCTO_GM"].map(str)
            df_promo["CANTIDAD_DESCUENTO"]=df_promo["CANTIDAD_DESCUENTO"].map(str)
            df_promo["MONTO_DESCUENTO"]=df_promo["MONTO_DESCUENTO"].map(str)
            df_promo["MONTO_DESCUENTO_GASTO"]=df_promo["MONTO_DESCUENTO_GASTO"].map(str)
            df_promo["FLAG_PV_PL"]=df_promo["FLAG_PV_PL"].map(str)
            df_promo["TIPO_PV_PL"]=df_promo["TIPO_PV_PL"].map(str)
            df_promo["CANTIDAD_PESO"]=df_promo["CANTIDAD_PESO"].map(str)
            df_promo["NUMERO_CUPON"]=df_promo["NUMERO_CUPON"].map(str)
            df_promo["CODIGO_PRODUCTO"]=df_promo["CODIGO_PRODUCTO"].map(str)
            df_promo["MONTO_DSCTO_GASTO_AGORA"]=df_promo["MONTO_DSCTO_GASTO_AGORA"].map(str)
            df_promo["LINEA"]=df_promo["LINEA"].map(str)
            q_promo.put(df_promo)
    else:
        return True
    


def procesar_cab_bq(obj_bq,project,dataset,table_cab,df_cab):
    obj_bq.ins_table(project,dataset,table_cab,df_cab)
    print(f"Inserción exitosa en {project}.{dataset}.{table_cab} - Big Query")

def procesar_det_bq(obj_bq,project,dataset,table_det,df_det):
    obj_bq.ins_table(project,dataset,table_det,df_det)
    print(f"Inserción exitosa en {project}.{dataset}.{table_det} - Big Query")

def procesar_fpe_bq(obj_bq,project,dataset,table_fpe,df_fpe):
    obj_bq.ins_table(project,dataset,table_fpe,df_fpe)
    print(f"Inserción exitosa en {project}.{dataset}.{table_fpe} - Big Query")

def procesar_pd_bq(obj_bq,project,dataset,table_pd,df_pd):
    obj_bq.ins_table(project,dataset,table_pd,df_pd)
    print(f"Inserción exitosa en {project}.{dataset}.{table_pd} - Big Query")

def enviar_correo (mensaje):
  try:
    remitente = "over.salazar@spsa.pe"
    destinatario = ['over.salazar@spsa.pe']
    
    mensaje = f"""Estimados por favor revisar el proceso de carga Venta en Linea BI, está cayendo con el siguiente error:<br><h2 style="color:red">{mensaje}</h2>Saludos<br>Over Salazar"""

    email = EmailMessage()
    email["From"] = remitente
    email["To"] = destinatario
    email["Subject"] = "Alerta Venta en Linea BI"
    email.set_content(mensaje, subtype="html")

    smtp = SMTP(host="smtp.office365.com",port="587")
    smtp.starttls()
    smtp.login(remitente, "xbsjcjhsqwjfxvbm")
    smtp.sendmail(remitente, destinatario, email.as_string())
    smtp.quit()
    smtp.close()
  except Exception as e:
    print(e)
    raise Exception("Error en el envío de correo")

def valor_reemplazo(col):
    if col.dtype == 'float64' or col.dtype == 'int64':
        return 0  
    elif col.dtype == 'object':
        return ''  
    else:
        return None  

def procesar_local(fecha_actual_f_i,fecha_actual_f_f,cod_local):
    try:
        obj_ora_s=ora.oracle(usuario=usuario,contraseña=contraseña,dsn=ip_server_name)
        obj_ora_s.inicializar_oracle()
        obj_ora_s.conectar_oracle()
        query_trx_carga=r_q_trx_carga(fecha_actual_f_i,fecha_actual_f_f,cod_local)
        tp_trx=obj_ora_s.ejecutar_query(query_trx_carga)
        if tp_trx:
            obj_ora_s.query_insert_tupla("insert into EXCT2SP.ICT2_TRXS_VTA_BI_GROUP_DIARIO(hed_pais,hed_origentrx,hed_local,hed_pos,hed_numtrx,hed_fechatrx,hed_horatrx,hed_fcontable,hed_tipotrx,hed_tipodoc,hed_subtipo,fec_registro,hed_anulado) values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13)",tp_trx)
            obj_ora_s.commit()
            print(f"Inserción exitosa en ICT2_TRXS_VTA_BI_GROUP_DIARIO con fechas {fecha_actual_f_i} {fecha_actual_f_f}  y local {cod_local}")
            
            q_c=queue.Queue()
            q_d=queue.Queue()
            q_fpe=queue.Queue()
            q_promo=queue.Queue()
           
            #procesar_fpexterno_df(fecha_actual_f_i,fecha_actual_f_f,cod_local,obj_ora_s,q_fpe)
            #procesar_cab_df(fecha_actual_f_i,fecha_actual_f_f,cod_local,obj_ora_s,q_c)
            #procesar_det_df(fecha_actual_f_i,fecha_actual_f_f,cod_local,obj_ora_s,q_d)

            threads_l = []

            thread_c = ThreadWithException(target=procesar_cab_df, args=(fecha_actual_f_i,fecha_actual_f_f,cod_local,obj_ora_s,q_c))
            thread_d = ThreadWithException(target=procesar_det_df, args=(fecha_actual_f_i,fecha_actual_f_f,cod_local,obj_ora_s,q_d))
            thread_fpe = ThreadWithException(target=procesar_fpexterno_df, args=(fecha_actual_f_i,fecha_actual_f_f,cod_local,obj_ora_s,q_fpe))
            thread_promo = ThreadWithException(target=procesar_promocion_df, args=(fecha_actual_f_i,fecha_actual_f_f,cod_local,obj_ora_s,q_promo))

            thread_c.start()
            thread_d.start()
            thread_fpe.start()
            thread_promo.start()

            threads_l.append(thread_c)
            threads_l.append(thread_d)
            threads_l.append(thread_fpe)
            threads_l.append(thread_promo)

            for t in threads_l:
                t.join()
        
            for t in threads_l:
                if t.exception:
                    raise ValueError(t.exception)

            df_cab=q_c.get()
            df_det=q_d.get()
            
            df_fpe=pd.DataFrame()
            df_pro=pd.DataFrame()

            if not q_fpe.empty():
                df_fpe=q_fpe.get()
            
            if not q_promo.empty():
                df_pro=q_promo.get()

            obj_ora_s.cerrar()
            return 1,df_cab,df_det,df_fpe,df_pro
        else:
            print(f"No hay data para cargar en ict2_trxs_vta_bi_group con fechas {fecha_actual_f_i} {fecha_actual_f_f}  y local {cod_local}")
            return 0,False,False,False,False
    except Exception as e:
        try:
            err=f"{e} para el local {cod_local} con fechas {fecha_actual_f_i} {fecha_actual_f_f}"
            print(err)
            enviar_correo (err)
            print(f"Error al procesar la data de los dias {fecha_actual_f_i}  {fecha_actual_f_f} y local {cod_local}")
            fecha_actual_f_s_i=datetime.datetime.strptime(datetime.datetime.strftime(datetime.datetime.strptime(fecha_actual_f_i,"%Y%m%d"),"%d/%m/%Y"),"%d/%m/%Y")
            fecha_actual_f_s_f=datetime.datetime.strptime(datetime.datetime.strftime(datetime.datetime.strptime(fecha_actual_f_f,"%Y%m%d"),"%d/%m/%Y"),"%d/%m/%Y")
            ls_v_fl_error=[]
            ls_a_fl_error=[]
            ls_v_fl_error.append(fecha_actual_f_s_i)
            ls_v_fl_error.append(cod_local)
            ls_v_fl_error.append(0)
            ls_a_fl_error.append(ls_v_fl_error)
            ls_v_fl_error=[]
            ls_v_fl_error.append(fecha_actual_f_s_f)
            ls_v_fl_error.append(cod_local)
            ls_v_fl_error.append(0)
            ls_a_fl_error.append(ls_v_fl_error)
            print(ls_a_fl_error)
            obj_ora_s.query_insert_tupla("insert into EXCT2SP.ICT2_VAL_CARGA_VTA_BI_DIARIO(hed_fcontable,hed_local,est_carga) values (:1,:2,:3)",ls_a_fl_error)
            obj_ora_s.commit()
            return 9999,False,False
        except Exception as e:
            return 9999,False,False
try:
    obj_bq=bq.bigq(json=file_json)
    project="sistemas-bi"
    dataset="BI_Core_Stage"
    table_cab="tmp_ticket_cabecera"
    table_det="tmp_ticket_detalle"
    table_fpe="tmp_forma_pago_externo"
    table_promo="tmp_promocion_detalle"
    #obj_bq.clear_table(project,dataset,table_cab)
    #obj_bq.clear_table(project,dataset,table_det)
    #obj_bq.clear_table(project,dataset,table_fpe)
    #obj_bq.clear_table(project,dataset,table_promo)
    #print(f"Se Truncarón las tablas {table_cab} y {table_det} temporales")

    obj_ora_p=ora.oracle(usuario=usuario,contraseña=contraseña,dsn=ip_server_name)
    obj_ora_p.inicializar_oracle()
    obj_ora_p.conectar_oracle()
    obj_ora_p.ejecutar_query_sin_return("truncate table EXCT2SP.ICT2_VAL_CARGA_VTA_BI_DIARIO")
    
    df_cab_acum=pd.DataFrame(columns=["FECHA_PROCESO","CODIGO_LOCAL","NUMERO_TERMINAL","NUMERO_TRANSACCION","CODIGO_VENDEDOR","FECHA_COMPRA","CODIGO_TIPO_TRANSACCION","CODIGO_TIPO_DOCUMENTO","HORA_TRANSACCION","ESTADO_TRANSACCION","VENTA_BRUTA","MONTO_AJUSTE","VENTA_CON_DESCUENTO","VENTA_NETA","MONTO_DESCUENTO_ITEM","MONTO_REDONDEO","MONTO_DONACION","CODIGO_CAJERO","TIPO_CAMBIO","CODIGO_TIPO_COMPROBANTE","RUC_FACTURA","SERIE_TRANSACCION","CODIGO_RAZON_DEVOLUCION","CODIGO_SUPERVISOR","FLAG_SUPERGARANTIA","FID_TIPOREG","NUMERO_DOCUMENTO_CLIENTE","NUMERO_TARJETA_CLIENTE","MONTO_IGV","MONTO_ISC","MONTO_IGV_GASTO","MONTO_COSTO_TOTAL","FLAG_CENTRO_NEGOCIO","NUMERO_CENTRO_NEGOCIO_O","NUMERO_TICKET","NUMERO_BOLETA_ELECTRONICA","TIEMPO_ENTRE_TRANSACCION","CODIGO_CAR","NUMERO_DOCUMENTO_CAR","FLAG_DESCUENTO_COLABORADOR","FLAG_DESPACHO","FECHA_PROCESO_ORI","CODIGO_LOCAL_ORI","NUMERO_TERMINAL_ORI","NUMERO_TRANSACCION_ORI","NUMERO_DOCUMENTO_CLIENTE_LOY","NUMERO_CENTRO_NEGOCIO","HED_SUBTIPO","NUMERO_TARJETA","NUM_DOCUM_IDE","HED_TIMBMPAGO","HED_TIMBPROD","CODIGO_RECARGA","NUMERO_RECARGA","NUMERO_DOCUMENTO_CLIENTE_LOY_B","AGORA_SHOP","HED_TDOCCLTE","HED_NOMCLTE","HED_APECLTE","PEDIDO_ECOMMERCE"
    ])
    df_det_acum=pd.DataFrame(columns=[
        "FECHA_PROCESO","CODIGO_LOCAL","NUMERO_TERMINAL","NUMERO_TRANSACCION","CODIGO_VENDEDOR","NUMERO_CORRELATIVO","CODIGO_PRODUCTO","CODIGO_EAN","VENTA_UNIDAD","VENTA_BRUTA","VENTA_CON_DESCUENTO","MONTO_DSCTO_PRODUCTO","MONTO_DSCTO_PROPORCIONAL","MONTO_IGV","MONTO_ISC","VENTA_NETA","CODIGO_SUPERVISOR_C_PRECIO","CODIGO_MOTIVO","FLAG_AFECTO","VALOR_IGV","MONTO_IGV_DSCTO_GASTO","MONTO_IGV_DSCTO","MONTO_PRECIO_REG","MONTO_PRECIO_VIG","MONTO_COSTO_PROMEDIO","VALOR_PRECIO_CAMBIO","MONTO_DSCTO_PRD_CAMBIOPRECIO","MONTO_PRECIO_UNITARIO","MONTO_AJUSTE_TRX","MONTO_DSCTO_MARGEN_TICKET","MONTO_DSCTO_MARGEN_PRODUCTO","MONTO_DSCTO_GASTO_FINANCIERO","MONTO_DSCTO_GASTO_MARKETING","GV_MARGEN_TICKET","IGV_MARGEN_PRODUCTO","IGV__GASTO_FINANCIERO","IGV_GASTO_MARKETING","FLAG_DESPACHO","NOM_CLIENTE_DD","NUM_NV","NUM_NV_ORI","FLAG_INGRESO"
    ])
    df_fpe_acum=pd.DataFrame(columns=[
        "FECHA_PROCESO","CODIGO_LOCAL","NUMERO_TERMINAL","NUMERO_TRANSACCION","CORRELATIVO","CODIGO_PAGO","CODIGO_TIPO_PAGO","NUMERO_CUENTA","TIPO_CUOTA","NUMERO_CUOTA","IMPORTE_PAGO_BRUTO","NUMERO_RUC","FECHA_EXPIRACION","CODIGO_AUTORIZACION","NUMERO_CUPON","CODIGO_MONEDA","MARCA_BANDA","MARCA_BATCH","CODIGO_EN_LINEA","CODIGO_TIPO_DOCUMENTO","CODIGO_CARGO","CODIGO_REFERENCIA","ID_VISA","NUMERO_DOCUMENTO","NUMERO_VOUCHER","MONTO_RECARGO","CODIGO_SUPERVISOR","TIPO_CREDITO","LINEA"
      ])
    df_promo_acum=pd.DataFrame(columns=[
        "FECHA_PROCESO","CODIGO_LOCAL","NUMERO_TERMINAL","NUMERO_TANSACCION","CORRELATIVO","CODIGO_PROMOCION","MONTO_DSCTO_MARGEN_TICKET","MONTO_DSCTO_MARGEN_PRODUCTO","MONTO_DSCTO_GASTO_FINANCIERO","MONTO_DSCTO_GASTO_MARKETING","IGV_DSCTO_MT","IGV_DSCTO_MP","IGV_DSCTO_GF","IGV_DSCTO_GM","CANTIDAD_DESCUENTO","MONTO_DESCUENTO","MONTO_DESCUENTO_GASTO","FLAG_PV_PL","TIPO_PV_PL","CANTIDAD_PESO","NUMERO_CUPON","CODIGO_PRODUCTO","MONTO_DSCTO_GASTO_AGORA","LINEA"
])
    ls_a_fl_error=[]    
    ls_loc=listar_locales(obj_ora_p,q_local)
    ls_loc_t=[]

    for i in ls_loc:
        cod_local=i[0]
        ls_loc_t.append(cod_local)

    with concurrent.futures.ThreadPoolExecutor(max_workers=c) as executor:
        resultados = [executor.submit(procesar_local,fecha_actual_f_i,fecha_actual_f_f,i) for i in ls_loc_t]

    for resultado in concurrent.futures.as_completed(resultados):
        f,dfc,dfd,dfpe,dfp=resultado.result()
        if f==1:
            df_cab_acum=pd.concat([df_cab_acum,dfc])
            df_det_acum=pd.concat([df_det_acum,dfd])
            if dfpe.empty!=True:
                df_fpe_acum=pd.concat([df_fpe_acum,dfpe])
            if dfp.empty!=True:
                df_promo_acum=pd.concat([df_promo_acum,dfp])
            
    if df_cab_acum.empty == False and df_det_acum.empty == False:
        print("Si hay datos en los df acum cab y det para cargar a tmp big query ")

        threads_df = []
        pbqcab = ThreadWithException(target=procesar_cab_bq,args=(obj_bq,project,dataset,table_cab,df_cab_acum))
        pbqdet = ThreadWithException(target=procesar_det_bq,args=(obj_bq,project,dataset,table_det,df_det_acum))

        pbqcab.start()
        pbqdet.start()
        threads_df.append(pbqcab)
        threads_df.append(pbqdet)

        if df_fpe_acum.empty!=True:
            pbqfpe = ThreadWithException(target=procesar_fpe_bq,args=(obj_bq,project,dataset,table_fpe,df_fpe_acum))
            pbqfpe.start()
            threads_df.append(pbqfpe)
        
        if df_promo_acum.empty!=True:
            pbqpd = ThreadWithException(target=procesar_pd_bq,args=(obj_bq,project,dataset,table_promo,df_promo_acum))
            pbqpd.start()
            threads_df.append(pbqpd)

        for t in threads_df:
            t.join()
        
        for t in threads_df:
            if t.exception:
                raise ValueError(t.exception)
        
        obj_ora_p.ejecutar_query_sin_return(r_q_upd_tabla_carga(fecha_actual_f_i,fecha_actual_f_f))
        obj_ora_p.commit()

    else:
        print("No hay data en df por lo tanto no se procesa en Big Query")
    obj_ora_p.cerrar()
except Exception as e:
    enviar_correo (e)


        

  


    

