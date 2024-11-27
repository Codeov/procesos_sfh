import est_bq as bq
import est_ora as ora
import pandas as pd
import queue
import concurrent.futures
import threading

pd.set_option('future.no_silent_downcasting', True)

c=50
json_bq=r"D:\python\credenciales biq query\sistemas-bi-7a46b3894448.json"
usuario= "SINTERFACE"
contraseña= "SF5590X"
ip_server_name= "10.20.11.20/SPT01"

query_dim_local_modelo="""
    select 
distinct (a.cod_alterno)  codigo_local_pmm
from
`sistemas-bi.SPSA.dim_local_modelo` a
--where a.cod_alterno='1432'
"""
query_area_producto="""
    select
    rtrim(ltrim(a.codigo_area)) codigo_area
    from
    (
    select distinct a.codigo_area from `SPSA.dim_producto` a
    where a.codigo_area like '%A%'
    --and a.codigo_area='A09'
    order by cast(right(a.codigo_area,2) as int64)
    ) a
"""

def query_precios(cod_local,cod_area):
    query_p=f"""
    select 
    distinct
    q.ORG_LVL_NUMBER as cod_local,
    q.PRD_LVL_NUMBER as sku,
    q.afecto as afecto,
    cz.cst_cost as costo,
    q.PRC_PRICE  as precio_base, 
    p1.prc_qty_brk_qty as q1,
    p1.prc_qty_brk_tprc as p1,
    p2.prc_qty_brk_qty as c2,
    p2.prc_qty_brk_tprc as p2,
    h.valor   as precio_promo,
    h.ini_prom as  fechaini,
    h.fin_prom as fechafin,
    TO_CHAR(h.cod_prom,'99999999') cod_promo
    from 
    (
    SELECT   c.org_lvl_child,c.prd_lvl_child,c.prc_qty_brk_id,
                o.org_lvl_number,
                i.prd_lvl_number,i.afecto,
                min(c.prc_price) prc_price
            FROM chlprce2 c
            inner join IFHPRDMST I
                on c.prd_lvl_child = i.prd_lvl_child
            and i.prd_style_ind = 'F'
            
            inner join IFHORGMST o
                on c.org_lvl_child = o.org_lvl_child
            WHERE c.PRC_HDR_NUMBER =
                (SELECT MAX(PRC1.PRC_HDR_NUMBER)
                    FROM CHLPRCE2 PRC1
                    WHERE PRC1.PRD_LVL_CHILD = c.prd_lvl_child
                    AND PRC1.ORG_LVL_CHILD = c.org_lvl_child
                    and prc1.download_date is not null
                    )
            and NVL(c.PRC_PRICE, 0) > 0
            and c.prc_book_number = 1
            and c.download_date is not null  
            AND O.ORG_LVL_NUMBER={cod_local} 
            and  I.COD_AREA ='{cod_area}'
            group by c.org_lvl_child,c.prd_lvl_child,c.prc_qty_brk_id,
                o.org_lvl_number,
                i.prd_lvl_number,i.afecto
    ) q
    left join prom_dpc_vigente_hst h
        on Q.PRD_LVL_NUMBER = h.prd_lvl_number
    and q.org_lvl_number = h.org_lvl_number
    left join cstzstee z
    on z.org_lvl_child = q.org_lvl_child
    left join CSTMSTEE Cz
        on cz.PRD_LVL_CHILD = q.PRD_LVL_CHILD
    and cz.CST_ZONE_ID = z.cst_zone_id
    and cz.CST_LEVEL = 2
    and cz.CST_TYPE_ID = 22
    left join  PRCQBMEE_SIV P1 on p1.prc_qty_brk_id = q.prc_qty_brk_id and p1.id_num=1
    left join  PRCQBMEE_SIV P2 on p2.prc_qty_brk_id = q.prc_qty_brk_id and p2.id_num=2
    """
    return query_p

def query_surtido(cod_local,cod_area):
    query_s=f"""
    SELECT DISTINCT T.PRD_LVL_NUMBER sku, 
        O.ORG_LVL_NUMBER cod_local,
        EINTERFACE.JSATELITE_PKG_INTERFACES.fn_Retorna_Atrib_Prd_Tda(t.PRD_LVL_CHILD, O.ORG_LVL_CHILD,'TYPOPER','HDRCLUSTER',1) cluster_surtido,
        P.PRD_STATUS estado_cia,
        (SELECT  PRD_STATUS_DESC  FROM PRDSTSEE WHERE PRD_STATUS  = P.PRD_STATUS) descripcion_est_cia,
        T.FEC_VIGENCIA fec_est_cia,
        NVL(X5.PRD_STATUS,T.COD_EST) est_local, 
        (SELECT  PRD_STATUS_DESC  FROM PRDSTSEE WHERE PRD_STATUS = NVL(X5.PRD_STATUS,T.COD_EST)) des_est_local ,
        TO_CHAR(TO_DATE(tp_pkg_gen_funciones.FN_GET_EST_PRD_ORG(O.ORG_LVL_CHILD , T.PRD_LVL_CHILD, '1'),'YYYYMMDD'),'DD/MM/YYYY') fec_est_local ,
        TO_CHAR(X6.FEC_REGISTRO, 'DD/MM/YYYY') fec_catalog,
    NVL(    
    (select o2.org_lvl_number 
    from TRFDCPEE  t1 , 
    orgmstee o1 , 
    prdmstee p1 ,
    orgmstee o2 
    where t1.org_lvl_child=o1.org_lvl_child
    and t1.prd_lvl_child=p1.prd_lvl_child
    and t1.trf_whs_child=o2.org_lvl_child
    and p1.prd_lvl_child=p.prd_lvl_child
    and o1.org_lvl_child=o.org_lvl_child
    and t1.trf_whs_priority = 1
    ) ,
    (SELECT  ORG_LVL_NUMBER FROM ORGMSTEE WHERE ORG_LVL_NUMBER = jsatelite_pkg_interfaces.FN_GET_ABAST_TIENDA_CD(T.PRD_LVL_CHILD,O.ORG_LVL_CHILD) )

    ) cent_suministrador,
    
        NVL(
    (select o2.org_name_full
    from TRFDCPEE  t1 , 
    orgmstee o1 , 
    prdmstee p1 ,
    orgmstee o2 
    where t1.org_lvl_child=o1.org_lvl_child
    and t1.prd_lvl_child=p1.prd_lvl_child
    and t1.trf_whs_child=o2.org_lvl_child
    and p1.prd_lvl_child=p.prd_lvl_child
    and o1.org_lvl_child=o.org_lvl_child
    and t1.trf_whs_priority = 1
    ) ,
    (SELECT   ORG_NAME_FULL FROM ORGMSTEE  WHERE ORG_LVL_NUMBER = jsatelite_pkg_interfaces.FN_GET_ABAST_TIENDA_CD(T.PRD_LVL_CHILD,O.ORG_LVL_CHILD))
    ) des_centro
    FROM  FMTPDSEE  F 
        INNER JOIN PRDFXSEE X ON
        F.PRD_AFH_KEY = X.PRD_AFH_KEY
        INNER JOIN IFHPRDMST T  ON
        T.PRD_LVL_CHILD = F.PRD_LVL_CHILD
        INNER JOIN ORGMSTEE O ON
        O.ORG_LVL_CHILD = X.ORG_LVL_CHILD
        INNER JOIN PRDMSTEE P ON
        P.PRD_LVL_CHILD = T.PRD_LVL_CHILD
        INNER JOIN INVSKUEE K ON
        K.PRD_SKU_TYPE = P.PRD_SKU_TYPE
        LEFT JOIN (SELECT * FROM PRDSBLEE) X5 ON
        X5.PRD_LVL_CHILD = T.PRD_LVL_CHILD
        AND  X5.ORG_LVL_CHILD = O.ORG_LVL_CHILD
        LEFT JOIN 
        (select MAX(F6.AUDIT_DATE) FEC_REGISTRO,F6.PRD_LVL_CHILD,T6.ORG_LVL_CHILD
    FROM PRDAPDAH  F6,
        PRDAPHEE  P6,
        PRDAFDEE  PT6,
        PRDFXSEE  T6,
        IFHPRDMST I5,
        IFHORGMST O5
    WHERE F6.PRD_APH_KEY = P6.PRD_APH_KEY
    AND P6.PRD_APH_KEY = PT6.PRD_APH_KEY
    AND PT6.PRD_AFH_KEY = T6.PRD_AFH_KEY
    AND I5.PRD_LVL_CHILD = F6.PRD_LVL_CHILD
    AND O5.ORG_LVL_CHILD = T6.ORG_LVL_CHILD
    AND F6.AUDIT_TYPE = 'A'
    AND    O5.ORG_LVL_NUMBER = {cod_local}
    AND   I5.cod_area = '{cod_area}'
    GROUP BY F6.PRD_LVL_CHILD,T6.ORG_LVL_CHILD
            ) X6 ON
    
    TO_NUMBER(X6.PRD_LVL_CHILD) = T.PRD_LVL_CHILD
    AND TO_NUMBER(X6.ORG_LVL_CHILD) = O.ORG_LVL_CHILD 
        
    WHERE T.PRD_LVL_ID IN (1,0)
    AND  T.PRD_STYLE_IND = 'F'
    AND O.ORG_LVL_NUMBER= {cod_local}
    AND T.COD_AREA='{cod_area}'
    """
    return query_s

ob_bq=bq.bigq(json_bq)
dfp=ob_bq.consultar_table(query_area_producto)

def consultar_precio_pmm(cod_local,cod_area,ob_ora,q_p):
    v_q_precios=query_precios(cod_local,cod_area)
    tp_v_precios=ob_ora.ejecutar_query(v_q_precios)
    if tp_v_precios:
        df_v_precios=pd.DataFrame(data=tp_v_precios,columns=["COD_LOCAL","SKU","AFECTO","COSTO","PRECIO_BASE","Q1","P1","C2","P2","PRECIO_PROMO","FECHAINI","FECHAFIN","COD_PROMO"])
        df_v_precios['COSTO']=df_v_precios['COSTO'].fillna(0)
        df_v_precios['PRECIO_BASE']=df_v_precios['PRECIO_BASE'].fillna(0)
        df_v_precios['PRECIO_PROMO']=df_v_precios['PRECIO_PROMO'].fillna(0)
        df_v_precios['Q1']=df_v_precios['Q1'].fillna(0)
        df_v_precios['P1']=df_v_precios['P1'].fillna(0)
        df_v_precios['C2']=df_v_precios['C2'].fillna(0)
        df_v_precios['P2']=df_v_precios['P2'].fillna(0)
        df_v_precios['COD_LOCAL_IT']=str(cod_local)
        df_v_precios['COD_AREA_IT']=str(cod_area)
        df_v_precios['COD_LOCAL']=df_v_precios['COD_LOCAL'].astype(str)
        df_v_precios['SKU']=df_v_precios['SKU'].astype(str)
        df_v_precios['AFECTO']=df_v_precios['AFECTO'].astype(str)
        df_v_precios['COSTO']=df_v_precios['COSTO'].astype(str)
        df_v_precios['PRECIO_BASE']=df_v_precios['PRECIO_BASE'].astype(str)
        df_v_precios['Q1']=df_v_precios['Q1'].astype(str)
        df_v_precios['P1']=df_v_precios['P1'].astype(str)
        df_v_precios['C2']=df_v_precios['C2'].astype(str)
        df_v_precios['P2']=df_v_precios['P2'].astype(str)
        df_v_precios['PRECIO_PROMO']=df_v_precios['PRECIO_PROMO'].astype(str)
        df_v_precios['FECHAINI']=df_v_precios['FECHAINI'].astype(str)
        df_v_precios['FECHAFIN']=df_v_precios['FECHAFIN'].astype(str)
        df_v_precios['COD_PROMO']=df_v_precios['COD_PROMO'].astype(str)
        q_p.put(df_v_precios)
        return 1
    return 0
def consultar_surtido_pmm(cod_local,cod_area,ob_ora,q_s):
    v_q_surtido=query_surtido(cod_local,cod_area)
    tp_v_surtido=ob_ora.ejecutar_query(v_q_surtido)
    if tp_v_surtido:
        df_v_surtido=pd.DataFrame(data=tp_v_surtido,columns=["SKU","COD_LOCAL","CLUSTER_SURTIDO","ESTADO_CIA","DESCRIPCION_EST_CIA","FEC_EST_CIA","EST_LOCAL","DES_EST_LOCAL","FEC_EST_LOCAL","FEC_CATALOG","CENT_SUMINISTRADOR","DES_CENTRO"])
        df_v_surtido['COD_LOCAL_IT']=str(cod_local)
        df_v_surtido['COD_AREA_IT']=str(cod_area)
        df_v_surtido['SKU']=df_v_surtido['SKU'].map(str)
        df_v_surtido['COD_LOCAL']=df_v_surtido['COD_LOCAL'].map(str)
        df_v_surtido['CLUSTER_SURTIDO']=df_v_surtido['CLUSTER_SURTIDO'].map(str)
        df_v_surtido['ESTADO_CIA']=df_v_surtido['ESTADO_CIA'].map(str)
        df_v_surtido['DESCRIPCION_EST_CIA']=df_v_surtido['DESCRIPCION_EST_CIA'].map(str)
        df_v_surtido['FEC_EST_CIA']=df_v_surtido['FEC_EST_CIA'].map(str)
        df_v_surtido['EST_LOCAL']=df_v_surtido['EST_LOCAL'].map(str)
        df_v_surtido['DES_EST_LOCAL']=df_v_surtido['DES_EST_LOCAL'].map(str)
        df_v_surtido['FEC_EST_LOCAL']=df_v_surtido['FEC_EST_LOCAL'].map(str)
        df_v_surtido['FEC_CATALOG']=df_v_surtido['FEC_CATALOG'].map(str)
        df_v_surtido['CENT_SUMINISTRADOR']=df_v_surtido['CENT_SUMINISTRADOR'].map(str)
        df_v_surtido['DES_CENTRO']=df_v_surtido['DES_CENTRO'].map(str)
        q_s.put(df_v_surtido)
        return 1
    return 0

def procesar_precio_surtido_pmm(cod_local,cod_area):
    ob_ora=ora.oracle(usuario=usuario,contraseña=contraseña,dsn=ip_server_name)
    ob_ora.inicializar_oracle()
    ob_ora.conectar_oracle()
    q_p=queue.Queue()
    q_s=queue.Queue()

    flag_data_precio=consultar_precio_pmm(cod_local,cod_area,ob_ora,q_p)
    flag_data_surtido=consultar_surtido_pmm(cod_local,cod_area,ob_ora,q_s)
   
    if flag_data_precio!=1:
       print(f"No hay data de precios para el local {cod_local} y area {cod_area}")
       df_q_p=False
    else:
        df_q_p=q_p.get()
        
    if flag_data_surtido!=1:
        print(f"No hay data de surtido para el local {cod_local} y area {cod_area}")
        df_q_s=False
    else:
        df_q_s=q_s.get()
    ob_ora.cerrar()
    return flag_data_precio,df_q_p,flag_data_surtido,df_q_s

df_precios_acum=pd.DataFrame(columns=["COD_LOCAL_IT","COD_AREA_IT","COD_LOCAL","SKU","AFECTO","COSTO","PRECIO_BASE","Q1","P1","C2","P2","PRECIO_PROMO","FECHAINI","FECHAFIN","COD_PROMO"])
df_surtido_acum=pd.DataFrame(columns=["COD_LOCAL_IT","COD_AREA_IT","SKU","COD_LOCAL","CLUSTER_SURTIDO","ESTADO_CIA","DESCRIPCION_EST_CIA","FEC_EST_CIA","EST_LOCAL","DES_EST_LOCAL","FEC_EST_LOCAL","FEC_CATALOG","CENT_SUMINISTRADOR","DES_CENTRO"])

project="sistemas-bi"
dataset="SPSA_STAGE"
table_p="tmp_precios_lf"
table_s="tmp_surtido_lf"

ob_bq.clear_table(project,dataset,table_p)
ob_bq.clear_table(project,dataset,table_s)

print(" se truncaron las tablas tmp_precios_lf y tmp_surtido_lf en bq ")

if dfp.empty!=True:
    dfp_f=pd.DataFrame(data=dfp)
    lsp=dfp_f['codigo_area'].values.tolist()

dfl=ob_bq.consultar_table(query_dim_local_modelo)
if dfl.empty!=True:
    dfl_f=pd.DataFrame(data=dfl)
    lsl=dfl_f['codigo_local_pmm'].values.tolist() 
    for i in lsl:
        print(lsl)
        print(lsp)

        with concurrent.futures.ThreadPoolExecutor(max_workers=c) as executor:
            resultados = [executor.submit(procesar_precio_surtido_pmm,i,a) for a in lsp]

        for resultado in concurrent.futures.as_completed(resultados):
            fprecio,dfprecio,fsurtido,dfsurtido=resultado.result()
            if fprecio==1:
                df_precios_acum=pd.concat([df_precios_acum,dfprecio])
            if fsurtido==1:
                df_surtido_acum=pd.concat([df_surtido_acum,dfsurtido])

        if df_surtido_acum.empty!=True:
            ob_bq.ins_table(project,dataset,table_s,df_surtido_acum)
        if df_surtido_acum.empty!=True:
            ob_bq.ins_table(project,dataset,table_p,df_precios_acum)

        print(f"se termino de procesar la data del local {i} en big query")

    #ob_bq.exec_query_sin_param(" call `sistemas-bi.SPSA.ins_tmp_detalle_pivot_lf`()")
        
"""
for f in lsp:
            fprecio,dfprecio,fsurtido,dfsurtido=procesar_precio_surtido_pmm(i,f)
            if fprecio==1:
             df_precios_acum=pd.concat([df_precios_acum,dfprecio])
            if fsurtido==1:
             df_surtido_acum=pd.concat([df_surtido_acum,dfsurtido])
        print(df_surtido_acum)
       
"""
        

           
            



