import est_bq as bq
import est_ora as ora
import pandas as pd
import concurrent.futures
import threading
import datetime
import pytz

t_inicio = datetime.datetime.now()
pd.set_option('future.no_silent_downcasting', True)

c = 150
json_bq = r"D:\python\credenciales biq query\sistemas-bi-7a46b3894448.json"
usuario = "SINTERFACE"
contraseña = "SF5590X"
ip_server_name = "10.20.11.20/SPT01"


def query_local(fecha):
    query_local = f"""
        SELECT distinct
             O.ORG_LVL_NUMBER as "tienda"
            FROM IFHPHYPIDEE PHY ,
                IFHPRDMST   IFH ,
                ORGMSTEE    O   ,
                INVTYPEE    Y   ,
                IFHPHYPIOEE OEE ,
                IFHPHYSTSEE SEE ,
                IFHPHYREAEE R   ,
                ifhphypihee HEE ,
                WMS_DIG_PROC_STOCK_WMS WDP
        WHERE PHY.PRD_LVL_CHILD    = IFH.PRD_LVL_CHILD
            AND PHY.ORG_LVL_CHILD    = O.ORG_LVL_CHILD
            AND PHY.INV_TYPE_CODE    = Y.INV_TYPE_CODE
            AND OEE.PHY_CTRL_NUM     = PHY.PHY_CTRL_NUM
            AND OEE.ORG_LVL_CHILD    = PHY.ORG_LVL_CHILD
            AND OEE.PHY_STATUS       = SEE.PHY_STS_ACTION
            AND R.ID_PHY_REASON(+)   = PHY.ID_PHY_REASON
            and HEE.PHY_CTRL_NUM     = PHY.PHY_CTRL_NUM
            AND WDP.ORG_LVL_CHILD(+) = PHY.ORG_LVL_CHILD
            AND WDP.PRD_LVL_CHILD(+) = PHY.PRD_LVL_CHILD
            AND OEE.PHY_OPEN_DATE = TO_DATE('{fecha}', 'YYYYMMDD') 
    """
    return query_local


def consulta_movimiento_inventario(fecha, local):
    query_movimiento_inventario = f"""
        SELECT PHY.PHY_CTRL_NUM || ''  as "Conteo",
             O.ORG_LVL_NUMBER        as "Tienda",
             O.ORG_NAME_FULL         as "Desc. Tienda",
             PHY.INV_TYPE_CODE       as "Tipo de inventario",
             HEE.PHY_SOURCE_INV      as "Origen",
             Y.INV_TYPE_DESC         as "Descripción inventario",
             IFH.PRD_LVL_NUMBER      as "Código SKU",
             IFH.PRD_FULL_NAME       as "Decripción Producto",
             IFH.DES_TIPNEG          as "Tipo de Producto",
             PHY_FRZ_DATE            as "Fecha de Ajuste",
             PHY.PHY_ACT_CNT_SALA    as "Cantidad Sala",
             PHY.PHY_ACT_CNT_BOD     as "Cantidad Almacen",
             NVL(WDP.QTY_WMS_ALM,0)  as "Cantidad WMS"    ,
             PHY.ON_HAND_QTY         as "Cantidad Teórica",
             PHY.PHY_ACT_CNT         as "Cantidad Física",
             DECODE(TRIM(PHY.PHY_ACT_CNT), NULL, 'No Contado', '') 
                                     as "No Contado",
             PHY.PHY_QTY_VAR         as "Cantidad Diferencia",
             OEE.PHY_STATUS          as "Código Estado",
             SEE.PHY_STATUS_DESC     as "Descripción Estado",
             PHY.ID_PHY_REASON       as "Motivo",
             R.ID_DESC_REASON        as "Descripcion",
             PHY.ON_HAND_QTY * PHY.PHY_FRZ_CST "Valor teórico",
             PHY.PHY_ACT_CNT * PHY.PHY_FRZ_CST "Valor físico",
             PHY.PHY_QTY_VAR * PHY.PHY_FRZ_CST "Valor diferencia",
             IFH.COD_AREA            as "Area",
             IFH.DES_AREA            as "Descripción Area",
             IFH.COD_SEC             as "Sección",
             IFH.DES_SEC             as "Descripción Sección",
             IFH.COD_CAT             as "Línea",
             IFH.DES_CAT             as "Descripción Línea ",
             IFH.COD_FAM             as "Familia",
             IFH.DES_FAM             as "Descricpión Familia",
             IFH.COD_SFAM            as "Subfamilia",
             IFH.DES_SFAM            as "Descripción Subfamilia"
        FROM IFHPHYPIDEE PHY ,
             IFHPRDMST   IFH ,
             ORGMSTEE    O   ,
             INVTYPEE    Y   ,
             IFHPHYPIOEE OEE ,
             IFHPHYSTSEE SEE ,
             IFHPHYREAEE R   ,
             ifhphypihee HEE ,
             WMS_DIG_PROC_STOCK_WMS WDP
       WHERE PHY.PRD_LVL_CHILD    = IFH.PRD_LVL_CHILD
         AND PHY.ORG_LVL_CHILD    = O.ORG_LVL_CHILD
         AND PHY.INV_TYPE_CODE    = Y.INV_TYPE_CODE
         AND OEE.PHY_CTRL_NUM     = PHY.PHY_CTRL_NUM
         AND OEE.ORG_LVL_CHILD    = PHY.ORG_LVL_CHILD
         AND OEE.PHY_STATUS       = SEE.PHY_STS_ACTION
         AND R.ID_PHY_REASON(+)   = PHY.ID_PHY_REASON
         and HEE.PHY_CTRL_NUM     = PHY.PHY_CTRL_NUM
         AND WDP.ORG_LVL_CHILD(+) = PHY.ORG_LVL_CHILD
         AND WDP.PRD_LVL_CHILD(+) = PHY.PRD_LVL_CHILD
         AND OEE.PHY_OPEN_DATE = TO_DATE('{fecha}', 'YYYYMMDD') 
         AND O.ORG_LVL_NUMBER={local}
    """
    return query_movimiento_inventario


def valor_reemplazo(col):
    if col.dtype == 'float64' or col.dtype == 'int64':
        return 0  
    elif col.dtype == 'object':
        return ''  
    elif col.dtype == 'datetime64[ns]':
        return pd.Timestamp('1900-01-01')
    else:
        return None  


lima_tz = pytz.timezone('America/Lima')
lima_now = datetime.datetime.now(lima_tz)
fecha_ayer = lima_now + datetime.timedelta(days=-1)
fecha_ayer = datetime.datetime.strftime(fecha_ayer, "%Y%m%d")
print(f"se esta procesando data del día {fecha_ayer}")

obj_ora = ora.oracle(usuario, contraseña, ip_server_name)
obj_ora.inicializar_oracle()
obj_ora.conectar_oracle()
tp_local = obj_ora.ejecutar_query(query_local(fecha_ayer))
ls_local = [elemento_tupla[0] for elemento_tupla in tp_local]

def procesar_local(local):
    tp = obj_ora.ejecutar_query(consulta_movimiento_inventario(fecha_ayer, local))
    dfv = pd.DataFrame(data=tp, columns=["conteo","tienda","des_tienda","tipo_inventario","origen","descripcion_inventario","sku","descripcion_producto","tipo_producto","fecha_ajuste","cantidad_sala","cantidad_almacen","cantidad_wms","cantidad_teorico","cantidad_fisica","no_contado","cantidad_diferencia","codigo_estado","descripcion_estado","motivo","descripcion","valor_teorico","valor_fisico","valor_diferencia","area","descripcion_area","seccion","descripcion_seccion","linea","descripcion_linea","familia","descripcion_familia","subfamilia","descripcion_subfamilia"])
    print(f"Df del Local {local} procesado")
    if not dfv.empty:
        for col in dfv.columns:
            dfv[col] = dfv[col].fillna(valor_reemplazo(dfv[col]))
        dfv['motivo'] = dfv['motivo'].replace('', 0).round(0).astype(int)
        for col in dfv.columns:
            dfv[col] = dfv[col].map(str)
        
        return dfv
    return None


df_acum_mi = pd.DataFrame(columns=["conteo","tienda","des_tienda","tipo_inventario","origen","descripcion_inventario","sku","descripcion_producto","tipo_producto","fecha_ajuste","cantidad_sala","cantidad_almacen","cantidad_wms","cantidad_teorico","cantidad_fisica","no_contado","cantidad_diferencia","codigo_estado","descripcion_estado","motivo","descripcion","valor_teorico","valor_fisico","valor_diferencia","area","descripcion_area","seccion","descripcion_seccion","linea","descripcion_linea","familia","descripcion_familia","subfamilia","descripcion_subfamilia"])
ls_df_mi = []
d=0
with concurrent.futures.ThreadPoolExecutor(max_workers=c) as executor:
    future_to_local = {executor.submit(procesar_local, local): local for local in ls_local}
    for future in concurrent.futures.as_completed(future_to_local):
        local = future_to_local[future]
        try:
            d=d+1
            print(d)
            dfv = future.result()
            if dfv is not None:
                ls_df_mi.append(dfv)
        except Exception as exc:
            print(f"El local {local} generó una excepción: {exc}")

file_json = json_bq
project="sistemas-bi"
dataset="SPSA_STAGE"
table="tmp_toma_inventario"

if ls_df_mi:
    df_acum_mi = pd.concat(ls_df_mi, ignore_index=True)
    obj_bq=bq.bigq(file_json)
    obj_bq.clear_table(project,dataset,table)
    obj_bq.ins_table(project,dataset,table,df_acum_mi)
    obj_bq.exec_query_sin_param("call `sistemas-bi.SPSA.ins_fact_toma_inventario`()")

else:
    print("No se generaron datos para subir a BigQuery")

obj_ora.cerrar()

t_fin = datetime.datetime.now()
tiempo = t_fin - t_inicio
print("tiempo total de ejecución es ", tiempo)
