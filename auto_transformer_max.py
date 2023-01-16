import datetime
from utils import CONNECTOR  # may be mysql.connector or django sql connector
import mysql.connector
import pandas as pd
import openpyxl

t1 = datetime.datetime.now()


def max_ss_load_mw(from_datetime_str, to_datetime_str, excel_path=r'max_ss_load_mw.xlsx', mw_thresh=350,
                   from_hour1=None,
                   to_hour1=None, from_hour2=None, to_hour2=None):
    if from_hour1:
        between_hour_str = f'AND (HOUR(MW.date_time) BETWEEN {from_hour1} AND {to_hour1})'
        if from_hour2:
            between_hour_str = f'''AND ((HOUR(MW.date_time) BETWEEN {from_hour1} AND {to_hour1}) 
                -- OR (HOUR(MW.date_time) BETWEEN {from_hour2} AND {to_hour2}))'''
    else:
        between_hour_str = ''

    max_zt_query_str = f"""

SELECT T_mw_mvar.substation, T_mw_mvar.transformer, T_mw_mvar.mva_rating, T_mw_mvar.ht_base_kv,
T_mw_mvar.lt_base_kv, T_mw_mvar.Tr_LT_MW_Max AS Tr_MW_Max, T_mw_mvar.Tr_LT_MVAR AS Tr_MVAR, 
SQRT(POW(T_mw_mvar.Tr_LT_MW_Max, 2) + POW(T_mw_mvar.Tr_LT_MVAR, 2)) AS Tr_MVA,
T_v.kV AS 'voltage (kV)', 
SQRT(POW(T_mw_mvar.Tr_LT_MW_Max, 2) + POW(T_mw_mvar.Tr_LT_MVAR, 2)) * 1000 / ( SQRT(3) * T_v.kV) 
AS 'Tr_HT_Current (Ampere)',
T_mw_mvar.date_time
-- , Tr_MVA / (SQRT(3) * T_v.kV) AS 'Tr_HT_Current (Ampere)'

FROM 
(
SELECT s.id AS ss_id, s.name AS 'substation', T.transformer, mva.mva_rating, T.tr_flow AS Tr_LT_MW_Max, 
	abs(mvar.value) AS Tr_LT_MVAR, -- b.name AS bus_name,
	T.date_time, tr_type_fl.ht_kv AS ht_base_kv, tr_type_fl.lt_kv AS lt_base_kv, T.eq_id
FROM
(
SELECT se.id AS eq_id, tr.mva_id, se.substation_id, tr.name AS 'transformer', tr.type_id AS tr_type_id,
	ABS(mw.value) AS tr_flow, mw.date_time
FROM
ois.mega_watt AS mw
RIGHT JOIN ois.substation_equipment AS se ON mw.sub_equip_id = se.id
RIGHT JOIN ois.transformer AS tr ON tr.id = se.transformer_id
RIGHT JOIN ois.transformer_type AS tr_type ON tr_type.id = tr.type_id
WHERE se.is_transformer_low = 1
AND ( (tr_type.id BETWEEN 2 AND 4) OR tr_type.id =8)
AND ABS(mw.value) > 2  -- threshold
AND (mw.date_time BETWEEN '{from_datetime_str}' AND '{to_datetime_str}')
) AS T
-- GROUP BY se.id

RIGHT JOIN

(
SELECT se.id AS eq_id, MAX(ABS(mw.value)) AS max_tr_flow
FROM
ois.mega_watt AS mw
RIGHT JOIN ois.substation_equipment AS se ON mw.sub_equip_id = se.id
RIGHT JOIN ois.transformer AS tr ON tr.id = se.transformer_id
RIGHT JOIN ois.transformer_type AS tr_type ON tr_type.id = tr.type_id
WHERE se.is_transformer_low = 1
AND ( (tr_type.id BETWEEN 2 AND 4) OR tr_type.id =8)
AND ABS(mw.value) > 2  -- threshold
AND mw.date_time BETWEEN '{from_datetime_str}' AND '{to_datetime_str}'
GROUP BY se.id
) AS T_max
ON T.eq_id = T_max.eq_id AND T.tr_flow = T_max.max_tr_flow
-- GROUP BY se.id

JOIN ois.substation AS s ON T.substation_id = s.id
-- JOIN ois.equipment_voltage AS eqv ON eqv.id = s.sub_voltage
JOIN ois.transformer_mva AS mva ON  mva.id = T.mva_id
JOIN ois.transformer_type_float AS tr_type_fl ON tr_type_fl.id = T.tr_type_id

INNER JOIN mega_var AS mvar ON T.eq_id = mvar.sub_equip_id AND mvar.date_time = T.date_time
GROUP BY T.eq_id
)
AS T_mw_mvar

INNER JOIN 

-- SELECT T_v.* FROM 
(
SELECT v.date_time, v.value AS kV, se.id AS eq_id, b.name AS bus_name, eqv.base_kv
FROM voltage AS v
JOIN ois.substation_equipment AS se ON se.id = v.sub_equip_id
JOIN bus AS b ON b.id = se.bus_id
JOIN ois.eq_voltage_float AS eqv ON eqv.id = b.bus_voltage
WHERE v.date_time BETWEEN '{from_datetime_str}' AND '{to_datetime_str}'
)
AS T_v

ON T_mw_mvar.date_time = T_v.date_time AND T_mw_mvar.ht_base_kv = T_v.base_kv
-- WHERE T_v.kV > 100
GROUP BY T_mw_mvar.transformer
ORDER BY T_v.base_kv DESC, T_mw_mvar.substation, T_mw_mvar.transformer

"""

    max_min_kv_df = pd.read_sql_query(max_zt_query_str, CONNECTOR)
    # max_min_kv_df1 = max_min_kv_df.set_index(['id'])
    # max_min_kv_df1.to_excel(excel_path)
    print(f'time elapsed = {datetime.datetime.now() - t1}')
    # print(f'Excel written in {excel_path}')
    """HTML Conversion"""
    return max_min_kv_df


df = max_ss_load_mw(from_datetime_str='2022-11-1 00:00', to_datetime_str='2022-11-30 23:00',
                    # from_hour1=8, to_hour1=12,
                    excel_path='max_ss_load_mw_12.xlsx')
a = 5
df.to_excel('auto_transformer_max_load.xlsx')
a = 5
