import datetime
from utils import CONNECTOR  # may be mysql.connector or django sql connector
import mysql.connector
import pandas as pd
import openpyxl

t1 = datetime.datetime.now()


def max_auto_transformer_load(from_datetime_str, to_datetime_str, mw_thresh=350,
                              from_hour1=None,
                              to_hour1=None, from_hour2=None, to_hour2=None):
    if from_hour1:
        between_hour_str = f'AND (HOUR(MW.date_time) BETWEEN {from_hour1} AND {to_hour1})'
        if from_hour2:
            between_hour_str = f'''AND ((HOUR(MW.date_time) BETWEEN {from_hour1} AND {to_hour1}) 
                -- OR (HOUR(MW.date_time) BETWEEN {from_hour2} AND {to_hour2}))'''
    else:
        between_hour_str = ''

    query_str = f"""
-- SET GLOBAL innodb_buffer_pool_size=1610612736;

SELECT T.`substation`, T.`transformer`, T.voltage_level, T.mw, T.mvar, T.mva, T.date_time FROM 
(
SELECT s.id AS 'ss',   tr.id AS 'tr', tt.voltage_level,  
MAX(POW(POW(mw.value, 2) + POW(mvar.value, 2), .5)) AS 'mva_max'
FROM
ois.transformer AS tr
JOIN ois.substation AS s ON s.id = tr.substation_id
JOIN ois.transformer_type AS tt ON tr.type_id = tt.id
JOIN ois.substation_equipment AS se ON se.transformer_id = tr.id
JOIN ois.mega_watt AS mw ON mw.sub_equip_id = se.id
JOIN ois.mega_var AS mvar ON mvar.sub_equip_id = se.id AND mw.date_time = mvar.date_time
WHERE tt.voltage_level IN ('400/230', '400/132', '230/132')
AND mw.date_time BETWEEN '{from_datetime_str}' AND '{to_datetime_str}'
AND mw.value < 600
GROUP BY se.id
) AS T_max

INNER JOIN

(
SELECT s.id AS 'ss', s.name AS 'substation', tr.id AS 'tr', tr.name AS 'transformer', tt.voltage_level, mw.value AS 'mw', mvar.value AS 'mvar',
POW(POW(mw.value, 2) + POW(mvar.value, 2), .5) AS 'mva', mw.date_time
FROM
ois.transformer AS tr
JOIN ois.substation AS s ON s.id = tr.substation_id
JOIN ois.transformer_type AS tt ON tr.type_id = tt.id
JOIN ois.substation_equipment AS se ON se.transformer_id = tr.id
JOIN ois.mega_watt AS mw ON mw.sub_equip_id = se.id
JOIN ois.mega_var AS mvar ON mvar.sub_equip_id = se.id AND mw.date_time = mvar.date_time
WHERE tt.voltage_level IN ('400/230', '400/132', '230/132')
AND mw.date_time BETWEEN '{from_datetime_str}' AND '{to_datetime_str}'
) AS T ON T_max.tr = T.tr AND T_max.mva_max = T.mva
GROUP BY T.tr
ORDER BY T.voltage_level DESC, T.`substation`, T.`transformer`
"""

    df = pd.read_sql_query(query_str, CONNECTOR)
    # max_min_kv_df1 = max_min_kv_df.set_index(['id'])
    # max_min_kv_df1.to_excel(excel_path)
    print(f'time elapsed = {datetime.datetime.now() - t1}')
    # print(f'Excel written in {excel_path}')
    """HTML Conversion"""
    return df


df = max_auto_transformer_load(from_datetime_str='2023-05-01 00:00', to_datetime_str='2023-05-31 23:00',
                               # from_hour1=8, to_hour1=12
                               )
a = 5
df.to_excel(r'H:\My Drive\IMD\Monthly_Report\2023\5.May\auto_transformer_max_load.xlsx')
a = 5
