import datetime
import os.path

from utils import CONNECTOR  # may be mysql.connector or django sql connector
import mysql.connector
import pandas as pd
import openpyxl

t1 = datetime.datetime.now()

from_datetime_str = '2022-01-01 00:00'
to_datetime_str = '2022-12-31 23:00'
folder_path = r'I:\My Drive\IMD\Analysis\Load\Zonewise'

query = f"""
SELECT T_tr.date_time, T_tr.zone AS zone_id, z.name AS zone_name
, tr_MW+IFNULL(T_gen.gen_MW,0) AS zone_total_load
FROM
(
SELECT s.zone, MW.date_time, sum(MW.value) as tr_MW
 -- tt.voltage_level, se.is_transformer_low
FROM mega_watt AS MW
JOIN substation_equipment AS se ON se.id = MW.sub_equip_id
JOIN transformer AS t ON se.transformer_id = t.id
JOIN transformer_type AS tt ON tt.id = t.type_id
JOIN substation AS s ON se.substation_id = s.id
WHERE se.is_transformer_low = 1
AND (tt.id = 1 OR tt.id = 6 OR tt.id = 7 OR tt.id = 8)
AND t.is_auxiliary = 0
AND MW.date_time BETWEEN '{from_datetime_str}' AND '{to_datetime_str}'
GROUP BY MW.date_time, s.zone
) AS T_tr

LEFT JOIN 

(
-- SET GLOBAL Innodb_buffer_pool_size = 5168709120;
SELECT MW.date_time, s.zone, sum(MW.value) AS gen_MW
-- f.is_generation, se.is_feeder
FROM mega_watt AS MW
JOIN substation_equipment AS se ON se.id = MW.sub_equip_id
JOIN feeder AS f ON se.feeder_id = f.id
JOIN substation AS s ON se.substation_id = s.id
-- JOIN zone AS z ON z.id = s.zone
-- JOIN gmd ON gmd.id = s.gmd
WHERE f.is_generation = 1
AND MW.date_time BETWEEN '{from_datetime_str}' AND '{to_datetime_str}'
GROUP BY MW.date_time, s.zone
) AS T_gen
ON T_tr.zone = T_gen.zone AND T_tr.date_time = T_gen.date_time

JOIN 

zone AS z
ON z.id = T_tr.zone
"""

df = pd.read_sql_query(sql=query, con=CONNECTOR, index_col=['date_time'])
df1 = pd.pivot_table(df, index=df.index, columns='zone_name', values='zone_total_load')
df1.to_excel(os.path.join(folder_path, 'hourly_zone_total_load_from_ois_2022.xlsx'))
a = 4
