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

SELECT z.id AS z_id, z.name AS z_name, s.name, s.id as ss_id, T_ss_MW1.* FROM 
(
SELECT T_tr.id, T_tr.date_time, tr_MW+IFNULL(T_gen.gen_MW,0) AS ss_MW
FROM
(
SELECT s.id, MW.date_time, sum(MW.value) as tr_MW
-- tt.voltage_level, se.is_transformer_low
FROM mega_watt AS MW
JOIN substation_equipment AS se ON se.id = MW.sub_equip_id
JOIN transformer AS t ON se.transformer_id = t.id
JOIN transformer_type AS tt ON tt.id = t.type_id
JOIN substation AS s ON se.substation_id = s.id
WHERE se.is_transformer_low = 1
and (s.id = 8 or s.id = 25 or s.id = 16 or s.id = 26)
AND (tt.id = 1 OR tt.id = 6 OR tt.id = 7 OR tt.id = 8)
AND t.is_auxiliary = 0
AND MW.date_time BETWEEN '{from_datetime_str}' AND '{to_datetime_str}'
{between_hour_str}

GROUP BY MW.date_time, s.id
) AS T_tr
LEFT JOIN 
(
-- SET GLOBAL Innodb_buffer_pool_size = 5168709120;
SELECT MW.date_time, s.id, sum(MW.value) AS gen_MW
-- f.is_generation, se.is_feeder
FROM mega_watt AS MW
JOIN substation_equipment AS se ON se.id = MW.sub_equip_id
JOIN feeder AS f ON se.feeder_id = f.id
JOIN substation AS s ON se.substation_id = s.id
-- JOIN zone AS z ON z.id = s.zone
-- JOIN gmd ON gmd.id = s.gmd
WHERE f.is_generation = 1
AND MW.date_time BETWEEN '{from_datetime_str}' AND '{to_datetime_str}' 
{between_hour_str}

GROUP BY MW.date_time, s.id
) AS T_gen
ON T_tr.id = T_gen.id AND T_tr.date_time = T_gen.date_time
) AS T_ss_MW1


JOIN substation AS s ON s.id = T_ss_MW1.id
JOIN zone AS z ON z.id = s.zone
-- GROUP BY s.id
ORDER BY 2, 4, 5

"""

    loadshed_query_str = f"""
select * from general_event as ge
where is_loadshed = 1
and ge.date_time between '{from_datetime_str}' and '{to_datetime_str}'
-- and (s.id = 8 or s.id = 25 or s.id = 16 or s.id = 26)
"""
    loadshed_df = pd.read_sql_query(loadshed_query_str, CONNECTOR)
    max_min_kv_df = pd.read_sql_query(max_zt_query_str, CONNECTOR)
    max_min_kv_df1 = max_min_kv_df.set_index(['date_time', 'id'])
    a = 4
    # max_min_kv_df1.to_excel(excel_path)
    print(f'time elapsed = {datetime.datetime.now() - t1}')
    # print(f'Excel written in {excel_path}')
    """HTML Conversion"""
    return max_min_kv_df1


df = max_ss_load_mw(from_datetime_str='2023-8-6 00:00', to_datetime_str='2023-8-6 3:00',
                    # from_hour1=8, to_hour1=12,
                    excel_path='ss_load_mw_13.xlsx')
a = 5
df.to_csv('max_ss_load_mw_2.csv')
a = 5
