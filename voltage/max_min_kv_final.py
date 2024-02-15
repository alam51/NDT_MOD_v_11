import datetime

import sqlalchemy

from utils import CONNECTOR  # may be mysql.connector or django sql connector
import mysql.connector
import pandas as pd
import openpyxl
import re
t1 = datetime.datetime.now()

sqlalchemy_con_str = "mysql://root:pgcb1234@localhost/ois"

# engine = sqlalchemy.create_engine(sqlalchemy_con_str, echo=True)

# eq_voltage_query_str = f"""
# select * from ois.equipment_voltage
# """
# eq_voltage_df = pd.read_sql_query(eq_voltage_query_str, CONNECTOR, index_col=['id'])
# eq_voltage_df.loc[:, 'base_kv'] = eq_voltage_df.loc[:, 'name'].apply(lambda x: float(re.findall('[0-9]+', x)[0]))
# eq_voltage_df1 = eq_voltage_df.drop(columns=['name'])
#
# eq_voltage_df1.to_sql('eq_voltage_float', con=sqlalchemy_con_str, if_exists='replace', index=True)

a = 5


def ss_max_min_voltage(from_datetime_str, to_datetime_str, excel_path=r'ss_max_min_kv.xlsx'):
    max_zt_query_str = f"""
    SELECT T_max.*, T_min.kV, T_min.date_time FROM(
SELECT T1.* FROM(
SELECT s.id, s.name, eqv.base_kv, v.value AS 'kV', v.date_time from
substation AS s 
INNER JOIN eq_voltage_float AS eqv ON s.sub_voltage = eqv.id
INNER JOIN bus AS b ON b.substation_id = s.id
INNER JOIN substation_equipment AS se ON se.bus_id = b.id
INNER JOIN voltage AS v ON v.sub_equip_id = se.id
WHERE s.is_active = 1 AND se.is_bus = 1 AND b.bus_voltage = s.sub_voltage
AND v.date_time BETWEEN '{from_datetime_str}' AND '{to_datetime_str}'
AND ((v.value < eqv.base_kv * 1.12) AND (v.value > eqv.base_kv * 0.85))
) AS T1

RIGHT JOIN (
SELECT s.id, s.name, eqv.base_kv, MAX(v.value) AS 'min_kV' FROM
substation AS s 
INNER JOIN eq_voltage_float AS eqv ON s.sub_voltage = eqv.id
INNER JOIN bus AS b ON b.substation_id = s.id
INNER JOIN substation_equipment AS se ON se.bus_id = b.id
INNER JOIN voltage AS v ON v.sub_equip_id = se.id
WHERE s.is_active = 1 AND se.is_bus = 1 AND b.bus_voltage = s.sub_voltage
AND v.date_time BETWEEN '{from_datetime_str}' AND '{to_datetime_str}'
AND ((v.value < eqv.base_kv * 1.12) AND (v.value > eqv.base_kv * 0.85))
GROUP BY s.id
) AS T2 ON T1.id = T2.id AND T1.kV = T2.min_kV
GROUP BY 1) AS T_max

JOIN 

(SELECT T1.* FROM(
SELECT s.id, s.name, eqv.base_kv, v.value AS 'kV', v.date_time from
substation AS s 
INNER JOIN eq_voltage_float AS eqv ON s.sub_voltage = eqv.id
INNER JOIN bus AS b ON b.substation_id = s.id
INNER JOIN substation_equipment AS se ON se.bus_id = b.id
INNER JOIN voltage AS v ON v.sub_equip_id = se.id
WHERE s.is_active = 1 AND se.is_bus = 1 AND b.bus_voltage = s.sub_voltage
AND v.date_time BETWEEN '{from_datetime_str}' AND '{to_datetime_str}'
AND ((v.value < eqv.base_kv * 1.12) AND (v.value > eqv.base_kv * 0.85))
) AS T1

RIGHT JOIN (
SELECT s.id, s.name, eqv.base_kv, MIN(v.value) AS 'min_kV' FROM
substation AS s 
INNER JOIN eq_voltage_float AS eqv ON s.sub_voltage = eqv.id
INNER JOIN bus AS b ON b.substation_id = s.id
INNER JOIN substation_equipment AS se ON se.bus_id = b.id
INNER JOIN voltage AS v ON v.sub_equip_id = se.id
WHERE s.is_active = 1 AND se.is_bus = 1 AND b.bus_voltage = s.sub_voltage
AND v.date_time BETWEEN '{from_datetime_str}' AND '{to_datetime_str}'
AND ((v.value < eqv.base_kv * 1.12) AND (v.value > eqv.base_kv * 0.85))
GROUP BY s.id
) AS T2 ON T1.id = T2.id AND T1.kV = T2.min_kV
GROUP BY 1) AS T_min ON T_max.id = T_min.id
ORDER BY base_kV DESC, NAME ASC
    """

    # max_min_kv_df = pd.read_sql_query(max_zt_query_str, CONNECTOR)
    max_min_kv_df = pd.read_sql_query(max_zt_query_str, CONNECTOR)
    max_min_kv_df.to_excel(excel_path)
    print(f'time elapsed = {datetime.datetime.now() - t1}')
    print(f'Excel written in {excel_path}')
    """HTML Conversion"""
    return max_min_kv_df


df = ss_max_min_voltage(from_datetime_str='2023-12-01 00:00', to_datetime_str='2023-12-31 23:00',
                        excel_path=r'G:\My Drive\IMD\Monthly_Report\2023\12.December\ss_max_min_kv.xlsx')
