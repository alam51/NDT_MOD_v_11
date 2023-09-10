import pandas as pd
from utils import CONNECTOR

from_date_str = '2023-1-1'
to_date_str = '2023-1-1 1:00'
query_str = f"""
SELECT se.substation_id AS 'ss_id', se.id AS 'eq_id', 
v.value AS 'voltage', ev.name AS 'base_kv'
FROM 
ois.bus AS b 
JOIN ois.substation_equipment AS se ON b.id = se.bus_id
JOIN ois.equipment_voltage AS ev ON b.bus_voltage = ev.id
JOIN ois.voltage AS v ON v.sub_equip_id = se.id
WHERE v.date_time BETWEEN '{from_date_str}' AND '{to_date_str}'
AND v.value > 50
"""

df = pd.read_sql(sql=query_str, con=CONNECTOR)
# df['base_kv_float'] = df['base_kv'].apply(lambda x: float(x.split(' ')[0]))
# df['voltage_pu'] = df['voltage'] / df['base_kv_float']
df_pivoted = pd.pivot_table(data=df, index='date_time', columns=['ss_id', 'eq_id'], values='voltage')
a = 5