import numpy as np
import pandas as pd
from utils import CONNECTOR

from_date_str = '2023-7-1'
to_date_str = '2023-07-31 23:00'
query_str = f"""
SELECT s.name as 'substation', se.substation_id AS 'ss_id', se.name as 'equipment', se.id AS 'eq_id', 
v.value AS 'voltage', ev.name AS 'base_kv', v.date_time
FROM 
ois.bus AS b 
JOIN ois.substation_equipment AS se ON b.id = se.bus_id
JOIN ois.equipment_voltage AS ev ON b.bus_voltage = ev.id
JOIN ois.voltage AS v ON v.sub_equip_id = se.id
join ois.substation as s on s.id = se.substation_id
WHERE v.date_time BETWEEN '{from_date_str}' AND '{to_date_str}'
AND v.value > 80 and v.value < 500 
"""

df = pd.read_sql(sql=query_str, con=CONNECTOR)
df['base_kv_float'] = df['base_kv'].apply(lambda x: float(x.split(' ')[0]))
df['voltage_pu'] = df['voltage'] / df['base_kv_float']

_quantile_array = np.arange(start=0.05, stop=1.0, step=.05)
quantile_array = np.concatenate([[0.02], _quantile_array, [.98]])

df_400kV = df[df['base_kv_float'] == 400.0]
df_pivoted_400_pu = pd.pivot_table(data=df_400kV, index=['date_time'],
                                columns=['substation', 'equipment'], values='voltage_pu')
df_400kV_pu_quantile = df_pivoted_400_pu.quantile(quantile_array, method='single',
                                                  interpolation='lower', numeric_only=True).T

df_pivoted_400 = pd.pivot_table(data=df_400kV, index=['date_time'],
                                columns=['substation', 'equipment'], values='voltage')
df_400kV_quantile = df_pivoted_400.quantile(quantile_array, method='single', numeric_only=True, interpolation='lower').T

"""*************230kV************"""
df_230kV = df[df['base_kv_float'] == 230.0]
df_pivoted_230_pu = pd.pivot_table(data=df_230kV, index=['date_time'],
                                columns=['substation', 'equipment'], values='voltage_pu')
df_230kV_pu_quantile = df_pivoted_230_pu.quantile(quantile_array, method='single', numeric_only=True,
                                                  interpolation='lower').T

df_pivoted_230 = pd.pivot_table(data=df_230kV, index=['date_time'],
                                columns=['substation', 'equipment'], values='voltage')
df_230kV_quantile = df_pivoted_230.quantile(quantile_array, method='single', numeric_only=True, interpolation='lower').T

"""*********132kV*************"""
df_132kV = df[df['base_kv_float'] == 132.0]
df_pivoted_132_pu = pd.pivot_table(data=df_132kV, index=['date_time'],
                                columns=['substation', 'equipment'], values='voltage_pu')
df_132kV_pu_quantile = df_pivoted_132_pu.quantile(quantile_array, method='single', numeric_only=True,
                                                  interpolation='lower').T

df_pivoted_132 = pd.pivot_table(data=df_132kV, index=['date_time'],
                                columns=['substation', 'equipment'], values='voltage')
df_132kV_quantile = df_pivoted_132.quantile(quantile_array, method='single', numeric_only=True,
                                            interpolation='lower').T

a = 5

with pd.ExcelWriter('bus_voltage_percentile_rank.xlsx') as writer:
    df_400kV_quantile.to_excel(writer, sheet_name='400kV_value')
    df_230kV_quantile.to_excel(writer, sheet_name='230kV_value')
    df_132kV_quantile.to_excel(writer, sheet_name='132kV_value')
    df_400kV_pu_quantile.to_excel(writer, sheet_name='400kV_pu')
    df_230kV_pu_quantile.to_excel(writer, sheet_name='230kV_pu')
    df_132kV_pu_quantile.to_excel(writer, sheet_name='132kV_pu')
