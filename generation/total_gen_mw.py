import numpy as np

from utils import LDD_CONNECTOR as CONNECTOR
import pandas as pd

from_datetime = '2022-03-01 00:00'
to_datetime = '2022-06-30 23:00'
query_str = f"""
SELECT mw.date_time, SUM(mw.value) AS gen_mw
FROM pgcbfinal.mega_watt AS mw
WHERE mw.date_time BETWEEN '{from_datetime}' AND '{to_datetime}'
GROUP BY mw.date_time
"""

df = pd.read_sql_query(query_str, CONNECTOR, index_col='date_time')
# df1 = df.pivot(index=None, columns='fuel_name', values='gen_mw')
df.loc[:, 'hour_min'] = list(map(lambda x: x.strftime('%H:%M'), df.index))
df.loc[:, 'date'] = list(map(lambda x: x.strftime('%d %b'), df.index))
df1 = pd.pivot_table(df, index='hour_min', columns='date', values='gen_mw', aggfunc=np.average)
df2 = df1.fillna(0)
sum_col = df2.apply(np.sum, axis=1)
df2.loc[:, 'Total_Gen'] = sum_col
df2.to_excel('fuelwise_generation_mw.xlsx')
a = 4
