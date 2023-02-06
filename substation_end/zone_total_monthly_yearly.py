import numpy as np
from pandas import ExcelWriter
from utils import LDD_CONNECTOR as CONNECTOR
import pandas as pd
import matplotlib.pyplot as plt

from_datetime = '2016-01-01 00:00'
to_datetime = '2016-01-01 00:00'

query_str = f"""
SELECT s.date_time, g.name AS `zone`, s.value AS zone_total
FROM                                               
pgcbfinal.gmd_supply AS s                          
JOIN pgcbfinal.gmd AS g ON s.gmd_id = g.id         
WHERE s.date_time BETWEEN '{from_datetime}' AND '{to_datetime}'
"""

df = pd.read_sql_query(query_str, CONNECTOR, index_col=None)
# df1 = df.pivot(index=None, columns='fuel_name', values='gen_mw')
df1 = pd.pivot_table(df, index='date_time', columns='zone', values='zone_total', aggfunc=np.average)
dhaka_cols = [col for col in df1.columns if 'dhaka' in col.lower()]
non_dhaka_cols = [col for col in df1.columns if 'dhaka' not in col.lower()]
df_dhaka = df1.loc[:, dhaka_cols]
df_non_dhaka = df1.loc[:, non_dhaka_cols]

series_dhaka_sum = df_dhaka.apply(np.sum, axis=1)
df_non_dhaka.insert(0, 'Dhaka', series_dhaka_sum)
op_df = df_non_dhaka

date_range = pd.date_range(start=from_datetime, end=to_datetime, freq='1M', inclusive='left')
with ExcelWriter('zone_total_load_2021.xlsx') as writer:
    for i, date_time in enumerate(date_range):
        current_month_str = f'{date_time.year}-{date_time.month}'
        current_month_df = op_df.loc[current_month_str]
        current_sheet_name = date_time.strftime('%b-%y')
        current_month_df.to_excel(writer, sheet_name=current_sheet_name)
#     df2.to_excel(writer, sheet_name='hourly_gen')
#     df3.to_excel(writer, sheet_name='yearly_gen')
a = 4
