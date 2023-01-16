import datetime

import numpy as np
from pandas import ExcelWriter
from utils import LDD_CONNECTOR as CONNECTOR
import pandas as pd
import matplotlib.pyplot as plt

from_datetime = '2016-01-01 00:00'
to_datetime = '2023-01-31 23:00'
query_str = f"""
SELECT kwh.date, f.name AS 'fuel', (SUM(kwh.value) / POW(10, 6)) AS total_gen_Mkwh
FROM
pgcbfinal.kilo_watt_hour AS kwh
JOIN generation_unit AS g ON kwh.generation_unit_id = g.id
JOIN power_station AS ps ON g.power_station_id = ps.id
JOIN producer AS pr ON ps.producer_id = pr.id
JOIN fuel AS f ON ps.fuel_id = f.id
JOIN `pgcbfinal`.`area` AS z ON ps.area_id = z.id
WHERE kwh.date BETWEEN '{from_datetime}' AND '{to_datetime}'
GROUP BY kwh.date, f.name
"""

df = pd.read_sql_query(query_str, CONNECTOR, index_col='date')
# df1 = df.pivot(index=None, columns='fuel_name', values='gen_mw')
df1 = pd.pivot_table(df, index=df.index, columns='fuel', values='total_gen_Mkwh', aggfunc=np.sum)
df2 = df1.fillna(0)
sum_col = df2.apply(np.sum, axis=1)
df2.loc[:, 'Total_Gen'] = sum_col
# df2.iloc[:, :-1].plot.area()
# plt.show()

year_list = [i.year for i in df2.index]


def fin_year_from_datetime(date_time: datetime.datetime) -> str:
    if date_time.month <= 6:
        return f'{date_time.year - 1}-{date_time.year}'
    else:
        return f'{date_time.year}-{date_time.year+1}'


f_year_list = list(map(fin_year_from_datetime, df2.index))
df2.loc[:, 'fin_year'] = f_year_list
df3 = df2.groupby(by='fin_year').sum()
# writer =
with ExcelWriter('fuelwise_generation_MkWh_FY.xlsx') as writer:
    df2.to_excel(writer, sheet_name='daily_gen')
    df3.to_excel(writer, sheet_name='fin_yearly_gen')
a = 4
