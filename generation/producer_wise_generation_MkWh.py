import numpy as np
from pandas import ExcelWriter
from utils import LDD_CONNECTOR as CONNECTOR
import pandas as pd
import matplotlib.pyplot as plt

from_date = '2021-01-01'
to_date = '2023-09-30'

query_str = f"""
SELECT kwh.date, pr.name AS 'power_producer', (SUM(kwh.value) / POW(10, 6)) AS total_gen_Mkwh
FROM
pgcbfinal.kilo_watt_hour AS kwh
JOIN generation_unit AS g ON kwh.generation_unit_id = g.id
JOIN power_station AS ps ON g.power_station_id = ps.id
JOIN producer AS pr ON ps.producer_id = pr.id
JOIN fuel AS f ON ps.fuel_id = f.id
JOIN `pgcbfinal`.`area` AS z ON ps.area_id = z.id
WHERE kwh.date BETWEEN '{from_date}' AND '{to_date}'
GROUP BY kwh.date, pr.name
"""

df = pd.read_sql_query(query_str, CONNECTOR, index_col='date')
# df1 = df.pivot(index=None, columns='fuel_name', values='gen_mw')
df1 = pd.pivot_table(df, index=df.index, columns='power_producer', values='total_gen_Mkwh', aggfunc=np.sum)
df2 = df1.fillna(0)
sum_col = df2.apply(np.sum, axis=1)
df2.loc[:, 'Total_Gen'] = sum_col
df2.iloc[:, :-1].plot.area()
plt.show()

df2.loc[:, '_BPDB'] = df2.loc[:, 'PDB'] + df2.loc[:, 'SIPP, PDB']
df2.loc[:, '_IPP'] = df2.loc[:, 'BPDB-RPCL'] + df2.loc[:, 'IPP']
df2.loc[:, '_Gen_Coms'] = df2.loc[:, 'APSCL'] + df2.loc[:, 'EGCB'] + df2.loc[:, 'NWPGCL']
df2.loc[:, '_REB'] = df2.loc[:, 'SIPP, REB']
df2.loc[:, '_Rental'] = df2.loc[:, 'QRPP'] + df2.loc[:, 'RPP']
df2.loc[:, '_HVDC'] = df2.loc[:, 'HVDC']
df2.loc[:, '_Tripura'] = df2.loc[:, 'Import']


month_list = [i.strftime('%Y-%m') for i in df2.index]
df2.loc[:, 'month'] = month_list
df_month = df2.groupby(by='month').sum()

year_list = [i.year for i in df2.index]
df2.loc[:, 'year'] = year_list
df_year = df2.groupby(by='year').sum()
# writer =
with ExcelWriter('power_producer_wise_generation_MkWh.xlsx') as writer:
    df2.to_excel(writer, sheet_name='daily_gen_MkWh')
    df_month.to_excel(writer, sheet_name='monthly_gen_MkWh')
    df_year.to_excel(writer, sheet_name='yearly_gen_MkWh')
a = 4
