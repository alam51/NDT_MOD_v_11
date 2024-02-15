import datetime
import os.path

import numpy as np

from utils import LDD_CONNECTOR as CONNECTOR1
import pandas as pd


def fuelwise_gen_mw(from_datetime: str, to_datetime: str, folder: str) -> None:
    query_str = f"""
    SELECT mw.date_time, g.grid_name, ps.fuel_id, f.name AS fuel_name, SUM(mw.value) as gen_mw
    FROM mega_watt AS mw
    JOIN generation_unit AS g ON g.id = mw.generation_unit_id
    JOIN power_station AS ps ON g.power_station_id = ps.id
    JOIN fuel AS f ON ps.fuel_id = f.id
    WHERE (mw.date_time BETWEEN '{from_datetime}' AND '{to_datetime}')
    -- AND g.grid_name = 'Eastern'
    GROUP BY mw.date_time, g.grid_name, ps.fuel_id
    """

    df = pd.read_sql_query(query_str, CONNECTOR1, index_col='date_time')
    # df1 = df.pivot(index=None, columns='fuel_name', values='gen_mw')
    df1 = pd.pivot_table(df, index=df.index, columns='fuel_name', values='gen_mw', aggfunc=np.sum)
    df2 = df1.fillna(0)
    sum_col = df2.apply(np.sum, axis=1)
    df2.loc[:, 'Total_Gen'] = sum_col
    df2.to_excel(os.path.join(folder, 'fuelwise_generation_mw.xlsx'))
    df.to_excel(os.path.join(folder, 'regionwise_generation_mw.xlsx'))
    a = 4
