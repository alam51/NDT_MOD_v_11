from utils import LDD_CONNECTOR as CONNECTOR
import pandas as pd

start_date_str = '2022-1-1'
end_date_str = '2022-12-31'

start_date = pd.to_datetime(start_date_str)
end_date = pd.to_datetime(end_date_str)
time_span = end_date - start_date
time_span_days = time_span.days

ampere_query_str = f"""
SELECT ps.name AS 'power_station', g.name AS 'generator', pr.name AS 'producer', z.name AS 'zone',
f.name AS 'fuel', SUM(kwh.value) AS total_gen_kwh, ps.installed_capacity
FROM
pgcbfinal.kilo_watt_hour AS kwh
JOIN generation_unit AS g ON kwh.generation_unit_id = g.id
JOIN power_station AS ps ON g.power_station_id = ps.id
JOIN producer AS pr ON ps.producer_id = pr.id
JOIN fuel AS f ON ps.fuel_id = f.id
JOIN `pgcbfinal`.`area` AS z ON ps.area_id = z.id
WHERE kwh.date BETWEEN '{start_date_str}' AND '{end_date_str}'
GROUP BY g.id
"""

df_plant_factor = pd.read_sql(ampere_query_str, CONNECTOR)
df_plant_factor.loc[:, 'plant_factor'] = df_plant_factor.loc[:, 'total_gen_kwh'] / \
                                         (df_plant_factor.loc[:, 'installed_capacity'] * 1000 * time_span_days * 24)

# df_plant_factor_pivoted = df_plant_factor.pivot(index=['generator', ])
df_plant_factor.to_excel('plant_factor.xlsx')
a = 4
