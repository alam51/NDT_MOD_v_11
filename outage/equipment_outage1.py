import pandas as pd
from utils import CONNECTOR
import traceback

from_datetime = '2023-07-01 00:00'
to_datetime = '2023-07-31 23:00'
query_str = f"""
SELECT T1.*, tr.id AS tr_id, l.id AS l_id, tl.id AS tl_id FROM (
SELECT c.name AS 'circle', g.name AS 'gmd', z.name AS 'zone', s.id AS ss_id, s.name AS 'substation', 
se.id as 'eq_id', se.name AS 'equipment', se.is_bus, se.is_transformer, 
se.is_line, e.event_info, e.date_time, e.is_trip, e.is_scheduled, e.is_forced, e.is_project_work,
 (e.is_trip + e.is_scheduled + e.is_forced + e.is_project_work) AS outage_status_sum,  e.is_restored, 
 e.mw_interruption
FROM
ois.event AS e
JOIN ois.substation_equipment AS se ON e.sub_equip_id = se.id
JOIN substation AS s ON s.id = se.substation_id
JOIN gmd AS g ON s.gmd = g.id
JOIN circle AS c ON c.id = g.circle_id
JOIN ois.`zone` AS z ON z.id = s.`zone`
WHERE 
(e.date_time BETWEEN '{from_datetime}' AND '{to_datetime}')
AND (se.is_line = 1 OR se.is_transformer = 1 OR se.is_bus = 1)
GROUP BY se.id, e.date_time
ORDER BY c.name, g.name, s.name, se.id, e.date_time
) AS T1

JOIN ois.substation_equipment AS se ON T1.eq_id = se.id
LEFT JOIN ois.transformer AS tr ON se.transformer_id = tr.id
LEFT JOIN ois.line AS l ON se.line_id = l.id
LEFT JOIN ois.transmission_line AS tl ON l.tl_id = tl.id
"""

df = pd.read_sql_query(query_str, CONNECTOR, index_col=['date_time', 'eq_id'])

for i, (date_time, eq_id) in enumerate(df.index):
    try:
        if df.loc[(date_time, eq_id), 'outage_status_sum'] == 1 and df.loc[df.index[i + 1], 'is_restored'] == 1 \
                and df.index[i][1] == df.index[i+1][1]:
            df.loc[(date_time, eq_id), 'restoration_time'] = df.index[i + 1][0]
    except IndexError:
        print(f'Error in:\n{df.loc[(date_time, eq_id), :]}')
        traceback.format_exc()


a = 8
df1 = df.reset_index(names=['outage_time', 'eq_id'])
df1.loc[:, 'duration'] = df1.loc[:, 'restoration_time'] - df1.loc[:, 'outage_time']
df2 = df1[df1.outage_status_sum == 1]
a = 5

with pd.ExcelWriter('outage_summary.xlsx') as writer:
    df.to_excel(writer, sheet_name='raw')
    df2.to_excel(writer, sheet_name='processed')

