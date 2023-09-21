import pandas as pd
from utils import CONNECTOR
import traceback

from_datetime = '2023-08-01 00:00'
to_datetime = '2023-08-31 23:00'

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
                and df.index[i][1] == df.index[i + 1][1]:
            df.loc[(date_time, eq_id), 'restoration_time'] = df.index[i + 1][0]
    except IndexError:
        print(f'Error in:\n{df.loc[(date_time, eq_id), :]}')
        traceback.format_exc()

a = 8
df1 = df.reset_index(names=['outage_time', 'eq_id'])
df1.loc[:, 'duration'] = df1.loc[:, 'restoration_time'] - df1.loc[:, 'outage_time']

df2 = df1[df1.outage_status_sum == 1]
outage_type_series = [pd.NA for i in df2.index]
df2.loc[:, 'outage_type'] = outage_type_series
for _, i in enumerate(df2.index):
    if df2.loc[i, 'is_trip'] == 1:
        df2.loc[i, 'outage_type'] = 'T'
    elif df2.loc[i, 'is_forced'] == 1:
        df2.loc[i, 'outage_type'] = 'E/O'
    elif df2.loc[i, 'is_scheduled'] == 1:
        df2.loc[i, 'outage_type'] = 'S/O'
    elif df2.loc[i, 'is_project_work'] == 1:
        df2.loc[i, 'outage_type'] = 'D/W'
# _df2 = df2.loc['circle', 'gmd', 'substation', 'equipment', ]
"""*************************************Bus Part Start*************************************"""
df_bus_raw = df2[df2['is_bus'] == 1]
df_bus = df_bus_raw.loc[:, ['circle', 'substation', 'equipment', 'outage_time', 'restoration_time',
                          'duration', 'outage_type', 'event_info','mw_interruption', 'is_trip', 'is_forced',
                            'is_scheduled', 'is_project_work']]

"""*************************************Transformer Part Start*************************************"""
df_tr_raw = df2[df2['is_transformer'] == 1]

df_tr = df_tr_raw.loc[:, ['circle', 'substation', 'equipment', 'outage_time', 'restoration_time',
                          'duration', 'outage_type', 'event_info','mw_interruption', 'is_trip', 'is_forced',
                            'is_scheduled', 'is_project_work', 'tr_id']]
_df_tr = df_tr.sort_values(by=['circle', 'substation', 'tr_id', 'outage_time'])
_df_tr.loc[:, 'is_repeat'] = [False for i in range(len(_df_tr))]  # At beginning declare all rows as no repeat
for i, idx in enumerate(_df_tr.index[:-2]):
    current_tr_id = _df_tr.loc[idx, 'tr_id']
    next_tr_id = _df_tr.loc[_df_tr.index[i + 1], 'tr_id']
    current_outage_time = _df_tr.loc[idx, 'outage_time']
    current_restoration_time = _df_tr.loc[idx, 'restoration_time']
    next_outage_time = _df_tr.loc[_df_tr.index[i + 1], 'outage_time']
    next_restoration_time = _df_tr.loc[_df_tr.index[i + 1], 'restoration_time']
    current_time_in_next_time_range = ((next_outage_time <= current_outage_time <= next_restoration_time) or
                                       (next_outage_time <= current_restoration_time <= next_restoration_time))
    next_time_in_current_time_range = ((current_outage_time <= next_outage_time <= current_restoration_time) or
                                       (current_outage_time <= next_restoration_time <= current_restoration_time))

    if (not _df_tr.loc[idx, 'is_repeat']) and current_tr_id == next_tr_id and \
            (current_time_in_next_time_range or next_time_in_current_time_range):
        if _df_tr.loc[idx, 'mw_interruption'] < _df_tr.loc[_df_tr.index[i + 1], 'mw_interruption']:
            _df_tr.loc[idx, 'is_repeat'] = True
        else:
            _df_tr.loc[_df_tr.index[i + 1], 'is_repeat'] = True

# df_tr = _df_tr.query("(not 'is_repeat') and ('restoration_time' != pd.NaT)")
df_tr = _df_tr[_df_tr.is_repeat == False].iloc[:, :-2]
"""**********************************Transformer Part End*******************************"""

"""*********************************Line Part Start*************************************"""
df_line_raw = df2[df2['is_line'] == 1]

df_line = df_line_raw.loc[:, ['circle', 'substation', 'equipment', 'outage_time', 'restoration_time',
                          'duration', 'outage_type', 'event_info','mw_interruption', 'is_trip', 'is_forced',
                            'is_scheduled', 'is_project_work', 'tl_id']]
_df_tl = df_line.sort_values(by=['tl_id', 'outage_time'])
_df_tl.loc[:, 'is_repeat'] = [False for i in range(len(_df_tl))]  # At beginning declare all rows as no repeat
for i, idx in enumerate(_df_tl.index[:-2]):
    current_tl_id = _df_tl.loc[idx, 'tl_id']
    next_tl_id = _df_tl.loc[_df_tl.index[i + 1], 'tl_id']
    current_outage_time = _df_tl.loc[idx, 'outage_time']
    current_restoration_time = _df_tl.loc[idx, 'restoration_time']
    next_outage_time = _df_tl.loc[_df_tl.index[i + 1], 'outage_time']
    next_restoration_time = _df_tl.loc[_df_tl.index[i + 1], 'restoration_time']
    current_time_in_next_time_range = ((next_outage_time <= current_outage_time <= next_restoration_time) or
                                       (next_outage_time <= current_restoration_time <= next_restoration_time))
    next_time_in_current_time_range = ((current_outage_time <= next_outage_time <= current_restoration_time) or
                                       (current_outage_time <= next_restoration_time <= current_restoration_time))

    if (not _df_tl.loc[idx, 'is_repeat']) and current_tl_id == next_tl_id and \
            (current_time_in_next_time_range or next_time_in_current_time_range):
        if _df_tl.loc[idx, 'mw_interruption'] < _df_tl.loc[_df_tl.index[i + 1], 'mw_interruption']:
            _df_tl.loc[idx, 'is_repeat'] = True
        else:
            _df_tl.loc[_df_tl.index[i + 1], 'is_repeat'] = True
df_tl1 = _df_tl[_df_tl.is_repeat == False].iloc[:, :-2]
df_tl = df_tl1.sort_values(by=['circle', 'substation', 'equipment', 'outage_time'])
a = 5
"""*********************************Line Part End*************************************"""

_df_equipment = pd.concat([df_bus, df_tr])
df_equipment = _df_equipment.sort_values(by=['circle', 'substation', 'equipment', 'outage_time'])

df_equipment_trip_emergency = df_equipment[(df_equipment['is_trip'] == 1) | (df_equipment['is_forced'] == 1)]
df_equipment_scheduled_project = df_equipment[(df_equipment['is_scheduled'] == 1) | (df_equipment['is_project_work'] == 1)]

df_line_trip_emergency = df_tl[(df_tl['is_trip'] == 1) | (df_tl['is_forced'] == 1)]
df_line_scheduled_project = df_tl[(df_tl['is_scheduled'] == 1) | (df_tl['is_project_work'] == 1)]
a = 5

with pd.ExcelWriter('outage_summary1.xlsx') as writer:
    df2.to_excel(writer, sheet_name=f'outage_master_{from_datetime[:7]}')
    df_equipment_trip_emergency.to_excel(writer, sheet_name='QF-LDC-23')
    df_line_trip_emergency.to_excel(writer, sheet_name='QF-LDC-24')
    df_equipment_scheduled_project.to_excel(writer, sheet_name='QF-LDC-25')
    df_line_scheduled_project.to_excel(writer, sheet_name='QF-LDC-26')
