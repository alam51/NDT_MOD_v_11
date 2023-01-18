import datetime

import numpy as np
from pandas import ExcelWriter
from utils import CONNECTOR
import pandas as pd
import matplotlib.pyplot as plt


def transformer_max_flow(from_datetime: str, to_datetime: str) -> pd.DataFrame:
    _from_datetime = from_datetime
    _to_datetime = to_datetime
    query_str = f"""
    -- SET GLOBAL Innodb_buffer_pool_size = 5168709120;
    
    SELECT T_max.*, T.date_time FROM 
    (
    SELECT r.name AS 'region', z.name AS 'zone', c.name AS 'circle', g.name AS 'gmd',
    s.name AS 'substation', tr_type.voltage_level, tr.id AS 'tr_id', tr.name AS 'transformer', tr_mva.mva_rating,
    MAX(abs(mw.value)) AS max_mw
     FROM
    ois.transformer AS tr
    JOIN ois.transformer_type AS tr_type ON tr_type.id = tr.type_id
    JOIN ois.transformer_mva AS tr_mva ON tr.mva_id = tr_mva.id
    JOIN ois.substation_equipment AS se ON se.transformer_id = tr.id
    JOIN ois.mega_watt AS mw ON mw.sub_equip_id = se.id
    JOIN ois.substation AS s ON s.id = se.substation_id
    JOIN gmd AS g ON g.id = s.gmd
    JOIN ois.circle AS c ON c.id = g.circle_id
    JOIN ois.zone AS z ON z.id = s.zone
    JOIN ois.region AS r ON r.id = z.region_id
    WHERE mw.date_time BETWEEN '{_from_datetime}' AND '{_to_datetime}'
    GROUP BY tr.id
    ) AS T_max
    
    LEFT JOIN 
    
    (
    SELECT tr.id AS 'tr_id', abs(mw.value) AS mw, mw.date_time
    FROM
    ois.transformer AS tr
    JOIN ois.substation_equipment AS se ON se.transformer_id = tr.id
    JOIN ois.mega_watt AS mw ON mw.sub_equip_id = se.id
    WHERE mw.date_time BETWEEN '{_from_datetime}' AND '{_to_datetime}'
    -- GROUP BY tr.id
    ) AS T
    
    ON T_max.max_mw = T.mw AND T_max.tr_id = T.tr_id
    GROUP BY T_max.tr_id
    ORDER BY 1,2,3,4,5,6,8,9
    
    """

    df = pd.read_sql_query(query_str, CONNECTOR)
    mva_rating_float = df.loc[:, 'mva_rating'].apply(lambda x: float(x.split('/')[-1]))
    df.loc[:, 'mva_rating_fl'] = mva_rating_float
    df.loc[:, '% loading'] = df.loc[:, 'max_mw'] / df.loc[:, 'mva_rating_fl']
    return df
