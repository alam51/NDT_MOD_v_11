from utils import LDD_CONNECTOR as CONNECTOR
import pandas as pd

start_datetime_str = '2022-1-1'
end_datetime_str = '2022-12-31'

# start_date = pd.to_datetime(start_date_str)
# end_date = pd.to_datetime(end_date_str)
# time_span = end_date - start_date
# time_span_days = time_span.days

ampere_query_str = f"""
SELECT * FROM
pgcbfinal.generation_unit_status AS s
JOIN pgcbfinal.generation_unit AS g ON s.generation_unit_id = g.id
JOIN pgcbfinal.ps_and_unit_status_type AS st ON st.id = s.status_type_id
-- WHERE LOWER(g.name) LIKE '%sylhet%225%' 
AND (s.start_date_time BETWEEN '{start_datetime_str}' AND '{end_datetime_str}')
"""

df_gen_unit_outage = pd.read_sql(ampere_query_str, CONNECTOR)
a = 4
