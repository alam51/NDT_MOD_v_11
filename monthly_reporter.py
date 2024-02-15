import os.path
import traceback

from ewic.ewic_from_LDD import EWIC
from generation.fuelwise_gen_mkwh import fuelwise_gen_mkwh
from generation.fuelwise_gen_mw import fuelwise_gen_mw
from load.substation_wise.max_ss_load_mw_final import max_ss_load_mw
from outage.equipment_outage_final import equipment_outage_final
from transformer.auto_transformer_max1 import max_auto_transformer_load
from voltage.max_min_kv_final import ss_max_min_voltage

start_datetime = '2024-01-01 00:00'
end_datetime = '2024-01-02 00:00'
folder_roksana = r'G:\My Drive\IMD\Monthly_Report\2024\1_Jan\Roksana'
folder_susama = r'G:\My Drive\IMD\Monthly_Report\2024\1_Jan\Susama'

start_date = start_datetime[0:10]
end_date = end_datetime[0:10]

try:
    ewic = EWIC(start_date, end_date, folder_roksana)
except:
    traceback.format_exc()

try:
    fuelwise_gen_mkwh(start_date, end_date, folder_roksana)
    fuelwise_gen_mw(start_datetime, end_datetime, folder_roksana)
except:
    traceback.format_exc()

try:
    equipment_outage_final(start_datetime, end_datetime, folder_roksana)
except:
    traceback.format_exc()

try:
    max_ss_load_df = max_ss_load_mw(start_datetime, end_datetime)
    max_ss_load_df.to_excel(os.path.join(folder_susama, 'max_ss_load.xlsx'))
except:
    traceback.format_exc()

try:
    max_auto_transformer_load_df = max_auto_transformer_load(start_datetime, end_datetime)
    max_auto_transformer_load_df.to_excel(folder_susama, 'max_auto_transformer_load.xlsx')

except:
    traceback.format_exc()

try:
    ss_max_min_voltage(start_datetime, end_datetime, os.path.join(folder_susama, r'max_min_kv.xlsx'))
except:
    traceback.format_exc()
