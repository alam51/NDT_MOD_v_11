import pandas as pd

file_path = r'gen_demand_loadshed.csv'

i = 1600
url = f'https://erp.pgcb.gov.bd/web/generations/view_generations_bn?page={i}'
raw_df = pd.read_html(url)[0]

df = pd.read_csv(file_path, index_col=0, parse_dates=True)
df.loc[:, 'year'] = df.index.year
df.loc[:, 'month'] = df.index.month
df1 = df.groupby(['year', 'month']).sum()
df1.to_excel('monthly_total_Fuel_wise_gen_dem_LS_MWh.xlsx')
a = 4
