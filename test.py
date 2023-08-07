import pandas as pd

file_path = r'gen_demand_loadshed.csv'

df = pd.read_csv(file_path, index_col=0, parse_dates=True)
df.loc[:, 'year'] = df.index.year
df.loc[:, 'month'] = df.index.month
df1 = df.groupby(['year', 'month']).sum()
df1.to_excel('monthly_sum_gen_dem_ls.xlsx')
a = 4