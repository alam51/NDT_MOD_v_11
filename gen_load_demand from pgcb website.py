import datetime
import pandas as pd
import traceback
# _df = pd.read_html(r'https://erp.pgcb.gov.bd/web/generations/view_generations_bn?page=3')[0]
# _df.to_excel('c.xlsx')


def reformat_df(_df: pd.DataFrame) -> pd.DataFrame:
    for i, _ in enumerate(_df.index):
        date_str = _df.loc[i, 'তারিখ']
        try:
            date_time = pd.to_datetime(date_str, dayfirst=True, yearfirst=False)
        except:
            _df.drop([i], inplace=True)
            continue
        time = _df.loc[i, 'সময়']
        if time.startswith('২৪'):
            date_time += pd.to_timedelta('1 days')
        else:
            hr_min = time.split(':')
            date_time = date_time.replace(hour=int(hr_min[0]), minute=int(hr_min[1]))

        _df.loc[i, 'date_time'] = date_time
    df0 = _df.set_index('date_time').iloc[:, 2:-1]
    df1 = df0.fillna(0)
    df2 = df1.applymap(lambda x: float(x))
    return df2


combined_df = pd.DataFrame()

time_start = datetime.datetime.now()
for i in range(1, 5000):
    try:
        print(i)
        url = f'https://erp.pgcb.gov.bd/web/generations/view_generations_bn?page={i}'
        raw_df = pd.read_html(url)[0]
        if raw_df.empty:
            print("***Completed reading all pages***")
            break
        formatted_df = reformat_df(raw_df)
        combined_df = pd.concat([combined_df, formatted_df])
    except KeyError as e:
        traceback.print_exc()
        continue
    # finally:
a = 4

hourly_data_file_path = 'hourly_gen_demand_loadshed.csv'
combined_df.to_csv(hourly_data_file_path)

df = pd.read_csv(hourly_data_file_path, index_col=0, parse_dates=True)
df.loc[:, 'year'] = df.index.year
df.loc[:, 'month'] = df.index.month
df1 = df.groupby(['year', 'month']).sum()
df1.to_excel('monthly_total_Fuel_wise_gen_dem_LS_MWh.xlsx')
a = 4

print(f'Time taken= {datetime.datetime.now() - time_start}')
# combined_df.to_excel('gen_demand_loadshed.xlsx')
