import requests
import pandas as pd

url = 'https://bank.gov.ua/NBU_Exchange/exchange_site?start=20230101&end=20231231&sort=exchangedate&order=desc&json'
response = requests.get(url)
data = response.json()

df = pd.DataFrame(data)
print(df.dtypes)
df['calcdate'] = pd.to_datetime(df['calcdate'], format='%d.%m.%Y')
df['rate_per_unit'] = df['rate_per_unit'].astype(float)
print(df.head())
print(df.dtypes)
print(df.isnull().sum())
df['rate_per_unit'] = df.groupby('cc')['rate_per_unit'].transform(lambda x: x.fillna(x.mean()))

print(df.isnull().sum())
df.dropna(inplace=True)
top_currencies = df['cc'].value_counts().head(5).index
df_filtered = df[df['cc'].isin(top_currencies)].copy()
range_df = df_filtered.groupby('cc')['rate_per_unit'].agg(['max', 'min'])
range_df['diff_min_max'] = range_df['max'] - range_df['min']

grouped = df_filtered.merge(range_df['diff_min_max'], on='cc', how='left').groupby('cc')
ga = grouped.agg(
    mean_rate=('rate_per_unit', 'mean'),
    median_rate=('rate_per_unit', 'median'),
    mode_rate=('rate_per_unit', lambda x: x.mode().iloc[0] if not x.mode().empty else None),
    max_rate=('rate_per_unit', 'max'),
    min_rate=('rate_per_unit', 'min')
)
max_mean_currency = ga['mean_rate'].idxmax()
min_mean_currency = ga['mean_rate'].idxmin()
print(f'Валюта з найвищим середнім курсом: {max_mean_currency}')
print(f'Валюта з найнижчим середнім курсом: {min_mean_currency}')
print(ga)
