import pandas as pd
import csv

data_file = 'micro-master-equity.csv'
data = pd.read_csv(data_file)
n = data.shape[0]

flagged_dates = []


def apply_limits(df, param, hgt, hlt):
    flagged = []
    for i in range(df.shape[0]):
        row = df.iloc[i, :]
        error = ''
        if row[param] < hgt:
            error = f'{param} {row[param]} < {hgt}'

        if row[param] > hlt:
            error = f'{param} {row[param]} > {hlt}'

        if error != '':
            flagged.append({
                'date': row['DATE'],
                'value': row[param],
                'param': param,
                'fund': row['FUND_NAME'],
                'error': error
            })
    global flagged_dates
    for i in flagged:
        flagged_dates.append(i)
    return flagged


def backfill(fund_name, param, hgt, hlt):
    global data
    fund_data = data[data['FUND_NAME'] == fund_name].copy()
    fund_data['DATE'] = pd.to_datetime(fund_data['DATE'], errors='coerce', dayfirst=False)
    flagged_months = sorted(apply_limits(fund_data, param, hgt, hlt),
                            key=lambda x: pd.to_datetime(x['date'], dayfirst=False))

    for f in flagged_months:

        current_date = pd.to_datetime(f['date'], dayfirst=False)
        prior_months = fund_data[fund_data['DATE'] < current_date]
        if not prior_months.empty:
            most_recent = prior_months['DATE'].max()
            most_recent_value = fund_data.loc[fund_data['DATE'] == most_recent, param].values[0]
            fund_data.loc[fund_data['DATE'] == current_date, param] = most_recent_value

    return fund_data


funds = data['FUND_NAME'].unique()

rules = [('AUM_MCAP', 0.00000005, 0.002), ('TOP_10', 0.1, 0.9), ('NO_OF_STOCKS', 10, 502),
         ('SALES_GROWTH', -0.3, 0.5), ('BV_GROWTH', -0.45, 0.5), ('CF_GROWTH', -0.45, 0.725),
         ('HIST_EARN_GR', -0.4, 0.7), ('FORW_EARN_GR', -0.0002, 0.6), ('DIV_YIELD', 0.002, 0.05),
         ('BOOK_PRICE', 0.01, 1.2), ('PRICE_TO_CF', 0.5, 45), ('PRICE_TO_SALES', 0.25, 20),
         ('PE_RATIO', 2, 45), (('LARGE_CAP', 'MID_CAP', 'SMALL_CAP'), 0.8, 1.05),
         (('CYCLICAL', 'DEFENSIVE', 'SENSITIVE'), 1, 1), (('GROWTH','VALUE'), 0, 1)]  # structure: (CRITERION, LOWER, UPPER)
                                                                                      # ((criteria 1, 2..), LOWER SUM, UPPER SUM)

updated_data = pd.DataFrame()

for fund in funds:

    print(fund)
    fund_data = data[data['FUND_NAME'] == fund]

    for r in rules:
        print(f'{fund}: {r[0]}')
        if type(r[0]) is tuple:

            rule_name = '_'.join(r[0])

            x = fund_data.loc[:, r[0][0]].copy()
            fund_data.loc[:, rule_name] = x
            x = pd.to_datetime(fund_data['DATE'], errors='coerce', dayfirst=False).copy()
            fund_data.loc[:, 'DATE2'] = x

            for p in r[0][1:]:
                fund_data.loc[:, rule_name] += fund_data.loc[:, p]

            flagged_months = sorted(apply_limits(fund_data, rule_name, r[1], r[2]),
                                    key=lambda i: pd.to_datetime(i['date'], dayfirst=False))

            for f in flagged_months:

                current_date = pd.to_datetime(f['date'], dayfirst=False)
                prior_months = fund_data[fund_data['DATE2'] < current_date]

                for param in r[0]:
                    if not prior_months.empty:
                        most_recent = prior_months['DATE2'].max()
                        most_recent_value = fund_data.loc[fund_data['DATE2'] == most_recent, param].values[0]
                        fund_data.loc[fund_data['DATE2'] == current_date, param] = most_recent_value

            del fund_data['DATE2']
            del fund_data[rule_name]

        else:
            ud = backfill(fund, r[0], r[1], r[2])
            fund_data.loc[:, r[0]] = ud.loc[:, r[0]]

    updated_data = pd.concat([updated_data, fund_data])

updated_data.to_csv('updated_data.csv', encoding='utf-8')

keys = flagged_dates[0].keys()

with open('flagged.csv', 'w', newline='') as output_file:
    dict_writer = csv.DictWriter(output_file, fieldnames=keys)
    dict_writer.writeheader()
    dict_writer.writerows(flagged_dates)
