import pandas as pd


def days_backfilled(df1, df2, date_column, ID):
    # Ensure NAV_DATE columns are datetime for accurate processing
    df1[date_column] = pd.to_datetime(df1[date_column])
    df2[date_column] = pd.to_datetime(df2[date_column])

    # Count the number of rows for each SCHEME_ID in both dataframes
    df1_counts = df1.groupby(ID).size().reset_index(name='df1_count')
    df2_counts = df2.groupby(ID).size().reset_index(name='df2_count')

    # Merge the counts dataframes to compute the difference
    counts = pd.merge(df1_counts, df2_counts, on=ID, how='outer').fillna(0)
    counts['days_backfilled'] = counts['df2_count'] - counts['df1_count']

    # Create the resulting dataframe
    del counts['df2_count']
    del counts['df1_count']
    return counts


def missing_days(df, date_column, ID):
    total_missing_days = []

    for id, group in df.groupby(ID):
        group = group.sort_values(date_column)
        full_date_range = pd.date_range(start=group[date_column].min(), end=group[date_column].max(), freq='D')
        no_missing_days = len(full_date_range) - len(group)
        total_missing_days.append({ID: id, 'no_missing_days': no_missing_days})  # Note the column name here
    return pd.DataFrame(total_missing_days)


def find_missing_dates(df, ID, date_column):
    df[date_column] = pd.to_datetime(df[date_column])

    def group_missing_dates(group):
        group = group.sort_values(date_column)
        date_set = set(group[date_column])
        all_dates = pd.date_range(start=group[date_column].min(), end=group[date_column].max())
        missing_dates = [date for date in all_dates if date not in date_set]
        return pd.Series({'missing_dates': missing_dates})

    return df.groupby(ID, group_keys=False).apply(group_missing_dates).reset_index()

def compare(df1, df2, col1, col2, ID):
    # Merge the two dataframes
    comparison = pd.merge(df1, df2, on=ID, how='outer')

    # Add a column to check if the values are equal
    comparison['is_equal'] = comparison[col1] == comparison[col2]

    # Count the number of matches and mismatches
    total_count = len(comparison)
    match_count = comparison['is_equal'].sum()
    mismatch_count = total_count - match_count

    print(f"Total schemes compared: {total_count}")
    print(f"Matching schemes: {match_count}")
    print(f"Mismatching schemes: {mismatch_count}")

    # Return the comparison dataframe for further analysis if needed
    return comparison


# Main execution
indf = pd.read_csv('backfilled-nav (1).csv')
outdf = pd.read_csv('output.csv')

n_backfilled = days_backfilled(indf, outdf, 'NAV_DATE', 'SCHEME_ID')
n_missing = missing_days(indf, 'NAV_DATE', 'SCHEME_ID')

# Perform the comparison
comparison_result = compare(n_backfilled, n_missing, 'days_backfilled', 'no_missing_days', 'SCHEME_ID')  # Changed 'missing_days' to 'no_missing_days'

missing_dates_df = find_missing_dates(indf, 'SCHEME_ID', 'NAV_DATE')
comparison_result = pd.merge(comparison_result, missing_dates_df, on='SCHEME_ID', how='left')

# Optionally, save the comparison result to a CSV file
comparison_result.to_csv('comparison_result.csv', index=False)
