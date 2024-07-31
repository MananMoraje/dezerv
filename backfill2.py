import pandas as pd
from datetime import datetime
from multiprocessing import Process, Queue
import time
import queue


def read_csv(file):
    df = pd.read_csv(file)
    df['NAV_DATE'] = pd.to_datetime(df['NAV_DATE'])
    return df

"""
def func(l, Q, df):
    for scheme_id in l:
        scheme_data = df[df['SCHEME_ID'] == scheme_id]

        if 'BACKFILLED_AT' in scheme_data.columns:
            start_date = scheme_data.loc[scheme_data['BACKFILLED_AT'].notna(), 'NAV_DATE'].max()
        else:
            start_date = scheme_data['NAV_DATE'].min()

        full_date_range = pd.date_range(start=start_date, end=scheme_data['NAV_DATE'].max(), freq='D')
        scheme_data = scheme_data.set_index('NAV_DATE').reindex(full_date_range).rename_axis('NAV_DATE').reset_index()

        scheme_data['NAV_VALUE'] = scheme_data['NAV_VALUE'].bfill()
        scheme_data['MF_HISTORICAL_MONGO_ID'] = scheme_data['MF_HISTORICAL_MONGO_ID'].bfill()
        scheme_data['ISIN'] = scheme_data['ISIN'].bfill()
        scheme_data['NAV_DATE_STR'] = scheme_data['NAV_DATE'].dt.strftime('%Y-%m-%d')
        scheme_data['DOCUMENT_UPDATED_AT'] = scheme_data['DOCUMENT_UPDATED_AT'].bfill()
        scheme_data['SCHEME_ID'] = scheme_id
        scheme_data['BACKFILLED_AT'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S').split()[0]

        print(f'Adding scheme_id {scheme_id} to queue')
        Q.put(scheme_data)
        print(f'Added scheme_id {scheme_id} to queue')

    # Signal that this process is done
    Q.put(None)
"""


def func(l, Q, df):
    for scheme_id in l:
        scheme_data = df[df['SCHEME_ID'] == scheme_id]

        if 'BACKFILLED_AT' in scheme_data.columns:
            start_date = scheme_data.loc[scheme_data['BACKFILLED_AT'].notna(), 'NAV_DATE'].max()
        else:
            start_date = scheme_data['NAV_DATE'].min()

        end_date = scheme_data['NAV_DATE'].max()
        full_date_range = pd.date_range(start=start_date, end=end_date, freq='D')  # Include all days

        scheme_data = scheme_data.set_index('NAV_DATE').reindex(full_date_range).rename_axis('NAV_DATE').reset_index()

        scheme_data['NAV_VALUE'] = scheme_data['NAV_VALUE'].bfill(axis='rows')
        scheme_data['MF_HISTORICAL_MONGO_ID'] = scheme_data['MF_HISTORICAL_MONGO_ID'].bfill(axis='rows')
        scheme_data['ISIN'] = scheme_data['ISIN'].bfill(axis='rows')
        scheme_data['NAV_DATE_STR'] = scheme_data['NAV_DATE'].dt.strftime('%Y-%m-%d')
        scheme_data['DOCUMENT_UPDATED_AT'] = scheme_data['DOCUMENT_UPDATED_AT'].bfill(axis='rows')
        scheme_data['SCHEME_ID'] = scheme_id
        scheme_data['BACKFILLED_AT'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S').split()[0]

        print(f'Adding scheme_id {scheme_id} to queue')
        Q.put(scheme_data)

    # Signal that this process is done
    Q.put(None)


def process_queue(q, final_result):
    while True:
        try:
            data = q.get(timeout=0.2)  # Wait for 1 second
            if data is None:
                print('Data is none')
                break
            final_result = pd.concat([final_result, data])
            print('Concatted data to result')
        except queue.Empty:
            print('Queue is empty')
            continue
    return final_result


def main():
    print('Entered main')
    file = 'backfilled-nav (1).csv'
    df = read_csv(file)

    q = Queue()
    df_scheme_ids = df['SCHEME_ID'].unique()

    chunks = [list(df_scheme_ids[i:i + 500]) for i in range(0, len(df_scheme_ids), 500)]
    print(f"Number of chunks: {len(chunks)}")

    processes = []
    for chunk in chunks:
        print('Starting new process')
        p = Process(target=func, args=(chunk, q, df))
        processes.append(p)
        p.start()

    print('All processes started')

    final_result = pd.DataFrame()
    completed_processes = 0

    while completed_processes < len(processes):
        final_result = process_queue(q, final_result)

        # Check if any processes have completed
        for p in processes:
            if not p.is_alive():
                p.join()
                completed_processes += 1
                print(f'Process completed. {len(processes) - completed_processes} remaining.')

        time.sleep(0.1)  # Small delay to prevent busy waiting

    print('All processes completed')

    # Process any remaining items in the queue
    while not q.empty():
        final_result = process_queue(q, final_result)

    final_result = final_result.reset_index(drop=True)
    final_result.to_csv('output.csv', index=False)

    print('Output saved to CSV')


if __name__ == '__main__':

    main()
