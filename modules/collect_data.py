import os
import pandas as pd

path = os.environ.get('PROJECT_PATH', '.')


def collect_data():
    df1 = pd.read_csv(f'{path}/skillbox_data/ga_hits-001.csv')
    df2 = pd.read_csv(f'{path}/skillbox_data/ga_sessions.csv', low_memory=False)
    df = pd.merge(left=df1, right=df2, on='session_id', how='inner')
    df = df.fillna(0)

    df.to_csv(f'{path}/skillbox_data/main_data.csv', sep=',', index=False)


if __name__ == '__main__':
    collect_data()
