import os
import pandas as pd
import json

path = os.environ.get('PROJECT_PATH', '.')


def collect_new():
    df1 = []
    df2 = []
    name = ['2022-01-01', '2022-01-02', '2022-01-03', '2022-01-04', '2022-01-05']
    hits = [f'{path}/skillbox_data/extra_data/ga_hits_new_2022-01-01.json',
            f'{path}/skillbox_data/extra_data/ga_hits_new_2022-01-02.json',
            f'{path}/skillbox_data/extra_data/ga_hits_new_2022-01-03.json',
            f'{path}/skillbox_data/extra_data/ga_hits_new_2022-01-04.json',
            f'{path}/skillbox_data/extra_data/ga_hits_new_2022-01-05.json']
    for filename in hits:
        with open(filename) as json_file:
            data = json.load(json_file)
            for n in name:
                if n in data:
                    df = pd.DataFrame.from_dict(data[n])
                    df1.append(df)
    df1 = pd.concat(df1)

    sessions = [f'{path}/skillbox_data/extra_data/ga_sessions_new_2022-01-01.json',
                f'{path}/skillbox_data/extra_data/ga_sessions_new_2022-01-02.json',
                f'{path}/skillbox_data/extra_data/ga_sessions_new_2022-01-03.json',
                f'{path}/skillbox_data/extra_data/ga_sessions_new_2022-01-04.json',
                f'{path}/skillbox_data/extra_data/ga_sessions_new_2022-01-05.json']
    for filename in sessions:
        with open(filename) as json_file:
            data = json.load(json_file)
            for n in name:
                if n in data:
                    df = pd.DataFrame.from_dict(data[n])
                    df2.append(df)
    df2 = pd.concat(df2)
    df = pd.merge(left=df1, right=df2, on='session_id', how='inner')

    df.to_csv(f'{path}/skillbox_data/new_data.csv', sep=',', index=False)


if __name__ == '__main__':
    collect_new()
