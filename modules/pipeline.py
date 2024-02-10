import os
import json
import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer
from sklearn.preprocessing import StandardScaler

path = os.environ.get('PROJECT_PATH', '.')


def filter_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    columns_to_drop = ['event_label', 'event_value', 'device_model', 'utm_keyword',
                       'hit_time', 'device_os', 'hit_referer', 'device_brand']
    df = df.drop(columns_to_drop, axis=1)
    df = df[~df.utm_adcontent.isna()]
    df = df[~df.utm_campaign.isna()]
    df = df[~df.utm_source.isna()]
    return df


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['hit_date'] = pd.to_datetime(df['hit_date'], format='%Y-%m-%d', utc=True)
    df['visit_datetime'] = df['visit_date'] + " " + df['visit_time']
    df['visit_datetime'] = pd.to_datetime(df['visit_datetime'], format='%Y-%m-%d %H:%M:%S', utc=True)
    df['client_id'] = df['client_id'].astype(object)
    return df


def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    def calculate_outliers(data):
        q25 = data.quantile(0.25)
        q75 = data.quantile(0.75)
        iqr = q75 - q25
        boundaries = (q25 - 1.5 * iqr, q75 + 1.5 * iqr)
        return boundaries

    df = df.copy()
    boundaries_hit_number = calculate_outliers(df['hit_number'])
    df.loc[df['hit_number'] < boundaries_hit_number[0], 'hit_number'] = round(boundaries_hit_number[0])
    df.loc[df['hit_number'] > boundaries_hit_number[1], 'hit_number'] = round(boundaries_hit_number[1])
    boundaries_visit_number = calculate_outliers(df['visit_number'])
    df.loc[df['visit_number'] < boundaries_visit_number[0], 'visit_number'] = round(boundaries_visit_number[0])
    df.loc[df['visit_number'] > boundaries_visit_number[1], 'visit_number'] = round(boundaries_visit_number[1])
    return df


def standardization(df: pd.DataFrame):
    df = df.copy()
    data = df[['hit_number', 'visit_number']]
    std_scaler = StandardScaler()
    std_scaler.fit(data)
    std_scaled = std_scaler.transform(data)
    hit_number_std = std_scaled[0]
    visit_number_std = std_scaled[1]
    df[['hit_number_std', 'visit_number_std']] = std_scaled
    return df


def pipeline() -> None:
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

    preprocessor = Pipeline(steps=[
        ('filter', FunctionTransformer(filter_data)),
        ('normalize', FunctionTransformer(normalize)),
        ('outlier_remover', FunctionTransformer(remove_outliers)),
        ('standardization', FunctionTransformer(standardization))
    ])

    transformed = preprocessor.fit_transform(df)
    transformed_df = pd.DataFrame(transformed)

    transformed_df.to_csv(f'{path}/skillbox_data/new_data.csv', sep=',', index=False)


if __name__ == '__main__':
    pipeline()
