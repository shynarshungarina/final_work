import os
import pandas as pd
import MySQLdb

path = os.environ.get('PROJECT_PATH', '.')


def load():
    mydb = MySQLdb.connect(
        host="localhost",
        user="root",
        password="1FW$d4s5cvhTF",
        database="sber_data"
    )
    cursor = mydb.cursor()
    new_data = pd.read_csv(f'{path}/skillbox_data/new_data.csv')

    for i, row in new_data.iterrows():
        sql = "INSERT INTO ga_data(session_id, hit_date, hit_number, hit_type, hit_page_path, \
              event_category, event_action, client_id, visit_date, visit_time, visit_number, utm_source, \
              utm_medium, utm_campaign, utm_adcontent, device_category, device_screen_resolution, device_browser, \
              geo_country, geo_city, visit_datetime, hit_number_std, visit_number_std ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor.execute(sql, tuple(row))

    mydb.commit()
    cursor.close()
    print("Record inserted")


if __name__ == '__main__':
    load()
