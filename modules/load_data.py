import os
import pandas as pd
import MySQLdb

path = os.environ.get('PROJECT_PATH', '.')


def load_data():
    data = pd.read_csv(f'{path}/skillbox_data/main_data.csv', low_memory=False)
    mydb1 = MySQLdb.connect(
        host="localhost",
        user="root",
        password="1FW$d4s5cvhTF",
        database="sber_data"
    )
    mydb2 = MySQLdb.connect(
        host="localhost",
        user="root",
        password="1FW$d4s5cvhTF",
        database="sber_data"
    )
    cursor1 = mydb1.cursor()
    cursor1.execute("""
            CREATE TABLE IF NOT EXISTS analytics (
                session_id VARCHAR(100),
                hit_date VARCHAR(100),
                hit_time FLOAT,
                hit_number INT,
                hit_type VARCHAR(100),
                hit_referer VARCHAR(100),
                hit_page_path VARCHAR(2000),
                event_category VARCHAR(100),
                event_action VARCHAR(100),
                event_label VARCHAR(100),
                event_value FLOAT,
                client_id VARCHAR(100),
                visit_date VARCHAR(100),
                visit_time VARCHAR(100),
                visit_number INT,
                utm_source VARCHAR(100),
                utm_medium VARCHAR(100),
                utm_campaign VARCHAR(100),
                utm_adcontent VARCHAR(100),
                utm_keyword VARCHAR(100),
                device_category VARCHAR(100),
                device_os VARCHAR(100),
                device_brand VARCHAR(100),
                device_model VARCHAR(100),
                device_screen_resolution VARCHAR(100),
                device_browser VARCHAR(100),
                geo_country VARCHAR(100),
                geo_city VARCHAR(100)
            );
            TRUNCATE TABLE analytics;
        """
                   )
    result1 = cursor1.fetchall()
    cursor1.close()
    mydb1.close()

    cursor2 = mydb2.cursor()
    for i, row in data.iterrows():
        sql = "INSERT INTO analytics(session_id, hit_date, hit_time, hit_number, hit_type, \
              hit_referer, hit_page_path, event_category, event_action, event_label, event_value, client_id, \
              visit_date, visit_time, visit_number, utm_source, utm_medium, utm_campaign, utm_adcontent, \
              utm_keyword, device_category, device_os, device_brand, device_model, device_screen_resolution, \
              device_browser, geo_country, geo_city ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        cursor2.execute(sql, tuple(row))
    result2 = cursor2.fetchall()
    cursor2.close()
    mydb2.close()
    print("Record inserted")


if __name__ == '__main__':
    load_data()
