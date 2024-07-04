import requests
import csv
import pymysql
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import time

def save_to_mysql(data, file):
    try:
        conn = pymysql.connect(host='localhost', port=3306, user='root', password='oneokrock12345', database='jack', charset='utf8mb4')
        
        cursor = conn.cursor()
        cursor.execute('DELETE FROM ubike;')

        for row in data[1:]:  # Skip header row
            try:
                sareaen = row[0]
                sarea = row[1]
                lng = float(row[2])
                sna = row[3]
                snaen = row[4].replace("'", "\\'")
                bemp = int(row[5])
                ar = row[6]
                act = int(row[7])
                sno = int(row[8])
                aren = row[9].replace("'", "\\'")
                tot = int(row[10])
                _id = int(row[11])
                sbi = int(row[12])
                mday = row[13]
                lat = float(row[14])

                cursor.execute(
                    """INSERT INTO ubike(sareaen, sarea, lng, sna, snaen, bemp, ar, act, sno, aren, tot, _id, sbi, mday, lat) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (sareaen, sarea, lng, sna, snaen, bemp, ar, act, sno, aren, tot, _id, sbi, mday, lat)
                )
            except ValueError as ve:
                print(f"Skipping row due to ValueError: {ve}", file=file)
                continue

        conn.commit()
        conn.close()

    except Exception as e:
        print('資料庫連線失敗: ', e, file=file)

def get_ubike_data():
    with open('ubike.log', 'a', encoding='utf-8') as file:
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: 開始更新ubike資料...', file=file)
        response = requests.get('http://data.tycg.gov.tw/api/v1/rest/datastore/a1b4714b-3b75-4ff8-a8f2-cc377e4eaa0f?format=csv&limit=999')
        result = response.text.splitlines()
        result = list(csv.reader(result))

        save_to_mysql(result, file)
        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: 更新ubike資料完成', file=file)

def query(name):
    try:
        conn = pymysql.connect(host='localhost', port=3306, user='root', password='oneokrock12345', database='jack')
        cursor = conn.cursor()
        cursor.execute(f"SELECT sna, bemp, sbi FROM ubike WHERE sna like '%{name}%';")
        
        for i in cursor:
            print(f'名稱: {i[0]}, 可還: {i[1]}, 可借: {i[2]}')

        conn.close()

    except Exception as e:
        print('資料庫連線失敗: ', e)

# 指定時區（一定要指定，否則會失敗）
scheduler = BackgroundScheduler(timezone="Asia/Taipei")

scheduler.add_job(get_ubike_data, 'interval', minutes=1)

scheduler.start()  # 啟動排程

while True:
    print('===================================')
    print('-')
    print('- 桃園市Ubike租借系統')
    print('-')
    print('- 開發者: MR蔣')
    print('- 版本: 0.01')
    print('===================================')

    print('1. 查詢可借、可還')
    print('2. 租借')
    print('Q. 結束系統')
    user_input = input('=> ')

    if user_input == '1':
        user_input = input('請輸入站台名稱(quit=離開): ')
        if user_input.lower() == 'quit':
            continue
        query(user_input)
    elif user_input == '2':
        try:
            conn = pymysql.connect(host='localhost', port=3306, user='root', password='oneokrock12345', database='jack')
            cursor = conn.cursor()
            
            phone = input('請輸入電話號碼: ')
            count = cursor.execute(f"SELECT id FROM `member` m WHERE phone = '{phone}'")
            if count == 0:
                print('查無此會員')
                continue
            else:
                member_id = cursor.fetchone()[0]

            sna = input('請輸入要租借的站台名稱: ')
            count = cursor.execute(f"SELECT sno FROM ubike u WHERE sna = '{sna}';")
            if count == 0:
                print('查無此站台')
                continue
            else:
                sno = cursor.fetchone()[0]

            print(member_id, sno)
            
            conn.close()

        except Exception as e:
           print('資料庫連線失敗: ', e)

    elif user_input.lower() == 'q':
        print('掰掰')
        break
