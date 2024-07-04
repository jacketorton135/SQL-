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

        for row in data[1:]:  # Skip the header row
            cursor.execute(
                """INSERT INTO ubike(sareaen, sarea, lng, sna, snaen, bemp, ar, act, sno, aren, tot, _id, sbi, mday, lat) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14])
            )
        
        conn.commit()

    except Exception as e:
        print('資料庫連線失敗: ', e, file=file)
    finally:
        cursor.close()
        conn.close()

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
        conn = pymysql.connect(host='localhost', port=3306, user='root', password='oneokrock12345', database='jack', charset='utf8mb4')
        cursor = conn.cursor()
        cursor.execute("SELECT sna, bemp, sbi FROM ubike WHERE sna LIKE %s;", ('%' + name + '%',))
        
        for i in cursor:
            print(f'名稱: {i[0]}, 可還: {i[1]}, 可借: {i[2]}')

    except Exception as e:
        print('資料庫連線失敗: ', e)
    finally:
        cursor.close()
        conn.close()

# 指定時區（一定要指定，否則會失敗）
scheduler = BackgroundScheduler(timezone="Asia/Taipei")

scheduler.add_job(get_ubike_data, 'interval', minutes=1)

scheduler.start()  # 啟動排程

while True:
    try:
        user_input
    except:
        print('===================================')
        print('-')
        print('- 桃園市Ubike租借系統')
        print('-')
        print('- 開發者: Aaron Ho')
        print('- 版本: 0.01')
        print('===================================')

        print('1. 查詢可借、可還')
        print('2. 租借')
        print('3. 還車')
        print('4. 查詢租借紀錄')
        print('Q. 結束系統')

    user_input = input('=> ')

    if user_input == '1':
        user_input = input('請輸入站台名稱(quit=離開): ')
        if user_input.lower() == 'quit':
            continue
        query(user_input)
    elif user_input == '2':
        try:
            conn = pymysql.connect(host='localhost', port=3306, user='root', password='oneokrock12345', database='jack', charset='utf8mb4')
            cursor = conn.cursor()
            
            phone = input('請輸入電話號碼: ')
            count = cursor.execute("SELECT id FROM `member` m WHERE phone = %s", (phone,))
            if count == 0:
                print('查無此會員')
                continue
            else:
                member_id = cursor.fetchone()[0]

            sna = input('請輸入要租借的站台名稱: ')
            count = cursor.execute("SELECT sno FROM ubike u WHERE sna = %s;", (sna,))
            if count == 0:
                print('查無此站台')
                continue
            else:
                sno = cursor.fetchone()[0]

            cursor.execute("INSERT INTO rent_history(sno, member_id, rent_time) VALUES(%s, %s, %s);", (sno, member_id, datetime.now()))

            conn.commit()
        
            print(f'會員:{phone}({member_id}), 租借: {sna}({sno})站台成功')

        except Exception as e:
            print('資料庫連線失敗: ', e)
        finally:
            cursor.close()
            conn.close()
    elif user_input == '3':
        try:
            conn = pymysql.connect(host='localhost', port=3306, user='root', password='oneokrock12345', database='jack', charset='utf8mb4')
            cursor = conn.cursor()
            
            phone = input('請輸入電話號碼:')
            
            sql = """SELECT id FROM rent_history rh
                     WHERE member_id = (SELECT id FROM `member` m WHERE phone = %s)
                     AND return_time IS NULL
                     ORDER BY rent_time DESC
                     LIMIT 1;"""
            
            count = cursor.execute(sql, (phone,))
            
            if count == 0:
                print('沒有租借紀錄')
                continue
            
            # 取得租借紀錄id
            rent_id = cursor.fetchone()[0]

            cursor.execute("UPDATE rent_history SET return_time = NOW() WHERE id = %s;", (rent_id,))
            conn.commit()

            print('還車成功.')

        except Exception as e:
            print('資料庫連線失敗:', e)
        finally:
            cursor.close()
            conn.close()
    elif user_input == '4':
        try:
            conn = pymysql.connect(host='localhost', port=3306, user='root', password='oneokrock12345', database='jack', charset='utf8mb4')
            cursor = conn.cursor()
            
            phone = input('請輸入電話號碼:')
            
            sql = """SELECT m.id, m.name, m.phone, u.sna, rh.rent_time, rh.return_time 
                     FROM rent_history rh
                     JOIN `member` m ON rh.member_id = m.id
                     JOIN ubike u ON rh.sno = u.sno
                     WHERE m.phone = %s;"""
            
            count = cursor.execute(sql, (phone,))
            
            if count == 0:
                print('無租借紀錄')
                continue
            
            for row in cursor:
                print(f'會員ID: {row[0]}, 姓名:{row[1]}, 電話:{row[2]}, 站台:{row[3]}, 租車時間:{row[4]}, 還車時間:{row[5]}')

        except Exception as e:
            print('資料庫連線失敗: ', e)
        finally:
            cursor.close()
            conn.close()
    elif user_input.lower() == 'q':
        print('掰掰')
        break
