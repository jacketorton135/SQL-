import requests
import csv
import pymysql

def save_to_mysql(data):
    try:
        conn = pymysql.connect(host='localhost', port=3306, user='root', password='oneokrock12345', database='jack')
        
        cursor = conn.cursor()
        cursor.execute('DELETE FROM ubike;')

        for row in data:
            cursor.execute(f"INSERT INTO ubike(sareaen,sarea,lng,sna,snaen,bemp,ar,act,sno,aren,tot,_id,sbi,mday,lat) VALUES ('Zhongli Dist.','中壢區',121.194666,'{row[3]}','National Central University Library',5,'中大路300號(中央大學校內圖書館前)',1,{row[8]},'No.300, Zhongda Rd.',14,1,8,20240702104429,24.968128);")

        # for i in cursor:
        #     print(i)
        conn.commit()

    except Exception as e:
        print('資料庫連線失敗: ', e)

def get_ubike_data():
    response = requests.get('http://data.tycg.gov.tw/api/v1/rest/datastore/a1b4714b-3b75-4ff8-a8f2-cc377e4eaa0f?format=csv&limit=999')

    result = response.text.splitlines()

    result = list(csv.reader(result))
    
    print(result)

    save_to_mysql(result)

# save_to_mysql('')

get_ubike_data()