#-*-coding:UTF-8-*-

import requests
import json
import psycopg2
import datetime
import time
import hashlib


last_time = int(round(time.time()))*1000

today = datetime.date.today()

yesterday = today - datetime.timedelta(days=1)

# tomorrow = today + datetime.timedelta(days=1)

# acquire = today + datetime.timedelta(days=2)

today_start_time = int(round(time.mktime(time.strptime(str(yesterday), '%Y-%m-%d'))))*1000
print today_start_time

yesterday_end_time = (int(round(time.mktime(time.strptime(str(today), '%Y-%m-%d')))) - 1)*1000
print yesterday_end_time
# today_start_time = yesterday_end_time + 1

# today_end_time = int(time.mktime(time.strptime(str(tomorrow), '%Y-%m-%d'))) - 1

# tomorrow_start_time = int(time.mktime(time.strptime(str(tomorrow), '%Y-%m-%d')))

# tomorrow_end_time = int(time.mktime(time.strptime(str(acquire), '%Y-%m-%d'))) - 1

authorization = "/btgcp-dataservices/thirdparty/dmp/appdatas?appId=1001&appKey=ai_dian_ji&"+"timestamp="+str(last_time)
new_authorization = authorization[0:len(authorization)-1]
m = hashlib.md5()
m.update(authorization)
sign = m.hexdigest()
print sign
print authorization

headers = {
    "Content-Type": "application/json; charset=UTF-8",
    # "Referer": "http://dstest.btgonline.net/",
    "Authorization": sign
    }

# startTime = datetime.datetime.day

url = "http://dstest.btgonline.net/btgcp-dataservices/thirdparty/dmp/appdatas"

py_load = {"startTime": today_start_time, "endTime": yesterday_end_time, "appId": "1001", "type": "event", "timestamp": last_time, "offset": 0, "limit": 5000}

response = requests.post(url, data=json.dumps(py_load), headers=headers).text

data = json.loads(response)

while data['code'] != 0:
    response = requests.post(url, data=json.dumps(py_load), headers=headers).text

    data = json.loads(response)

total = data['total']//5000
print data['total']
print total
# total = data['total']//5000

conn = psycopg2.connect(database="mtdb0", user="pgsl", password="123tw456", host="10.0.3.8", port="5432")
cur = conn.cursor()
cur.execute("create table if not exists test_02(datasl jsonb)")


i = 0
while i <= total:
    py_load1 = {"startTime": today_start_time, "endTime": yesterday_end_time, "appId": "1001", "type": "event", "timestamp": last_time, "offset": i*5000, "limit": 5000}
    print py_load1
    response1 = requests.post(url, data=json.dumps(py_load1), headers=headers).text
    i += 1
    # print(response1)
    # with open("a.txt", "a") as f:
    #     f.write(response1)
    text = json.loads(response1)
    for j in text['data']:
        for key, value in j.items():
            if value == "" or value == None:
                j.pop(key)
        # with open("11.json", "a") as f:
        #     json.dump(j, f)
        #     f.write('\n')
        k = json.dumps(j)
        cur.execute("""INSERT INTO test_02(datasl)VALUES(%s)""", (k,))
    conn.commit()
cur.close()
conn.close()



# http://data.btghl.com/btgcp-dataservices/thirdparty/dmp/appdatas
