import redis
import mysql.connector

# Connecting to redis
r = redis.StrictRedis(host='localhost', port=6379, db=1, charset="utf-8", decode_responses=True)
# Connecting to MySQL
cnx = mysql.connector.connect(user='yulia2', password='eda2', host='127.0.0.1', database='weather')
cursor = cnx.cursor(buffered=True)

for key in r.scan_iter():
    key_str = key.split('-')
    hour = key_str[0]
    minute = key_str[1]
    second = key_str[2]
    day = key_str[3]
    month = key_str[4]
    year = key_str[5]
    data = r.lrange(key, 0, -1)

    if len(data) == 0:
        average = 0
    else:
        x = list(map(int, data))
        average = sum(x) / len(x)
        average = float("{:.1f}".format(average))
        print(average)

        cursor.execute(("INSERT INTO temperature(hour,minute,second,day,month,year,temperatue_average) "
                        "VALUES(%s,%s,%s,%s,%s,%s,%s)"),
                       (hour, minute, second, day, month, year, average))
        cnx.commit()
r.flushdb()

cursor.close()
cnx.close()
