import pymysql

# Connect to central db
central_node = pymysql.connect(
    host='0.tcp.ngrok.io',
    port=10392,
    user='root',
    password='root',
    database='dw_mco1'
)

cn_cursor = central_node.cursor()

# Fetch data 
cn_cursor.execute('SELECT * FROM movies WHERE rating > 0 AND year < 1980')
rows = cn_cursor.fetchall()

# Close connection
cn_cursor.close()
central_node.close()

# Connection to local device db
self = pymysql.connect (
    host  ='localhost',
    user='root',
    password='Yanny-1201',
    database="mco2"
)
# print(len(rows))

self_cursor = self.cursor()

# This part only needs to be run once
# self_cursor.execute("CREATE TABLE IF NOT EXISTS node_db (id INT, name VARCHAR(100), year INT, rating FLOAT, PRIMARY KEY(id)) ENGINE=InnoDB")

# for row in rows:
#     print(row)
#     self_cursor.execute("INSERT INTO node_db (id, name, year, rating) VALUES (%s, %s, %s, %s)", row)

self.commit()
self_cursor.execute("SELECT * FROM node_db")
final = self_cursor.fetchall()

self_cursor.close()
self.close()

print(len(final))
for row in final:
    print(row)


