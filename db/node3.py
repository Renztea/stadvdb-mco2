import pymysql

#central_node = pymysql.connect(
#    host = "0.tcp.ngrok.io",
#    port = "10392",
#    username = "root",
#    password = "root",
#    db = "dw_mco1",
#    )

central_node = pymysql.connect(
    host = "34.142.158.246",
    port = 3306,
    user = "root",
    password = "root",
    db = "mco2_imdb_ijs",
)


central_node_cur = central_node.cursor()
central_node_cur.execute("SELECT * FROM movies WHERE year >= 1980")
output = central_node_cur.fetchall()

central_node_cur.close()
central_node.close()

#node3 = pymysql.connect(
#    host = "4.tcp.ngrok.io",
#    port = "17853",
#    username = "root",
#    password = "root",
#    db = "dw_mco1",
#)

node3 = pymysql.connect(
    host = "35.247.162.62",
    port = 3306,
    user = "root",
    password = "root",
    db = "mco2_imdb_ijs",
)
node3_cur = node3.cursor()

for movie in output:
    node3_cur.execute("SELECT * FROM movies WHERE EXISTS (SELECT * FROM movies WHERE year >= 1980)")
    print(movie)
    
node3_cur.execute("INSERT IGNORE INTO movies (`id`, `name`, `year`, `rank`) VALUES (%s, %s, %s, %s)", movie)
node3.commit()

node3_cur.execute("SELECT * FROM movies")
final = node3_cur.fetchall()

node3_cur.close()
node3.close()

print(len(final))
for row in final:
    print(row)