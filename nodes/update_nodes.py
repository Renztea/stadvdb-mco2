import pymysql
import threading


def node_update(hostname, data, num):
    node_conn = pymysql.connect(
        host=hostname,
        port=3306,
        user="root",
        password="root",
        db="mco2_imdb_ijs",
    )
    node_cur = node_conn.cursor()

    total = len(data)
    i = 0

    print("> Updating Node %i...", num)
    for record in data:
        node_cur.execute("INSERT IGNORE INTO movies (`id`, `name`, `year`, `rank`) VALUES (%s, %s, %s, %s)", record)
        i = i + 1
        print("Node %i: %i/%i" % (num, i, total))

    node_conn.commit()

    node_cur.close()
    node_conn.close()
    print("> Node %i done updating.", num)
    return


def main():
    print("> Update starting...")
    central_node = pymysql.connect(
        host="34.142.158.246",
        port=3306,
        user="root",
        password="root",
        db="mco2_imdb_ijs",
    )

    central_node_cur = central_node.cursor()

    # Data for Node 2
    print("> Fetching data for Node 2...")
    central_node_cur.execute("SELECT * FROM movies WHERE year < 1980")
    data_before1980 = central_node_cur.fetchall()

    # Data for Node 3
    print("> Fetching data for Node 3...")
    central_node_cur.execute("SELECT * FROM movies WHERE year >= 1980")
    data_1980onwards = central_node_cur.fetchall()

    central_node_cur.close()
    central_node.close()

    node2_thread = threading.Thread(target=node_update, args=("35.247.134.226", data_before1980, 2))
    node3_thread = threading.Thread(target=node_update, args=("35.247.162.62", data_1980onwards, 3))

    node2_thread.start()
    node3_thread.start()
    node2_thread.join()
    node3_thread.join()


if __name__ == "__main__":
    main()

