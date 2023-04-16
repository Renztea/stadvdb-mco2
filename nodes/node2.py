from flask import Flask
from flask import render_template
from flask_mysqldb import MySQL
from flask import request
import pandas as pd
import mysql.connector as connector
import threading


def node_update(hostname, data, num):
    node_conn = connector.connect(
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
        i += 1
        print("Node %i: %i/%i" % num, i, total)

    node_conn.commit()

    node_cur.close()
    node_conn.close()
    print("> Node %i done updating.", num)
    return


# IP address specifically for this node
local_ip = "35.247.134.226"

app = Flask(__name__)
local_conn = MySQL()

app.config['MYSQL_HOST'] = local_ip
app.config['MYSQL_PORT'] = 3306
app.config['MYSQL_USER'] = "root"
app.config['MYSQL_PASSWORD'] = "root"
app.config['MYSQL_DB'] = "mco2_imdb_ijs"

# Central Node
local_conn.init_app(app)


@app.route("/")
@app.route("/index")
def index():
    return render_template("index.html")


@app.route("/check_db_size")
def check_db_size():
    cur = local_conn.connection.cursor()
    cur.execute("SELECT COUNT(*) FROM movies_200")
    results = cur.fetchall()

    print(results)

    cur.close()

    return render_template('index.html')


@app.route("/update_nodes")
def update_nodes():
    print("> Update starting...")
    node2_conn = connector.connect(
        host=local_ip,
        port=3306,
        user="root",
        password="root",
        db="mco2_imdb_ijs",
    )

    node2_cur = node2_conn.cursor()

    # Data for Node 2
    print("> Fetching data for Central Node...")
    node2_cur.execute("SELECT * FROM movies_200")
    data_before1980 = node2_cur.fetchall()

    node2_cur.close()
    node2_conn.close()

    central_node_thread = threading.Thread(target=node_update, args=("34.142.158.246", data_before1980, 1))

    central_node_thread.start()
    central_node_thread.join()

    return render_template('index.html')


@app.route("/send_query", methods=['POST'])
def send_query():
    cur = local_conn.connection.cursor()

    data = request.form['input_query']
    data = data.split(";")

    for query in data:
        if query != "":
            print("> " + query)
            cur.execute(query)
            result = cur.fetchall()

            if len(result) > 0:
                result_html = pd.DataFrame(result).to_html()
                fp = open("templates/result.html", "w")
                fp.write(result_html)
                fp.close()

    local_conn.connection.commit()

    cur.close()

    update_nodes()

    return render_template('index.html')


@app.route("/see_results")
def see_results():
    return render_template("result.html")
