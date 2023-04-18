from flask import Flask
from flask import render_template
from flask_mysqldb import MySQL
from flask import request
from flask import redirect
from flask import url_for
import pandas as pd
import mysql.connector as connector
import threading
from datetime import datetime

# IP address specifically for this node
local_ip = "35.247.162.62"

app = Flask(__name__)
local_conn = MySQL()

app.config['MYSQL_HOST'] = local_ip
app.config['MYSQL_PORT'] = 3306
app.config['MYSQL_USER'] = "root"
app.config['MYSQL_PASSWORD'] = "root"
app.config['MYSQL_DB'] = "mco2"

# Central Node
local_conn.init_app(app)


def log(event, desc):
    log_conn = connector.connect(
        host=local_ip,
        port=3306,
        user="root",
        password="root",
        db="mco2"
    )
    log_cur = log_conn.cursor()

    log_cur.execute("INSERT INTO `logs`(`timestamp`, `event`, `desc`) VALUES (%s, %s, %s)", (str(datetime.today()), str(event), str(desc)))
    log_conn.commit()

    log_cur.close()
    log_conn.close()
    return


def flag_executed():
    log_conn = connector.connect(
        host=local_ip,
        port=3306,
        user="root",
        password="root",
        db="mco2"
    )
    log_cur = log_conn.cursor()

    log_cur.execute("SELECT id FROM `logs` ORDER BY `id` DESC LIMIT 1")
    log_id = log_cur.fetchall()[0]

    log_cur.execute("UPDATE `logs` SET `executed`=1 WHERE `id`=%s", log_id)
    log_conn.commit()

    log_cur.close()
    log_conn.close()


def node_update(hostname, data, num):
    node_conn = connector.connect(
        host=hostname,
        port=3306,
        user="root",
        password="root",
        db="mco2",
    )
    node_cur = node_conn.cursor()

    total = len(data)
    i = 0

    print("> Updating Node %i...", num)
    for record in data:
        node_cur.execute("INSERT IGNORE INTO movies (`id`, `name`, `year`, `rank`) VALUES (%s, %s, %s, %s)", record)
        i += 1
        print("Node %i: %i/%i" % (num, i, total))

    node_conn.commit()

    node_cur.close()
    node_conn.close()
    print("> Node %i done updating." % num)
    return


@app.route("/")
@app.route("/index")
def index():
    return render_template("index.html")


@app.route("/check_db_size")
def check_db_size():
    cur = local_conn.connection.cursor()
    cur.execute("SELECT COUNT(*) FROM movies")
    results = cur.fetchall()

    print(results)
    cur.close()
    return render_template('index.html')


@app.route("/update_nodes")
def update_nodes():
    print("> Update starting...")
    node3_conn = connector.connect(
        host=local_ip,
        port=3306,
        user="root",
        password="root",
        db="mco2",
    )

    node3_cur = node3_conn.cursor()

    # Data for Node 2
    print("> Fetching data for Central Node...")
    node3_cur.execute("SELECT * FROM movies")
    data_1980onwards = node3_cur.fetchall()

    # Data for Node 2
    print("> Fetching data for Node 2...")
    node3_cur.execute("SELECT * FROM movies WHERE year < 1980")
    data_before1980 = node3_cur.fetchall()

    # Maintain list of movies with `year` >= 1980
    print("> Deleting records with year < 1980")
    node3_cur.execute("DELETE FROM movies WHERE year < 1980")
    print(node3_cur.fetchall())

    node3_conn.commit()

    node3_cur.close()
    node3_conn.close()

    central_node_thread = threading.Thread(target=node_update, args=("34.142.158.246", data_1980onwards, 1))
    node2_thread = threading.Thread(target=node_update, args=("34.142.187.114", data_before1980, 2))

    central_node_thread.start()
    node2_thread.start()
    central_node_thread.join()
    node2_thread.join()

    return render_template('index.html')


@app.route("/send_query", methods=['POST'])
def send_query():
    # refresh result.html everytime the user sends a new query
    fp = open("templates/result.html", "w")
    fp.close()

    update_flag = False
    cur = local_conn.connection.cursor()

    data = request.form['input_query']
    data = data.split(";")

    for query in data:
        if query != "":
            query = query.strip()
            print("> " + query)

            if (
                query[0:6].upper() != "SELECT" and
                query[0:3].upper() != "SET" and
                query[0:5].upper() != "START" and
                query[0:6].upper() != "COMMIT" and
                query[0:5].upper() != "BEGIN"
            ) and update_flag:
                update_flag = True

            if query[0:3].upper() == "SET":
                log("CHECK_START", "Start of transaction block")
                log("SET", query)
            elif query[0:5].upper() == "START" or query[0:5].upper() == "BEGIN":
                log("START", "Transaction start")
            elif query[0:6].upper() == "COMMIT":
                log("COMMIT", "Transaction finished")
            else:
                log("QUERY", query)

            try:
                cur.execute(query)
            except:
                print("> Query unsuccessful")
                return redirect(url_for("index"))
            else:
                flag_executed()
                result = cur.fetchall()

            if len(result) != 0:
                fp = open("templates/result.html", "w")
                result_html = pd.DataFrame(result).to_html()
                fp.write(str(result_html))
                fp.close()

    local_conn.connection.commit()
    cur.close()
    print("> Transaction finished.")

    if update_flag:
        update_nodes()

    return redirect(url_for("see_results"))


@app.route("/see_results")
def see_results():
    return render_template("result.html")


if __name__ == "__main__":
    app.run(host='127.0.0.1', port=5000, debug=True)
