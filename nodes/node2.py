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
local_ip = "34.142.187.114"

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


def flag_executed_query():
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

    print("> Updating Node %i..." % num)
    for record in data:
        node_cur.execute("SELECT IF(EXISTS(SELECT `id` FROM movies WHERE `id`=%s)=1, 1, 0)", [record[0]])
        exists = node_cur.fetchall()
        node_cur.execute("START TRANSACTION")
        if exists[0][0] == 1:
            # print("Updating record ID[" + str(record[0]) + "]...")
            try:
                node_cur.execute("UPDATE movies SET `id`=%s,`name`=%s,`year`=%s,`rank`=%s WHERE `id`=%s",
                                 (record[0], record[1], record[2], record[3], record[0]))
            except:
                log("UPDATE ERROR", "Could not execute UPDATE statement")
                node_cur.execute("ROLLBACK")
                print("> Update unsuccessful")
                return

        elif exists[0][0] == 0:
            print("Adding new record: " + str(record) + "...")

            try:
                node_cur.execute("INSERT INTO movies VALUES (%s, %s, %s, %s)", record)
            except:
                log("UPDATE ERROR", "Could not execute INSERT statement")
                node_cur.execute("ROLLBACK")
                print("> Update unsuccessful")
                return

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
    node2_conn = connector.connect(
        host=local_ip,
        port=3306,
        user="root",
        password="root",
        db="mco2",
    )
    node2_cur = node2_conn.cursor()

    try:
        # Data for Central Node
        print("> Fetching data for Central Node...")
        node2_cur.execute("SELECT * FROM movies")
    except:
        log("UPDATE ERROR", "Error occurred during update")
        print("> Update unsuccessful")
        return redirect(url_for("index"))
    else:
        data_before1980 = node2_cur.fetchall()

    try:
        # Data for Node 3
        print("> Fetching data for Node 3...")
        node2_cur.execute("SELECT * FROM movies WHERE year >= 1980")
    except:
        log("UPDATE ERROR", "Error occurred during update")
        print("> Update unsuccessful")
        return redirect(url_for("index"))
    else:
        data_1980onwards = node2_cur.fetchall()

    try:
        # Maintain list of movies with `year` < 1980
        node2_cur.execute("START TRANSACTION")
        print("> Deleting records with year >= 1980")
        node2_cur.execute("DELETE FROM movies WHERE year >= 1980")
    except:
        log("UPDATE ERROR", "Error occurred during update")
        print("> Update unsuccessful")
        node2_cur.execute("ROLLBACK")
        return redirect(url_for("index"))
    else:
        print(node2_cur.fetchall())
        node2_cur.execute("COMMIT")
        node2_conn.commit()

        node2_cur.close()
        node2_conn.close()

        central_node_thread = threading.Thread(target=node_update, args=("34.142.158.246", data_before1980, 1))
        node3_thread = threading.Thread(target=node_update, args=("35.247.162.62", data_1980onwards, 3))

        central_node_thread.start()
        node3_thread.start()
        central_node_thread.join()
        node3_thread.join()

    log("UPDATE SUCCESSFUL", "End of update process")
    flag_executed_query()
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
                flag_executed_query()
                result = cur.fetchall()

            if len(result) != 0:
                fp = open("templates/result.html", "w")
                df = pd.DataFrame(result)
                field_names = [i[0] for i in cur.description]
                df.columns = field_names
                result_html = df.to_html(col_space="50px", index=False, justify="center")
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
