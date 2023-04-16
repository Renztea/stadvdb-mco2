from flask import Flask, render_template, url_for
from flask_sqlalchemy import SQLAlchemy
from markupsafe import escape

app = Flask(__name__)


@app.route("/, methods=['GET', 'POST']")
def hello():
    return render_template('index.html')

if __name__ == "__main__":
    app.run(debug=True)