
from flask import Flask, render_template
import requests

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/standings')
def standings():
    resp = requests.get("https://statsapi.mlb.com/api/v1/standings?season=2025")
    records = resp.json()["records"]
    return render_template('standings.html', stats=records)

if __name__ == '__main__':
    app.run(debug=True)
