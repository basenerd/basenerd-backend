
from flask import Flask, render_template
import requests

app = Flask(__name__, static_folder='static', template_folder='templates')

# Homepage — renders your full layout with cards and nav
@app.route('/')
def home():
    return render_template('index.html')

# Standings — fetches from MLB API and renders the standings template
@app.route('/standings')
def standings():
    url = "https://statsapi.mlb.com/api/v1/standings?leagueId=103,104&season=2025&standingsTypes=regularSeason"
    resp = requests.get(url)
    records = resp.json().get("records", [])
    return render_template('standings.html', stats=records)

if __name__ == '__main__':
    app.run(debug=True)
