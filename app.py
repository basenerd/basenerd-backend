
from flask import Flask, render_template
import requests

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/standings')
def standings():
    # Try 2025 data
    url = "https://statsapi.mlb.com/api/v1/standings?leagueId=103,104&season=2025&standingsTypes=regularSeason"
    resp = requests.get(url)
    records = resp.json().get("records", [])

    # Log for Render
    print("RECORDS LENGTH:", len(records))

    # If 2025 is down, fall back to 2024
    if not records:
        url = "https://statsapi.mlb.com/api/v1/standings?leagueId=103,104&season=2024&standingsTypes=regularSeason"
        resp = requests.get(url)
        records = resp.json().get("records", [])
        print("Fallback to 2024. RECORDS LENGTH:", len(records))

    return render_template('standings.html', stats=records)

if __name__ == '__main__':
    app.run(debug=True)
