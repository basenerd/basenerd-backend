
from flask import Flask, render_template
import requests

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/standings')
def standings():
    url = "https://statsapi.mlb.com/api/v1/standings?leagueId=103,104&season=2025&standingsTypes=regularSeason"
    resp = requests.get(url)
    records = resp.json().get("records", [])

    # Add debug logging to Render logs
    print("RECORDS DEBUG:")
    for r in records:
        print(f"{r.get('division', {}).get('name')} | Teams: {len(r.get('teamRecords', []))}")

    return render_template('standings.html', stats=records)

if __name__ == '__main__':
    app.run(debug=True)
