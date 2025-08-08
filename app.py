
from flask import Flask, render_template

app = Flask(__name__)

@app.route('/standings')
def standings():
    import requests
    r = requests.get("https://statsapi.mlb.com/api/v1/standings?leagueId=103,104&season=2025&standingsTypes=regularSeason", timeout=15)
    records = r.json().get("records", [])
    return render_template('standings.html', stats=records)

if __name__ == '__main__':
    app.run(debug=True)
