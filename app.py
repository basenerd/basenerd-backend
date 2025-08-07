from flask import Flask, render_template
import requests

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/standings')
def standings():
    try:
        resp = requests.get("https://statsapi.mlb.com/api/v1/standings?leagueId=103,104&season=2025&standingsTypes=regularSeason")
        data = resp.json()
        print("RECORDS LENGTH:", len(data["records"]))
        return render_template('standings.html', stats=data["records"])
    except Exception as e:
        print("Error loading standings:", e)
        return f"Error: {e}"

if __name__ == '__main__':
    app.run(debug=True)
