from flask import Flask, render_template
import requests

app = Flask(__name__)

@app.route("/")
def home():
    # templates/index.html must exist (you have it)
    return render_template("index.html")

@app.route("/standings")
def standings():
    r = requests.get(
        "https://statsapi.mlb.com/api/v1/standings?leagueId=103,104&season=2025&standingsTypes=regularSeason",
        timeout=15
    )
    records = r.json().get("records", [])
    return render_template("standings.html", stats=records)

# Not used on Render (gunicorn runs the app), but fine for local dev
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

