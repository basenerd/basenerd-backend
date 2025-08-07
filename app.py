
from flask import Flask, render_template

app = Flask(__name__)

@app.route('/standings')
def standings():
    # Mocked standings data
    mock_stats = [
        {
            "league": {"name": "National League"},
            "division": {"name": "NL East"},
            "teamRecords": [
                {
                    "team": {"id": 121, "name": "New York Mets"},
                    "wins": 60,
                    "losses": 50,
                    "winningPercentage": ".545",
                    "gamesBack": "2.0"
                }
            ]
        },
        {
            "league": {"name": "American League"},
            "division": {"name": "AL West"},
            "teamRecords": [
                {
                    "team": {"id": 117, "name": "Houston Astros"},
                    "wins": 65,
                    "losses": 45,
                    "winningPercentage": ".591",
                    "gamesBack": "0.0"
                }
            ]
        }
    ]
    return render_template('standings.html', stats=mock_stats)

if __name__ == '__main__':
    app.run(debug=True)
