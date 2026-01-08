from flask import Flask, render_template

app = Flask(__name__)

@app.get("/")
def home():
    return render_template("home.html", title="Basenerd")

@app.get("/standings")
def standings():
    # Placeholder for now — we’ll wire in MLB standings after the site is stable.
    return render_template("standings.html", title="Standings")

# Optional: local run support (Render will use gunicorn)
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
