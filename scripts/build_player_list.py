# scripts/build_player_list.py

import requests
import psycopg2
import random

conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS players_index (
    id INTEGER PRIMARY KEY,
    full_name TEXT
)
""")

# Pull players from MLB people endpoint by starting letter
alphabet = "abcdefghijklmnopqrstuvwxyz"

for letter in alphabet:
    url = f"https://statsapi.mlb.com/api/v1/people/search?name={letter}"
    r = requests.get(url).json()
    for p in r.get("people", []):
        pid = p["id"]
        name = p["fullName"]
        cur.execute("""
            INSERT INTO players_index (id, full_name)
            VALUES (%s,%s)
            ON CONFLICT DO NOTHING
        """, (pid, name))

conn.commit()
cur.close()
conn.close()

print("Done building player index")
