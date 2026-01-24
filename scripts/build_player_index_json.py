import json
import time
import requests

alphabet = "abcdefghijklmnopqrstuvwxyz"
players = {}  # id -> fullName

for letter in alphabet:
    url = f"https://statsapi.mlb.com/api/v1/people/search?name={letter}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()

    for p in data.get("people", []):
        players[str(p["id"])] = p.get("fullName")

    print(f"{letter}: total players so far = {len(players)}")
    time.sleep(0.2)  # be nice to the API

out_path = "players_index.json"
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(players, f, ensure_ascii=False)

print(f"✅ Wrote {len(players)} players to {out_path}")
