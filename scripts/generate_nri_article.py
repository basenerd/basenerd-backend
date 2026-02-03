import requests

BASE = "https://statsapi.mlb.com/api/v1"

# CHANGE THIS if your route is different
PLAYER_URL_TEMPLATE = "/player/{player_id}"

ARTICLE_META = {
    "title": "2026 Spring Training Non-Roster Invitees (By Team)",
    "date": "2026-03-28",
    "author": "Nick Labella",
    "slug": "spring-training-nri-2026",
}

INTRO_BLURB = (
    "Non-roster invitees can swing Spring Training storylines fast — from breakout prospects to veteran depth pieces "
    "fighting for a spot. Below you’ll find each team’s invite list, plus placeholders for Top 100 and Team Top 30 "
    "prospect ranks so you can quickly spot the most interesting names.\n\n"
    "**How to use this page:** click your team below to jump straight to their table."
)

def get_teams():
    data = requests.get(f"{BASE}/teams", params={"sportId": 1}).json()
    teams = data.get("teams", [])
    # Sort by team name for consistent article order
    return sorted(teams, key=lambda t: t.get("name", ""))

def get_non_roster_invitees(team_id: int):
    data = requests.get(
        f"{BASE}/teams/{team_id}/roster",
        params={"rosterType": "nonRosterInvitees"}
    ).json()
    return data.get("roster", [])

def md_front_matter(meta: dict) -> str:
    lines = ["---"]
    for k, v in meta.items():
        lines.append(f"{k}: {v}")
    lines.append("---\n")
    return "\n".join(lines)

def md_table(rows):
    header = "| Player | Position | Highest Level | Top 100 Rank | Team Top 30 Rank |\n"
    sep    = "|---|---:|---:|---:|---:|\n"
    body = ""
    for r in rows:
        body += (
            f"| {r['player_md']} | {r['pos']} | {r['highest_level']} | {r['top100']} | {r['team30']} |\n"
        )
    return header + sep + body + "\n"

def main():
    teams = get_teams()

    # Build article
    out = []
    out.append(md_front_matter(ARTICLE_META))
    out.append(INTRO_BLURB + "\n")
    out.append("## Jump to a team")

    # Jump list uses TEAM ID as anchor: #<teamId>
    for t in teams:
        team_id = t.get("id")
        name = t.get("name", "Unknown Team")
        if team_id:
            out.append(f"- [{name}](#{team_id})")
    out.append("\n---\n")

    total_nri = 0

    for t in teams:
        team_id = t.get("id")
        team_name = t.get("name", "Unknown Team")
        if not team_id:
            continue

        roster = get_non_roster_invitees(team_id)

        # Build per-team rows
        rows = []
        for row in roster:
            person = row.get("person", {})
            player_id = person.get("id")
            full_name = person.get("fullName", "Unknown")
            pos = (row.get("position") or {}).get("abbreviation", "-")

            # Player markdown link
            if player_id:
                href = PLAYER_URL_TEMPLATE.format(player_id=player_id)
                player_md = f"[{full_name}]({href})"
            else:
                player_md = full_name

            rows.append({
                "player_md": player_md,
                "pos": pos,
                "highest_level": "-",  # fill manually
                "top100": "-",         # fill manually
                "team30": "-",         # fill manually
                "player_id": player_id
            })

        # Sort rows by position then player name (strip markdown)
        def sort_key(r):
            # crude "strip" for sorting: remove leading '[' and split at ']'
            label = r["player_md"]
            if label.startswith("[") and "](" in label:
                label = label[1:].split("](", 1)[0]
            return (r["pos"], label)

        rows = sorted(rows, key=sort_key)

        out.append(f'<a id="{team_id}"></a>')
        out.append(f"## {team_name}\n")

        if rows:
            out.append(md_table(rows))
            total_nri += len(rows)
        else:
            out.append("_No non-roster invitees returned by the API for this team (yet)._ \n")

        out.append("---\n")

    out.append(f"<!-- Total NRIs: {total_nri} -->\n")

    with open("articles/spring-training-nri-2026.md", "w", encoding="utf-8") as f:
        f.write("\n".join(out))

if __name__ == "__main__":
    main()



