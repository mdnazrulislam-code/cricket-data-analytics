#!/usr/bin/env python3
"""Store concluded Cricbuzz matches that are not already in Databricks.

Databricks tables live in cricsocc.bronze. The default schema is not used.

Reads the public recent-matches page. Upcoming and in-play matches are
ignored. Finished, drawn, tied, no-result, abandoned, and called-off
matches are stored from the scorecard, including venue, toss, result,
and officials when Cricbuzz has them.

Environment:
  DATABRICKS_SERVER_HOSTNAME
  DATABRICKS_HTTP_PATH
  DATABRICKS_TOKEN
"""

from __future__ import annotations

import http.client
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

RECENT_URL = "https://www.cricbuzz.com/cricket-match/live-scores/recent-matches"
HEADERS = {
    "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
    "User-Agent": "Mozilla/5.0 (compatible; CricSocc/0.1; +https://github.com/mdnazrulislam-code/cricsocc_full_stack)",
}
CATALOG = "cricsocc.bronze"
# Spark SQL treats "\" as an escape. Postgres standard strings do not.
SQL_DIALECT = "spark"


def fetch(url: str) -> tuple[int, str]:
    last = 0
    for attempt in range(4):
        request = urllib.request.Request(url, headers=HEADERS)
        try:
            with urllib.request.urlopen(request, timeout=45) as response:
                return response.status, response.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as error:
            last = error.code
            if error.code in (404, 204):
                return error.code, ""
            if error.code not in (403, 429) and error.code < 500:
                break
        except (urllib.error.URLError, http.client.IncompleteRead, TimeoutError, ConnectionError, OSError):
            last = 0
        time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"Cricbuzz HTTP {last} for {url}")


def unescape_rsc(raw: str) -> str:
    out = []
    i = 0
    while i < len(raw):
        if raw.startswith('\\"', i):
            out.append('"')
            i += 2
            continue
        if raw.startswith("\\\\", i):
            out.append(raw[i + 1] if i + 1 < len(raw) else "")
            i += 2
            continue
        out.append(raw[i])
        i += 1
    return "".join(out)


def take_escaped_object(html: str, start: int) -> str | None:
    depth = 0
    i = start
    while i < len(html):
        if html.startswith('\\"', i):
            i += 2
            continue
        if html.startswith("\\\\", i):
            i += 2
            continue
        ch = html[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return html[start : i + 1]
        i += 1
    return None


def extract_recent_cards(html: str) -> list[dict]:
    info_mark = '\\"matchInfo\\":'
    type_mark = '\\"matchType\\":\\"'
    score_mark = '\\"matchScore\\":'
    cards = []
    seen = set()
    category = ""
    anchor = -1
    cursor = 0
    while cursor < len(html):
        type_at = html.find(type_mark, cursor)
        info_at = html.find(info_mark, cursor)
        if info_at < 0:
            break
        if 0 <= type_at < info_at:
            start = type_at + len(type_mark)
            end = html.find('\\"', start)
            if end > start:
                category = html[start:end]
                anchor = type_at
            cursor = end + 2 if end > start else type_at + len(type_mark)
            continue
        if anchor >= 0 and info_at - anchor > 5000:
            category = ""
        anchor = info_at
        brace = html.find("{", info_at)
        if brace < 0:
            break
        raw = take_escaped_object(html, brace)
        if not raw:
            break
        cursor = brace + len(raw)
        try:
            info = json.loads(unescape_rsc(raw))
        except json.JSONDecodeError:
            continue
        match_id = num(info.get("matchId"))
        if not match_id or match_id in seen:
            continue
        seen.add(match_id)
        card = {"matchType": category, "matchInfo": info}
        score_at = html.find(score_mark, cursor)
        if 0 <= score_at and score_at - cursor < 40:
            score_brace = html.find("{", score_at)
            score_raw = take_escaped_object(html, score_brace) if score_brace >= 0 else None
            if score_raw:
                try:
                    card["matchScore"] = json.loads(unescape_rsc(score_raw))
                except json.JSONDecodeError:
                    pass
                else:
                    cursor = score_brace + len(score_raw)
        cards.append(card)
    return cards


def num(value) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def text(value) -> str:
    if value is None or isinstance(value, (dict, list)):
        return ""
    return str(value).strip()


def category_of(raw: str) -> str:
    value = (raw or "").lower()
    if "women" in value:
        return "women"
    if "league" in value:
        return "league"
    if "domestic" in value:
        return "domestic"
    if "international" in value:
        return "international"
    return "other"


def format_code(raw: str) -> str:
    value = (raw or "").lower()
    if "test" in value:
        return "test"
    if "odi" in value or "one-day" in value or "one day" in value:
        return "odi"
    if "t10" in value:
        return "t10"
    if "t20" in value or "twenty" in value:
        return "t20"
    if "hundred" in value:
        return "hundred"
    cleaned = re.sub(r"[^a-z0-9]+", "_", value).strip("_")
    return cleaned or "other"


def concluded_status(state: str, status: str) -> str | None:
    """A match that will not be played further. Live and upcoming return None."""
    blob = f"{state} {status}".lower()
    if re.search(r"abandon|called off|washed out", blob):
        return "abandoned"
    if "cancel" in blob:
        return "cancelled"
    if re.search(r"\bcomplete\b|won by|\bdrawn?\b|\btied?\b|no result", blob):
        return "finished"
    return None


def team_of(raw) -> dict | None:
    if not isinstance(raw, dict):
        return None
    team_id = num(raw.get("teamId") or raw.get("id"))
    name = text(raw.get("teamName") or raw.get("name"))
    if not team_id or not name:
        return None
    return {"id": team_id, "name": name, "short": text(raw.get("teamSName") or raw.get("shortName")) or None}


def score_line(block) -> str | None:
    if not isinstance(block, dict):
        return None
    parts = []
    for key in ("inngs1", "inngs2", "inngs3", "inngs4"):
        inn = block.get(key)
        if not isinstance(inn, dict) or inn.get("runs") is None:
            continue
        wickets = inn.get("wickets")
        parts.append(f"{inn['runs']}/{wickets}" if wickets is not None and wickets < 10 else str(inn["runs"]))
    return " & ".join(parts) if parts else None


def epoch_ms(value) -> datetime | None:
    try:
        stamp = int(value)
    except (TypeError, ValueError):
        return None
    if stamp <= 0:
        return None
    try:
        parsed = datetime.fromtimestamp(stamp / 1000, tz=timezone.utc).replace(tzinfo=None)
    except (OverflowError, OSError, ValueError):
        return None
    this_year = datetime.now(timezone.utc).year
    if parsed.year < 1800 or parsed.year > this_year + 1:
        return None
    return parsed


def year_anchor(name: str | None) -> datetime | None:
    found = re.findall(r"\b(18\d{2}|19\d{2}|20\d{2})\b", name or "")
    if not found:
        return None
    year = int(found[-1])
    if year > datetime.now(timezone.utc).year:
        return None
    return datetime(year, 1, 1)


def finished_fixtures(cards: list[dict]) -> list[dict]:
    fixtures = []
    for card in cards:
        info = card.get("matchInfo") if isinstance(card.get("matchInfo"), dict) else {}
        match_id = num(info.get("matchId"))
        home = team_of(info.get("team1"))
        away = team_of(info.get("team2"))
        if not match_id or not home or not away:
            continue
        status = concluded_status(text(info.get("state") or info.get("stateTitle")), text(info.get("status")))
        if status is None:
            continue
        venue = info.get("venueInfo") if isinstance(info.get("venueInfo"), dict) else {}
        score = card.get("matchScore") if isinstance(card.get("matchScore"), dict) else {}
        home_score = score_line(score.get("team1Score"))
        away_score = score_line(score.get("team2Score"))
        short = None
        if home_score or away_score:
            short = f"{home['short'] or home['name']} {home_score or '—'} · {away['short'] or away['name']} {away_score or '—'}"
        series_name = text(info.get("seriesName")) or "Cricket"
        fixtures.append(
            {
                "match_id": match_id,
                "series_id": num(info.get("seriesId")),
                "series_name": series_name,
                "category": category_of(f"{series_name} {text(info.get('matchDesc'))} {text(card.get('matchType'))}"),
                "format_code": format_code(text(info.get("matchFormat"))),
                "status": status,
                "start_time": epoch_ms(info.get("startDate")),
                "end_time": epoch_ms(info.get("endDate")),
                "home": home,
                "away": away,
                "match_desc": text(info.get("matchDesc")) or None,
                "status_text": text(info.get("status")) or None,
                "venue_name": text(venue.get("ground") or venue.get("name")) or None,
                "venue_city": text(venue.get("city")) or None,
                "venue_country": text(venue.get("country")) or None,
                "short_score": short,
            }
        )
    return fixtures


def numbered(raw) -> list:
    if not isinstance(raw, dict):
        return []
    return [value for _, value in sorted(raw.items(), key=lambda item: num(re.sub(r"\D", "", item[0])) or 0)]


def extras_of(raw) -> dict | None:
    if not isinstance(raw, dict):
        return None
    row = {
        "total": int(raw.get("total") or 0),
        "wides": int(raw.get("wides") or 0),
        "no_balls": int(raw.get("no_balls") or raw.get("noBalls") or 0),
        "byes": int(raw.get("byes") or 0),
        "leg_byes": int(raw.get("legByes") or raw.get("leg_byes") or 0),
        "penalty": int(raw.get("penalty") or 0),
    }
    return row if any(row.values()) else None


def wickets_of(raw) -> list[dict]:
    rows = []
    for item in numbered(raw):
        if not isinstance(item, dict):
            continue
        name = text(item.get("batName") or item.get("batsmanName") or item.get("name"))
        number = num(item.get("wktNbr") or item.get("wicketNbr") or item.get("wicketNumber"))
        if not name or not number:
            continue
        overs = item.get("wktOver") if item.get("wktOver") not in (None, "") else item.get("overs")
        rows.append(
            {
                "number": number,
                "runs": int(item.get("wktRuns") or item.get("runs") or 0),
                "overs": None if overs in (None, "") else str(overs),
                "player_id": num(item.get("batId")),
                "player_name": name,
            }
        )
    return sorted(rows, key=lambda row: row["number"])


def plain_commentary(value) -> str:
    cleaned = re.sub(r"\b[A-Z]\d+\$", " ", text(value))
    return re.sub(r"\s+", " ", cleaned).strip()


def whole_number(value) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def commentary_lines(payload: dict, innings: int) -> list[dict]:
    lines = []
    sequence = 0
    for block in payload.get("commentary") or []:
        if not isinstance(block, dict):
            continue
        for item in block.get("commentaryList") or []:
            if not isinstance(item, dict):
                continue
            comm = plain_commentary(item.get("commText"))
            if not comm:
                continue
            sequence += 1
            batter = item.get("batsmanStriker") if isinstance(item.get("batsmanStriker"), dict) else {}
            bowler = item.get("bowlerStriker") if isinstance(item.get("bowlerStriker"), dict) else {}
            over = item.get("overNumber")
            try:
                over_n = float(over) if over is not None and str(over) != "" else None
            except (TypeError, ValueError):
                over_n = None
            ball = whole_number(item.get("ballNbr"))
            lines.append(
                {
                    "innings": innings,
                    "sequence": sequence,
                    "over": over_n,
                    "ball": ball if ball and ball > 0 else None,
                    "is_delivery": bool(ball and ball > 0),
                    "event": text(item.get("event")) or None,
                    "batter_id": num(batter.get("batId")),
                    "batter_name": text(batter.get("batName")) or None,
                    "bowler_id": num(bowler.get("bowlId")),
                    "bowler_name": text(bowler.get("bowlName")) or None,
                    "runs": whole_number(item.get("totalRuns")),
                    "score": whole_number(item.get("batTeamScore")),
                    "text": comm,
                }
            )
    return lines


def officials_from_html(html: str) -> dict:
    found = {}
    for label, value in re.findall(r'font-bold">([^<]+)</div><div>([^<]*)</div>', html):
        key = label.strip().lower()
        if key not in found:
            found[key] = value.strip()
    umpires = [part.strip() for part in found.get("umpires", "").split(",") if part.strip()]
    return {
        "umpire1": umpires[0] if umpires else None,
        "umpire2": umpires[1] if len(umpires) > 1 else None,
        "umpire3": found.get("3rd umpire") or found.get("third umpire") or found.get("tv umpire") or (umpires[2] if len(umpires) > 2 else None),
        "referee": found.get("referee") or found.get("match referee"),
        "toss": found.get("toss") or None,
    }


def lit(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, datetime):
        return f"TIMESTAMP '{value.strftime('%Y-%m-%d %H:%M:%S')}'"
    text = str(value)
    if SQL_DIALECT == "spark":
        # A trailing backslash would escape the closing quote and the INSERT
        # would fail to parse (seen on commentary ending in "\").
        text = text.replace("\\", "\\\\")
    return "'" + text.replace("'", "''") + "'"


class Store:
    def __init__(self):
        global SQL_DIALECT
        SQL_DIALECT = "spark"
        host = os.environ.get("DATABRICKS_SERVER_HOSTNAME", "").strip()
        path = os.environ.get("DATABRICKS_HTTP_PATH", "").strip()
        token = os.environ.get("DATABRICKS_TOKEN", "").strip()
        if not host or not path or not token:
            raise SystemExit("Set DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, and DATABRICKS_TOKEN")
        from databricks import sql

        self.conn = sql.connect(server_hostname=host, http_path=path, access_token=token)
        self.cur = self.conn.cursor()

    def close(self):
        self.cur.close()
        self.conn.close()

    def run(self, statement: str):
        self.cur.execute(statement)

    def rows(self, statement: str) -> list[tuple]:
        self.cur.execute(statement)
        return list(self.cur.fetchall())

    def ensure_schema(self):
        self.run(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}")
        tables = {
            "teams": "team_id BIGINT, name STRING, short_name STRING",
            "players": "player_id BIGINT, name STRING",
            "series": "series_id BIGINT, name STRING, category STRING, start_date DATE, end_date DATE",
            "venues": "venue_id BIGINT, name STRING, city STRING, country STRING",
            "matches": """
                match_id BIGINT, series_id BIGINT, venue_id BIGINT, home_team_id BIGINT, away_team_id BIGINT,
                category STRING, format_code STRING, status STRING, start_time TIMESTAMP, end_time TIMESTAMP,
                match_desc STRING, status_text STRING, toss_text STRING, result_text STRING, short_score STRING,
                umpire1 STRING, umpire2 STRING, umpire3 STRING, referee STRING,
                officials_loaded BOOLEAN, commentary_loaded BOOLEAN
            """,
            "innings": """
                match_id BIGINT, innings_number INT, batting_team_id BIGINT, bowling_team_id BIGINT,
                runs INT, wickets INT, overs STRING, run_rate DOUBLE
            """,
            "extras": "match_id BIGINT, innings_number INT, total INT, wides INT, no_balls INT, byes INT, leg_byes INT, penalty INT",
            "batting": """
                match_id BIGINT, innings_number INT, player_id BIGINT, bat_order INT, runs INT, balls INT,
                fours INT, sixes INT, strike_rate STRING, is_out BOOLEAN, dismissal STRING,
                is_captain BOOLEAN, is_keeper BOOLEAN
            """,
            "bowling": """
                match_id BIGINT, innings_number INT, player_id BIGINT, bowl_order INT, overs STRING,
                maidens INT, runs INT, wickets INT, economy STRING
            """,
            "wickets": "match_id BIGINT, innings_number INT, wicket_number INT, player_id BIGINT, player_name STRING, runs INT, overs STRING",
            "squads": "match_id BIGINT, team_id BIGINT, player_id BIGINT, role STRING, is_captain BOOLEAN, is_keeper BOOLEAN, is_playing BOOLEAN",
            "commentary": """
                match_id BIGINT, innings_number INT, sequence_number INT, over_number DOUBLE, ball_number INT,
                is_delivery BOOLEAN, event STRING, batter_id BIGINT, batter_name STRING, bowler_id BIGINT,
                bowler_name STRING, runs INT, team_score INT, commentary_text STRING
            """,
        }
        for name, columns in tables.items():
            self.run(f"CREATE TABLE IF NOT EXISTS {CATALOG}.{name} ({columns})")

    def existing_ids(self, match_ids: list[int]) -> set[int]:
        if not match_ids:
            return set()
        listed = ",".join(str(match_id) for match_id in match_ids)
        found = self.rows(
            f"SELECT match_id FROM {CATALOG}.matches WHERE commentary_loaded = TRUE AND match_id IN ({listed})"
        )
        return {int(row[0]) for row in found}

    def clear_match(self, match_id: int):
        for table in ("commentary", "squads", "wickets", "bowling", "batting", "extras", "innings", "matches"):
            self.run(f"DELETE FROM {CATALOG}.{table} WHERE match_id = {int(match_id)}")

    def upsert_team(self, team: dict):
        self.run(
            f"""
            MERGE INTO {CATALOG}.teams AS t
            USING (SELECT {lit(team['id'])} AS team_id, {lit(team['name'])} AS name, {lit(team['short'])} AS short_name) AS s
            ON t.team_id = s.team_id
            WHEN MATCHED THEN UPDATE SET name = s.name, short_name = COALESCE(s.short_name, t.short_name)
            WHEN NOT MATCHED THEN INSERT (team_id, name, short_name) VALUES (s.team_id, s.name, s.short_name)
            """
        )

    def upsert_player(self, player_id: int, name: str):
        self.run(
            f"""
            MERGE INTO {CATALOG}.players AS p
            USING (SELECT {lit(player_id)} AS player_id, {lit(name)} AS name) AS s
            ON p.player_id = s.player_id
            WHEN MATCHED THEN UPDATE SET name = s.name
            WHEN NOT MATCHED THEN INSERT (player_id, name) VALUES (s.player_id, s.name)
            """
        )

    def upsert_series(self, fixture: dict, category: str):
        if not fixture["series_id"]:
            return
        start_at = fixture.get("start_time") or year_anchor(fixture.get("series_name"))
        if start_at is None:
            self.run(
                f"""
                MERGE INTO {CATALOG}.series AS s
                USING (SELECT {lit(fixture['series_id'])} AS series_id, {lit(fixture['series_name'])} AS name, {lit(category)} AS category) AS n
                ON s.series_id = n.series_id
                WHEN MATCHED THEN UPDATE SET
                  name = n.name,
                  category = CASE WHEN n.category = 'other' THEN s.category ELSE n.category END
                WHEN NOT MATCHED THEN INSERT (series_id, name, category) VALUES (n.series_id, n.name, n.category)
                """
            )
            return
        start = start_at.date()
        end = (fixture.get("end_time") or start_at).date()
        self.run(
            f"""
            MERGE INTO {CATALOG}.series AS s
            USING (
              SELECT {lit(fixture['series_id'])} AS series_id, {lit(fixture['series_name'])} AS name,
                     {lit(category)} AS category, DATE {lit(start.isoformat())} AS start_date,
                     DATE {lit(end.isoformat())} AS end_date
            ) AS n
            ON s.series_id = n.series_id
            WHEN MATCHED THEN UPDATE SET
              name = n.name,
              category = CASE WHEN n.category = 'other' THEN s.category ELSE n.category END,
              start_date = LEAST(s.start_date, n.start_date),
              end_date = GREATEST(s.end_date, n.end_date)
            WHEN NOT MATCHED THEN INSERT (series_id, name, category, start_date, end_date)
              VALUES (n.series_id, n.name, n.category, n.start_date, n.end_date)
            """
        )

    def venue_id(self, fixture: dict) -> int | None:
        if not fixture["venue_name"]:
            return None
        found = self.rows(
            f"""
            SELECT venue_id FROM {CATALOG}.venues
            WHERE name = {lit(fixture['venue_name'])}
              AND city <=> {lit(fixture['venue_city'])}
            """
        )
        if found:
            return int(found[0][0])
        new_id = int(self.rows(f"SELECT COALESCE(MAX(venue_id), 0) + 1 FROM {CATALOG}.venues")[0][0])
        self.run(
            f"""
            INSERT INTO {CATALOG}.venues (venue_id, name, city, country)
            VALUES ({lit(new_id)}, {lit(fixture['venue_name'])}, {lit(fixture['venue_city'])}, {lit(fixture['venue_country'])})
            """
        )
        return new_id

    def insert_match(self, fixture: dict, venue_id: int | None, category: str, officials: dict, has_commentary: bool):
        self.run(
            f"""
            INSERT INTO {CATALOG}.matches (
              match_id, series_id, venue_id, home_team_id, away_team_id, category, format_code,
              status, start_time, end_time, match_desc, status_text, toss_text, result_text, short_score,
              umpire1, umpire2, umpire3, referee, officials_loaded, commentary_loaded
            ) VALUES (
              {lit(fixture['match_id'])}, {lit(fixture['series_id'])}, {lit(venue_id)},
              {lit(fixture['home']['id'])}, {lit(fixture['away']['id'])}, {lit(category)}, {lit(fixture['format_code'])},
              {lit(fixture['status'])}, {lit(fixture['start_time'])}, {lit(fixture['end_time'])}, {lit(fixture['match_desc'])},
              {lit(fixture['status_text'])}, {lit(officials.get('toss'))}, {lit(fixture['status_text'])}, {lit(fixture['short_score'])},
              {lit(officials.get('umpire1'))}, {lit(officials.get('umpire2'))}, {lit(officials.get('umpire3'))}, {lit(officials.get('referee'))},
              {lit(bool(officials.get('umpire1') or officials.get('referee')))}, {lit(has_commentary)}
            )
            """
        )

    def insert_values(self, table: str, columns: list[str], rows: list[tuple]):
        if not rows:
            return
        for start in range(0, len(rows), 100):
            chunk = rows[start : start + 100]
            values = ", ".join("(" + ", ".join(lit(value) for value in row) + ")" for row in chunk)
            self.run(f"INSERT INTO {CATALOG}.{table} ({', '.join(columns)}) VALUES {values}")


class PostgresStore(Store):
    """Same tables in the Docker Postgres database, schema archive."""

    def __init__(self, url: str):
        global CATALOG, SQL_DIALECT
        CATALOG = "archive"
        SQL_DIALECT = "postgres"
        import psycopg

        self.conn = psycopg.connect(url, autocommit=True)
        self.cur = self.conn.cursor()

    def ensure_schema(self):
        self.run("CREATE SCHEMA IF NOT EXISTS archive")
        self.run(
            """
            CREATE TABLE IF NOT EXISTS archive.teams (
              team_id BIGINT PRIMARY KEY,
              name TEXT NOT NULL,
              short_name TEXT
            )
            """
        )
        self.run(
            """
            CREATE TABLE IF NOT EXISTS archive.players (
              player_id BIGINT PRIMARY KEY,
              name TEXT NOT NULL
            )
            """
        )
        self.run(
            """
            CREATE TABLE IF NOT EXISTS archive.series (
              series_id BIGINT PRIMARY KEY,
              name TEXT NOT NULL,
              category TEXT,
              start_date DATE,
              end_date DATE
            )
            """
        )
        self.run(
            """
            CREATE TABLE IF NOT EXISTS archive.venues (
              venue_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
              name TEXT NOT NULL,
              city TEXT,
              country TEXT,
              CONSTRAINT venues_name_city UNIQUE NULLS NOT DISTINCT (name, city)
            )
            """
        )
        self.run(
            """
            CREATE TABLE IF NOT EXISTS archive.matches (
              match_id BIGINT PRIMARY KEY,
              series_id BIGINT REFERENCES archive.series (series_id),
              venue_id BIGINT REFERENCES archive.venues (venue_id),
              home_team_id BIGINT NOT NULL REFERENCES archive.teams (team_id),
              away_team_id BIGINT NOT NULL REFERENCES archive.teams (team_id),
              category TEXT,
              format_code TEXT,
              status TEXT NOT NULL,
              start_time TIMESTAMP,
              end_time TIMESTAMP,
              match_desc TEXT,
              status_text TEXT,
              toss_text TEXT,
              result_text TEXT,
              short_score TEXT,
              umpire1 TEXT,
              umpire2 TEXT,
              umpire3 TEXT,
              referee TEXT,
              officials_loaded BOOLEAN NOT NULL DEFAULT FALSE,
              commentary_loaded BOOLEAN NOT NULL DEFAULT FALSE,
              CONSTRAINT matches_status_chk CHECK (status IN ('finished', 'abandoned', 'cancelled'))
            )
            """
        )
        self.run(
            """
            CREATE TABLE IF NOT EXISTS archive.innings (
              match_id BIGINT NOT NULL REFERENCES archive.matches (match_id) ON DELETE CASCADE,
              innings_number INT NOT NULL,
              batting_team_id BIGINT,
              bowling_team_id BIGINT,
              runs INT,
              wickets INT,
              overs TEXT,
              run_rate DOUBLE PRECISION,
              PRIMARY KEY (match_id, innings_number)
            )
            """
        )
        self.run(
            """
            CREATE TABLE IF NOT EXISTS archive.extras (
              match_id BIGINT NOT NULL,
              innings_number INT NOT NULL,
              total INT,
              wides INT,
              no_balls INT,
              byes INT,
              leg_byes INT,
              penalty INT,
              PRIMARY KEY (match_id, innings_number),
              FOREIGN KEY (match_id, innings_number) REFERENCES archive.innings (match_id, innings_number) ON DELETE CASCADE
            )
            """
        )
        self.run(
            """
            CREATE TABLE IF NOT EXISTS archive.batting (
              match_id BIGINT NOT NULL,
              innings_number INT NOT NULL,
              player_id BIGINT NOT NULL REFERENCES archive.players (player_id),
              bat_order INT,
              runs INT,
              balls INT,
              fours INT,
              sixes INT,
              strike_rate TEXT,
              is_out BOOLEAN,
              dismissal TEXT,
              is_captain BOOLEAN,
              is_keeper BOOLEAN,
              PRIMARY KEY (match_id, innings_number, player_id),
              FOREIGN KEY (match_id, innings_number) REFERENCES archive.innings (match_id, innings_number) ON DELETE CASCADE
            )
            """
        )
        self.run(
            """
            CREATE TABLE IF NOT EXISTS archive.bowling (
              match_id BIGINT NOT NULL,
              innings_number INT NOT NULL,
              player_id BIGINT NOT NULL REFERENCES archive.players (player_id),
              bowl_order INT,
              overs TEXT,
              maidens INT,
              runs INT,
              wickets INT,
              economy TEXT,
              PRIMARY KEY (match_id, innings_number, player_id),
              FOREIGN KEY (match_id, innings_number) REFERENCES archive.innings (match_id, innings_number) ON DELETE CASCADE
            )
            """
        )
        self.run(
            """
            CREATE TABLE IF NOT EXISTS archive.wickets (
              match_id BIGINT NOT NULL,
              innings_number INT NOT NULL,
              wicket_number INT NOT NULL,
              player_id BIGINT,
              player_name TEXT,
              runs INT,
              overs TEXT,
              PRIMARY KEY (match_id, innings_number, wicket_number),
              FOREIGN KEY (match_id, innings_number) REFERENCES archive.innings (match_id, innings_number) ON DELETE CASCADE
            )
            """
        )
        self.run(
            """
            CREATE TABLE IF NOT EXISTS archive.squads (
              match_id BIGINT NOT NULL REFERENCES archive.matches (match_id) ON DELETE CASCADE,
              team_id BIGINT NOT NULL REFERENCES archive.teams (team_id),
              player_id BIGINT NOT NULL REFERENCES archive.players (player_id),
              role TEXT,
              is_captain BOOLEAN,
              is_keeper BOOLEAN,
              is_playing BOOLEAN,
              PRIMARY KEY (match_id, team_id, player_id)
            )
            """
        )
        self.run(
            """
            CREATE TABLE IF NOT EXISTS archive.commentary (
              match_id BIGINT NOT NULL REFERENCES archive.matches (match_id) ON DELETE CASCADE,
              innings_number INT NOT NULL,
              sequence_number INT NOT NULL,
              over_number DOUBLE PRECISION,
              ball_number INT,
              is_delivery BOOLEAN,
              event TEXT,
              batter_id BIGINT,
              batter_name TEXT,
              bowler_id BIGINT,
              bowler_name TEXT,
              runs INT,
              team_score INT,
              commentary_text TEXT NOT NULL,
              PRIMARY KEY (match_id, innings_number, sequence_number)
            )
            """
        )

    def venue_id(self, fixture: dict) -> int | None:
        if not fixture["venue_name"]:
            return None
        found = self.rows(
            f"""
            INSERT INTO archive.venues (name, city, country)
            VALUES ({lit(fixture['venue_name'])}, {lit(fixture['venue_city'])}, {lit(fixture['venue_country'])})
            ON CONFLICT ON CONSTRAINT venues_name_city
            DO UPDATE SET country = COALESCE(EXCLUDED.country, archive.venues.country)
            RETURNING venue_id
            """
        )
        return int(found[0][0])


def load_env_file():
    path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())


def open_store():
    if "--postgres" not in sys.argv:
        return Store()
    load_env_file()
    url = os.environ.get("ARCHIVE_DATABASE_URL", "").strip() or os.environ.get("DATABASE_URL", "").strip()
    if not url:
        raise SystemExit("Set DATABASE_URL to the Docker Postgres database")
    return PostgresStore(url)


def scorecard_rows(match_id: int, raw: dict) -> dict:
    header = raw.get("matchHeader") if isinstance(raw.get("matchHeader"), dict) else {}
    status_text = text(raw.get("status") or header.get("status")) or None
    toss = header.get("tossResults") or header.get("toss")
    toss_text = None
    if isinstance(toss, dict):
        winner = text(toss.get("tossWinnerName"))
        decision = text(toss.get("decision")).lower()
        if winner:
            if "bat" in decision:
                toss_text = f"{winner} elected to bat"
            elif "bowl" in decision or "field" in decision:
                toss_text = f"{winner} elected to field"
            else:
                toss_text = f"{winner} won the toss"
    category = category_of(f"{text(header.get('seriesName'))} {text(header.get('matchType'))}")
    innings_rows = []
    extra_rows = []
    batting_rows = []
    bowling_rows = []
    wicket_rows = []
    squad_rows = []
    players = []
    teams = []
    for card in raw.get("scoreCard") or []:
        if not isinstance(card, dict):
            continue
        number = num(card.get("inningsId"))
        if not number:
            continue
        bat = card.get("batTeamDetails") if isinstance(card.get("batTeamDetails"), dict) else {}
        bowl = card.get("bowlTeamDetails") if isinstance(card.get("bowlTeamDetails"), dict) else {}
        score = card.get("scoreDetails") if isinstance(card.get("scoreDetails"), dict) else {}
        batting_id = num(bat.get("batTeamId"))
        bowling_id = num(bowl.get("bowlTeamId"))
        if batting_id and text(bat.get("batTeamName")):
            teams.append({"id": batting_id, "name": text(bat.get("batTeamName")), "short": text(bat.get("batTeamShortName")) or None})
        if bowling_id and text(bowl.get("bowlTeamName")):
            teams.append({"id": bowling_id, "name": text(bowl.get("bowlTeamName")), "short": text(bowl.get("bowlTeamShortName")) or None})
        rate = score.get("runRate")
        innings_rows.append(
            (
                match_id,
                number,
                batting_id,
                bowling_id,
                int(score.get("runs") or 0),
                int(score.get("wickets") or 0),
                None if score.get("overs") is None else str(score.get("overs")),
                None if rate is None else float(rate),
            )
        )
        extra = extras_of(card.get("extrasData") or score.get("extras"))
        if extra:
            extra_rows.append((match_id, number, extra["total"], extra["wides"], extra["no_balls"], extra["byes"], extra["leg_byes"], extra["penalty"]))
        order = 0
        for batter in numbered(bat.get("batsmenData")):
            if not isinstance(batter, dict):
                continue
            order += 1
            player_id = num(batter.get("batId") or batter.get("batterId") or batter.get("id"))
            name = text(batter.get("batName") or batter.get("name"))
            if not player_id or not name:
                continue
            players.append((player_id, name))
            dismissal = text(batter.get("outDesc")) or None
            is_out = bool(dismissal) and not re.search(r"not out", dismissal, re.I)
            captain = batter.get("captain") is True or batter.get("isCaptain") is True
            keeper = batter.get("keeper") is True or batter.get("isKeeper") is True
            batting_rows.append(
                (
                    match_id,
                    number,
                    player_id,
                    order,
                    int(batter.get("runs") or 0),
                    int(batter.get("balls") or 0),
                    int(batter.get("fours") or 0),
                    int(batter.get("sixes") or 0),
                    text(batter.get("strikeRate")) or None,
                    is_out,
                    dismissal,
                    captain,
                    keeper,
                )
            )
            if batting_id:
                squad_rows.append((match_id, batting_id, player_id, "batter", captain, keeper, True))
        order = 0
        for bowler in numbered(bowl.get("bowlersData")):
            if not isinstance(bowler, dict):
                continue
            order += 1
            player_id = num(bowler.get("bowlerId") or bowler.get("bowlId") or bowler.get("id"))
            name = text(bowler.get("bowlName") or bowler.get("name"))
            if not player_id or not name:
                continue
            players.append((player_id, name))
            bowling_rows.append(
                (
                    match_id,
                    number,
                    player_id,
                    order,
                    None if bowler.get("overs") is None else str(bowler.get("overs")),
                    int(bowler.get("maidens") or 0),
                    int(bowler.get("runs") or 0),
                    int(bowler.get("wickets") or 0),
                    text(bowler.get("economy")) or None,
                )
            )
            if bowling_id:
                squad_rows.append((match_id, bowling_id, player_id, "bowler", False, False, True))
        for wicket in wickets_of(card.get("wicketsData") or card.get("wickets")):
            if wicket["player_id"]:
                players.append((wicket["player_id"], wicket["player_name"]))
            wicket_rows.append((match_id, number, wicket["number"], wicket["player_id"], wicket["player_name"], wicket["runs"], wicket["overs"]))
    return {
        "status_text": status_text,
        "toss_text": toss_text,
        "category": category,
        "innings": innings_rows,
        "extras": extra_rows,
        "batting": batting_rows,
        "bowling": bowling_rows,
        "wickets": wicket_rows,
        "squads": list({(row[0], row[1], row[2]): row for row in squad_rows}.values()),
        "players": players,
        "teams": teams,
    }


def apply_scorecard(fixture: dict, raw: dict) -> bool:
    """Fill gaps from the scorecard. Return false when the match is still in play."""
    header = raw.get("matchHeader") if isinstance(raw.get("matchHeader"), dict) else {}
    state = text(header.get("state"))
    status_text = text(header.get("status") or raw.get("status"))
    decided = concluded_status(state, status_text)
    complete = raw.get("isMatchComplete") is True or raw.get("matchComplete") is True or state.lower() == "complete"
    if fixture["status"] not in ("abandoned", "cancelled") and not complete and decided is None:
        return False
    if decided:
        fixture["status"] = decided
    if status_text:
        fixture["status_text"] = status_text
    venue = header.get("venue") if isinstance(header.get("venue"), dict) else {}
    if text(venue.get("name")):
        fixture["venue_name"] = text(venue.get("name"))
        fixture["venue_city"] = text(venue.get("city")) or fixture.get("venue_city")
        fixture["venue_country"] = text(venue.get("country")) or fixture.get("venue_country")
    start = epoch_ms(header.get("matchStartTimestamp"))
    end = epoch_ms(header.get("matchCompleteTimestamp"))
    if start:
        fixture["start_time"] = start
    if end:
        fixture["end_time"] = end
    series_id = num(header.get("seriesId"))
    if series_id:
        fixture["series_id"] = series_id
    if text(header.get("seriesName")):
        fixture["series_name"] = text(header.get("seriesName"))
    if text(header.get("matchFormat")):
        fixture["format_code"] = format_code(text(header.get("matchFormat")))
    if text(header.get("matchDescription")):
        fixture["match_desc"] = text(header.get("matchDescription"))
    category = category_of(f"{fixture.get('series_name') or ''} {text(header.get('matchType'))} {fixture.get('match_desc') or ''}")
    if category != "other":
        fixture["category"] = category
    return True


def load_match(store: Store, fixture: dict):
    match_id = fixture["match_id"]
    if fixture["status"] not in ("finished", "abandoned", "cancelled"):
        return False
    status, body = fetch(f"https://www.cricbuzz.com/api/mcenter/scorecard/{match_id}")
    time.sleep(0.4)
    raw = json.loads(body) if body.strip() else {}
    if status != 200 or not isinstance(raw, dict):
        if fixture["status"] in ("abandoned", "cancelled"):
            raw = {}
        else:
            raise RuntimeError(f"scorecard HTTP {status}")
    if raw and not apply_scorecard(fixture, raw):
        return False
    if fixture["start_time"] is None:
        fixture["start_time"] = year_anchor(fixture.get("series_name"))
    parsed = scorecard_rows(match_id, raw) if raw else {
        "status_text": fixture.get("status_text"),
        "toss_text": None,
        "category": fixture["category"],
        "innings": [],
        "extras": [],
        "batting": [],
        "bowling": [],
        "wickets": [],
        "squads": [],
        "players": [],
        "teams": [],
    }
    if fixture["status_text"] is None:
        fixture["status_text"] = parsed["status_text"]
    category = parsed["category"] if parsed["category"] != "other" else fixture["category"]
    page_status, page = fetch(f"https://www.cricbuzz.com/live-cricket-scorecard/{match_id}")
    time.sleep(0.4)
    officials = officials_from_html(page) if page_status == 200 else {}
    officials["toss"] = officials.get("toss") or parsed["toss_text"]
    commentary = []
    innings_numbers = [row[1] for row in parsed["innings"]]
    for innings in innings_numbers:
        code, payload = fetch(f"https://www.cricbuzz.com/api/mcenter/{match_id}/full-commentary/{innings}")
        time.sleep(0.4)
        if code == 200 and payload.strip():
            commentary.extend(commentary_lines(json.loads(payload), innings))
    if fixture["status"] == "finished" and not parsed["innings"]:
        return False
    store.clear_match(match_id)
    for team in (fixture["home"], fixture["away"], *parsed["teams"]):
        store.upsert_team(team)
    for player_id, name in dict(parsed["players"]).items():
        store.upsert_player(player_id, name)
    store.upsert_series(fixture, category)
    venue = store.venue_id(fixture)
    store.insert_match(fixture, venue, category, officials, False)
    store.insert_values("innings", ["match_id", "innings_number", "batting_team_id", "bowling_team_id", "runs", "wickets", "overs", "run_rate"], parsed["innings"])
    store.insert_values("extras", ["match_id", "innings_number", "total", "wides", "no_balls", "byes", "leg_byes", "penalty"], parsed["extras"])
    store.insert_values("batting", ["match_id", "innings_number", "player_id", "bat_order", "runs", "balls", "fours", "sixes", "strike_rate", "is_out", "dismissal", "is_captain", "is_keeper"], parsed["batting"])
    store.insert_values("bowling", ["match_id", "innings_number", "player_id", "bowl_order", "overs", "maidens", "runs", "wickets", "economy"], parsed["bowling"])
    store.insert_values("wickets", ["match_id", "innings_number", "wicket_number", "player_id", "player_name", "runs", "overs"], parsed["wickets"])
    store.insert_values("squads", ["match_id", "team_id", "player_id", "role", "is_captain", "is_keeper", "is_playing"], parsed["squads"])
    store.insert_values(
        "commentary",
        [
            "match_id",
            "innings_number",
            "sequence_number",
            "over_number",
            "ball_number",
            "is_delivery",
            "event",
            "batter_id",
            "batter_name",
            "bowler_id",
            "bowler_name",
            "runs",
            "team_score",
            "commentary_text",
        ],
        [
            (
                match_id,
                row["innings"],
                row["sequence"],
                row["over"],
                row["ball"],
                row["is_delivery"],
                row["event"],
                row["batter_id"],
                row["batter_name"],
                row["bowler_id"],
                row["bowler_name"],
                row["runs"],
                row["score"],
                row["text"],
            )
            for row in commentary
        ],
    )
    store.run(f"UPDATE {CATALOG}.matches SET commentary_loaded = TRUE WHERE match_id = {int(match_id)}")
    return True


ARCHIVE_INDEX = "https://www.cricbuzz.com/cricket-scorecard-archives"
SERIES_LINK = re.compile(r'href="(/cricket-series/(\d+)/[^"]+/matches)"')


def option_int(flag: str, default: int) -> int:
    if flag not in sys.argv:
        return default
    position = sys.argv.index(flag)
    if position + 1 >= len(sys.argv):
        raise SystemExit(f"Missing value for {flag}")
    return int(sys.argv[position + 1])


def load_history(store: Store):
    start_year = option_int("--from-year", 1877)
    end_year = option_int("--to-year", datetime.now(timezone.utc).year)
    status, index = fetch(ARCHIVE_INDEX)
    if status != 200:
        raise SystemExit(f"Archive index HTTP {status}")
    years = sorted(int(year) for year in set(re.findall(r"scorecard-archives/(\d{4})", index)) if start_year <= int(year) <= end_year)
    if not years:
        raise SystemExit(f"No archive years from {start_year} to {end_year}")
    print(f"history {years[0]}-{years[-1]} ({len(years)} years)", flush=True)
    stored = 0
    failed = 0
    for year in years:
        try:
            code, html = fetch(f"{ARCHIVE_INDEX}/{year}")
        except Exception as error:
            failed += 1
            print(f"year {year} {error}", file=sys.stderr, flush=True)
            continue
        if code != 200:
            failed += 1
            print(f"year {year} HTTP {code}", file=sys.stderr, flush=True)
            continue
        series = {int(series_id): path for path, series_id in SERIES_LINK.findall(html)}
        print(f"year {year} series {len(series)}", flush=True)
        for series_id, path in series.items():
            try:
                page_code, page = fetch("https://www.cricbuzz.com" + path)
            except Exception as error:
                failed += 1
                print(f"series {series_id} {error}", file=sys.stderr, flush=True)
                continue
            time.sleep(0.2)
            if page_code != 200:
                failed += 1
                print(f"series {series_id} HTTP {page_code}", file=sys.stderr, flush=True)
                continue
            fixtures = [row for row in finished_fixtures(extract_recent_cards(page)) if row["series_id"] == series_id]
            known = store.existing_ids([row["match_id"] for row in fixtures])
            for fixture in fixtures:
                if fixture["match_id"] in known:
                    continue
                label = f"{fixture['match_id']} {fixture['home']['name']} vs {fixture['away']['name']}"
                try:
                    if load_match(store, fixture):
                        stored += 1
                        print(f"stored {label}", flush=True)
                    else:
                        print(f"skipped in play {label}", flush=True)
                except Exception as error:
                    if "duplicate key" in str(error).lower():
                        print(f"already stored {label}", flush=True)
                        continue
                    failed += 1
                    print(f"failed {label}: {error}", file=sys.stderr, flush=True)
        print(f"year {year} done stored {stored} failed {failed}", flush=True)
    print(f"history complete stored {stored} failed {failed}", flush=True)
    if failed:
        raise SystemExit(f"{failed} failure(s), stored {stored}")


def main():
    raise SystemExit(
        "This loader writes an older table shape. Use scripts/databricks_sync.py, "
        "which inserts into the current cricsocc.bronze tables."
    )


if __name__ == "__main__":
    main()
