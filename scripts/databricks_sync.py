#!/usr/bin/env python3
"""Insert finished Cricbuzz matches into the current cricsocc.bronze tables.

The daily GitHub job uses this script. Rows already stored are left as they
are. A match that is not stored yet is written with MERGE for teams, players,
series, and venues, then its match rows are inserted once. Repeating a run
does not add duplicate rows and does not send the old table shape.
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import databricks_daily_finished as source  # noqa: E402
from collect_archive import (  # noqa: E402
    FetchError,
    QualityError,
    SkipMatch,
    build_match,
)

source.SQL_DIALECT = "spark"
CATALOG = "cricsocc.bronze"
CHILD_TABLES = (
    "commentary",
    "powerplays",
    "partnerships",
    "wickets",
    "bowling",
    "batting",
    "extras",
    "innings",
    "awards",
    "squads",
    "match_facts",
)
COLUMNS = {
    "teams": ["team_id", "name", "short_name"],
    "players": ["player_id", "name"],
    "series": ["series_id", "name", "category", "start_time", "end_time", "source_path"],
    "series_years": ["series_id", "archive_year"],
    "venues": [
        "venue_id",
        "cricbuzz_venue_id",
        "name",
        "city",
        "country",
        "timezone",
        "latitude",
        "longitude",
        "capacity",
        "ends",
        "hosts",
    ],
    "matches": [
        "match_id",
        "series_id",
        "venue_id",
        "home_team_id",
        "away_team_id",
        "category",
        "format_code",
        "status",
        "match_desc",
        "start_time",
        "end_time",
        "day_night",
        "toss_text",
        "toss_winner_team_id",
        "toss_decision",
        "result_text",
        "status_text",
        "umpire1",
        "umpire2",
        "umpire3",
        "referee",
        "info_time_text",
        "commentary_loaded",
        "collected_at",
    ],
    "match_facts": ["match_id", "label", "value"],
    "awards": ["match_id", "award", "player_id", "player_name", "team_name"],
    "squads": ["match_id", "team_id", "player_id", "squad_group", "sort_order", "is_captain", "is_keeper"],
    "innings": [
        "match_id",
        "innings_number",
        "batting_team_id",
        "bowling_team_id",
        "runs",
        "wickets",
        "overs",
        "run_rate",
        "time_score",
    ],
    "extras": ["match_id", "innings_number", "total", "wides", "no_balls", "byes", "leg_byes", "penalty"],
    "batting": [
        "match_id",
        "innings_number",
        "player_id",
        "bat_order",
        "runs",
        "balls",
        "dots",
        "fours",
        "sixes",
        "minutes",
        "strike_rate",
        "is_out",
        "dismissal",
        "is_captain",
        "is_keeper",
        "bowler_id",
        "fielder1_id",
        "fielder2_id",
        "fielder3_id",
        "wicket_code",
    ],
    "bowling": [
        "match_id",
        "innings_number",
        "player_id",
        "bowl_order",
        "overs",
        "maidens",
        "runs",
        "wickets",
        "economy",
        "no_balls",
        "wides",
        "dots",
        "balls",
    ],
    "wickets": ["match_id", "innings_number", "wicket_number", "player_id", "player_name", "runs", "overs"],
    "partnerships": [
        "match_id",
        "innings_number",
        "partnership_number",
        "batter1_id",
        "batter1_name",
        "batter1_runs",
        "batter1_balls",
        "batter2_id",
        "batter2_name",
        "batter2_runs",
        "batter2_balls",
        "runs",
        "balls",
    ],
    "powerplays": ["match_id", "innings_number", "powerplay_number", "kind", "overs_from", "overs_to", "runs"],
    "commentary": [
        "match_id",
        "innings_number",
        "sequence_number",
        "over_number",
        "ball_in_over",
        "ball_number",
        "event",
        "batter_id",
        "batter_name",
        "bowler_id",
        "bowler_name",
        "legal_runs",
        "total_runs",
        "team_score",
        "commentary_text",
    ],
}


def load_env() -> None:
    path = Path(__file__).resolve().parents[1] / ".env"
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def cell(value) -> str:
    if isinstance(value, datetime):
        return f"CAST({source.lit(value)} AS TIMESTAMP_NTZ)"
    return source.lit(value)


CONNECT_ATTEMPTS = int(os.environ.get("DATABRICKS_CONNECT_ATTEMPTS", "12"))
CONNECT_BASE_SLEEP = float(os.environ.get("DATABRICKS_CONNECT_BASE_SLEEP", "5"))
STATEMENT_ATTEMPTS = int(os.environ.get("DATABRICKS_STATEMENT_ATTEMPTS", "6"))


def _transient(error: BaseException) -> bool:
    text = str(error).lower()
    markers = (
        "bad_request",
        "cannot create the resource",
        "try again later",
        "temporarily unavailable",
        "timeout",
        "timed out",
        "connection reset",
        "connection refused",
        "broken pipe",
        "service unavailable",
        "too many requests",
        "rate limit",
        "warehouse is stopped",
        "warehouse is starting",
        "pending",
        "throttl",
    )
    return any(marker in text for marker in markers)


def _sleep(attempt: int, base: float) -> None:
    delay = min(base * (2 ** (attempt - 1)), 60.0)
    print(f"databricks retry sleeping {delay:.1f}s before attempt {attempt + 1}", flush=True)
    time.sleep(delay)


def warehouse_id_from_path(http_path: str) -> str | None:
    marker = "/warehouses/"
    if marker not in http_path:
        return None
    return http_path.split(marker, 1)[1].strip("/").split("/")[0] or None


def wake_sql_warehouse(host: str, token: str, warehouse_id: str) -> None:
    """Start the SQL warehouse if it is stopped/stopping, then wait until running."""
    import urllib.error
    import urllib.request
    import json

    base = f"https://{host}/api/2.0/sql/warehouses/{warehouse_id}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def call(method: str, url: str) -> dict:
        request = urllib.request.Request(url, method=method, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                body = response.read().decode("utf-8")
                return json.loads(body) if body else {}
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"warehouse API HTTP {error.code}: {detail[:400]}") from error

    info = call("GET", base)
    state = str(info.get("state") or "").upper()
    print(f"databricks warehouse {warehouse_id} state {state or 'unknown'}", flush=True)
    if state in {"STOPPED", "STOPPING"}:
        print(f"databricks starting warehouse {warehouse_id}", flush=True)
        call("POST", f"{base}/start")
    deadline = time.time() + 300
    while time.time() < deadline:
        info = call("GET", base)
        state = str(info.get("state") or "").upper()
        if state == "RUNNING":
            print(f"databricks warehouse {warehouse_id} running", flush=True)
            return
        print(f"databricks warehouse {warehouse_id} state {state or 'unknown'}; waiting", flush=True)
        time.sleep(5)
    raise RuntimeError(f"warehouse {warehouse_id} did not become RUNNING in time (last state {state})")


class BronzeStore:
    def __init__(self):
        self.host = os.environ.get("DATABRICKS_SERVER_HOSTNAME", "").strip()
        self.path = os.environ.get("DATABRICKS_HTTP_PATH", "").strip()
        self.token = os.environ.get("DATABRICKS_TOKEN", "").strip()
        if not self.host or not self.path or not self.token:
            raise SystemExit("Set DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, and DATABRICKS_TOKEN")
        warehouse_id = warehouse_id_from_path(self.path)
        if warehouse_id:
            try:
                wake_sql_warehouse(self.host, self.token, warehouse_id)
            except Exception as error:
                # Still try to connect; retries below cover transient wake races.
                print(f"databricks warehouse wake skipped: {error}", flush=True)
        self.conn = None
        self.cur = None
        self._connect()

    def _connect(self) -> None:
        from databricks import sql

        last_error: BaseException | None = None
        for attempt in range(1, CONNECT_ATTEMPTS + 1):
            try:
                if self.cur is not None:
                    try:
                        self.cur.close()
                    except Exception:
                        pass
                if self.conn is not None:
                    try:
                        self.conn.close()
                    except Exception:
                        pass
                print(f"databricks connecting attempt {attempt}/{CONNECT_ATTEMPTS}", flush=True)
                self.conn = sql.connect(
                    server_hostname=self.host,
                    http_path=self.path,
                    access_token=self.token,
                )
                self.cur = self.conn.cursor()
                print("databricks connected", flush=True)
                return
            except BaseException as error:
                last_error = error
                if attempt >= CONNECT_ATTEMPTS or not _transient(error):
                    raise
                print(f"databricks connect failed attempt {attempt}/{CONNECT_ATTEMPTS}: {error}", flush=True)
                _sleep(attempt, CONNECT_BASE_SLEEP)
        raise last_error  # pragma: no cover

    def close(self) -> None:
        if self.cur is not None:
            self.cur.close()
        if self.conn is not None:
            self.conn.close()

    def _execute(self, statement: str) -> None:
        last_error: BaseException | None = None
        for attempt in range(1, STATEMENT_ATTEMPTS + 1):
            try:
                self.cur.execute(statement)
                return
            except BaseException as error:
                last_error = error
                if attempt >= STATEMENT_ATTEMPTS or not _transient(error):
                    raise
                print(
                    f"databricks statement failed attempt {attempt}/{STATEMENT_ATTEMPTS}: {error}",
                    flush=True,
                )
                _sleep(attempt, CONNECT_BASE_SLEEP)
                self._connect()
        raise last_error  # pragma: no cover

    def run(self, statement: str) -> None:
        self._execute(statement)
        try:
            self.cur.fetchall()
        except Exception:
            return

    def rows(self, statement: str) -> list[tuple]:
        self._execute(statement)
        return list(self.cur.fetchall())

    def existing_ids(self, match_ids: list[int]) -> set[int]:
        if not match_ids:
            return set()
        listed = ",".join(str(int(match_id)) for match_id in match_ids)
        found = self.rows(f"SELECT match_id FROM {CATALOG}.matches WHERE match_id IN ({listed})")
        return {int(row[0]) for row in found}

    def merge_teams(self, teams: list[dict]) -> None:
        unique = {}
        for team in teams:
            if team and team.get("id") and team.get("name"):
                unique[int(team["id"])] = team
        for team in unique.values():
            self.run(
                f"""
                MERGE INTO {CATALOG}.teams AS t
                USING (
                  SELECT {cell(int(team['id']))} AS team_id,
                         {cell(team['name'])} AS name,
                         {cell(team.get('short'))} AS short_name
                ) AS s
                ON t.team_id = s.team_id
                WHEN MATCHED THEN UPDATE SET
                  name = s.name,
                  short_name = COALESCE(s.short_name, t.short_name)
                WHEN NOT MATCHED THEN INSERT (team_id, name, short_name)
                  VALUES (s.team_id, s.name, s.short_name)
                """
            )

    def merge_players(self, players: list[tuple[int, str]]) -> None:
        unique: dict[int, str] = {}
        for player_id, name in players:
            if not player_id or not name:
                continue
            current = unique.get(int(player_id))
            if current is None or len(name) > len(current):
                unique[int(player_id)] = name
        items = list(unique.items())
        for start in range(0, len(items), 40):
            chunk = items[start : start + 40]
            selects = " UNION ALL ".join(
                f"SELECT {cell(player_id)} AS player_id, {cell(name)} AS name" for player_id, name in chunk
            )
            self.run(
                f"""
                MERGE INTO {CATALOG}.players AS p
                USING ({selects}) AS s
                ON p.player_id = s.player_id
                WHEN MATCHED AND length(s.name) > length(p.name) THEN UPDATE SET name = s.name
                WHEN NOT MATCHED THEN INSERT (player_id, name) VALUES (s.player_id, s.name)
                """
            )

    def merge_series(self, match: dict, source_path: str, archive_year: int | None) -> None:
        self.run(
            f"""
            MERGE INTO {CATALOG}.series AS t
            USING (
              SELECT {cell(match['series_id'])} AS series_id,
                     {cell(match['series_name'])} AS name,
                     {cell(match['category'])} AS category,
                     CAST(NULL AS TIMESTAMP_NTZ) AS start_time,
                     CAST(NULL AS TIMESTAMP_NTZ) AS end_time,
                     {cell(source_path)} AS source_path
            ) AS s
            ON t.series_id = s.series_id
            WHEN MATCHED THEN UPDATE SET
              name = s.name,
              category = CASE WHEN s.category = 'other' THEN t.category ELSE s.category END,
              source_path = COALESCE(s.source_path, t.source_path)
            WHEN NOT MATCHED THEN INSERT (series_id, name, category, start_time, end_time, source_path)
              VALUES (s.series_id, s.name, s.category, s.start_time, s.end_time, s.source_path)
            """
        )
        if archive_year is None or archive_year < 1800:
            return
        self.run(
            f"""
            MERGE INTO {CATALOG}.series_years AS t
            USING (
              SELECT {cell(match['series_id'])} AS series_id, {cell(int(archive_year))} AS archive_year
            ) AS s
            ON t.series_id = s.series_id AND t.archive_year = s.archive_year
            WHEN NOT MATCHED THEN INSERT (series_id, archive_year) VALUES (s.series_id, s.archive_year)
            """
        )

    def venue_id(self, venue: dict) -> int | None:
        if not venue.get("name"):
            return None
        cricbuzz_id = venue.get("cricbuzz_id")
        if cricbuzz_id:
            found = self.rows(
                f"SELECT venue_id FROM {CATALOG}.venues WHERE cricbuzz_venue_id = {cell(int(cricbuzz_id))}"
            )
        else:
            found = self.rows(
                f"""
                SELECT venue_id FROM {CATALOG}.venues
                WHERE name = {cell(venue['name'])} AND city <=> {cell(venue.get('city'))}
                """
            )
        if found:
            venue_id = int(found[0][0])
            self.run(
                f"""
                UPDATE {CATALOG}.venues
                SET country = COALESCE({cell(venue.get('country'))}, country),
                    timezone = COALESCE({cell(venue.get('timezone'))}, timezone),
                    latitude = COALESCE({cell(venue.get('latitude'))}, latitude),
                    longitude = COALESCE({cell(venue.get('longitude'))}, longitude),
                    capacity = COALESCE({cell(venue.get('capacity'))}, capacity),
                    ends = COALESCE({cell(venue.get('ends'))}, ends),
                    hosts = COALESCE({cell(venue.get('hosts'))}, hosts)
                WHERE venue_id = {venue_id}
                """
            )
            return venue_id
        venue_id = int(self.rows(f"SELECT COALESCE(MAX(venue_id), 0) + 1 FROM {CATALOG}.venues")[0][0])
        self.insert_rows(
            "venues",
            [
                (
                    venue_id,
                    int(cricbuzz_id) if cricbuzz_id else None,
                    venue["name"],
                    venue.get("city"),
                    venue.get("country"),
                    venue.get("timezone"),
                    venue.get("latitude"),
                    venue.get("longitude"),
                    venue.get("capacity"),
                    venue.get("ends"),
                    venue.get("hosts"),
                )
            ],
        )
        return venue_id

    def insert_rows(self, table: str, rows: list[tuple]) -> None:
        columns = COLUMNS[table]
        if not rows:
            return
        width = len(columns)
        for row in rows:
            if len(row) != width:
                raise RuntimeError(f"{table} row has {len(row)} values, expected {width}")
        listed = ", ".join(f"`{column}`" for column in columns)
        for start in range(0, len(rows), 20):
            chunk = rows[start : start + 20]
            values = ", ".join("(" + ", ".join(cell(value) for value in row) + ")" for row in chunk)
            self.run(f"INSERT INTO {CATALOG}.{table} ({listed}) VALUES {values}")

    def save(self, bundle: dict) -> None:
        match = bundle["match"]
        parsed = bundle["parsed"]
        match_id = int(match["match_id"])
        if self.existing_ids([match_id]):
            return
        players = list(parsed["players"])
        for person in bundle["squads"]:
            players.append((person["player_id"], person["name"]))
        self.merge_teams([match["home"], match["away"]])
        self.merge_players(players)
        self.merge_series(match, bundle["source_path"], bundle.get("archive_year"))
        venue_id = self.venue_id(bundle["venue"])
        for table in CHILD_TABLES:
            self.run(f"DELETE FROM {CATALOG}.{table} WHERE match_id = {match_id}")
        if bundle["facts"]:
            self.insert_rows(
                "match_facts",
                [(match_id, label, value) for label, value in bundle["facts"].items() if label and value],
            )
        if bundle["squads"]:
            self.insert_rows(
                "squads",
                [
                    (
                        match_id,
                        person["team_id"],
                        person["player_id"],
                        person["group"],
                        person["order"],
                        person["captain"],
                        person["keeper"],
                    )
                    for person in bundle["squads"]
                ],
            )
        if parsed["awards"]:
            self.insert_rows(
                "awards",
                [
                    (match_id, row["award"], row["player_id"], row["player_name"], row["team_name"])
                    for row in parsed["awards"]
                ],
            )
        if parsed["innings"]:
            self.insert_rows(
                "innings",
                [
                    (
                        match_id,
                        row["number"],
                        row["batting_team_id"],
                        row["bowling_team_id"],
                        row["runs"],
                        row["wickets"],
                        row["overs"],
                        row["run_rate"],
                        row["time_score"],
                    )
                    for row in parsed["innings"]
                ],
            )
        extras_rows = list(parsed["extras"])
        covered = {row["innings"] for row in extras_rows}
        for innings in parsed["innings"]:
            batter_runs = sum(row["runs"] for row in parsed["batting"] if row["innings"] == innings["number"])
            if innings["number"] not in covered and batter_runs == innings["runs"]:
                extras_rows.append(
                    {
                        "innings": innings["number"],
                        "total": 0,
                        "wides": 0,
                        "no_balls": 0,
                        "byes": 0,
                        "leg_byes": 0,
                        "penalty": 0,
                    }
                )
        if extras_rows:
            self.insert_rows(
                "extras",
                [
                    (
                        match_id,
                        row["innings"],
                        row["total"],
                        row["wides"],
                        row["no_balls"],
                        row["byes"],
                        row["leg_byes"],
                        row["penalty"],
                    )
                    for row in extras_rows
                ],
            )
        if parsed["batting"]:
            self.insert_rows(
                "batting",
                [
                    (
                        match_id,
                        row["innings"],
                        row["player_id"],
                        row["order"],
                        row["runs"],
                        row["balls"],
                        row["dots"],
                        row["fours"],
                        row["sixes"],
                        row["minutes"],
                        row["strike_rate"],
                        row["is_out"],
                        row["dismissal"],
                        row["is_captain"],
                        row["is_keeper"],
                        row["bowler_id"],
                        row["fielder1_id"],
                        row["fielder2_id"],
                        row["fielder3_id"],
                        row["wicket_code"],
                    )
                    for row in parsed["batting"]
                ],
            )
        if parsed["bowling"]:
            self.insert_rows(
                "bowling",
                [
                    (
                        match_id,
                        row["innings"],
                        row["player_id"],
                        row["order"],
                        row["overs"],
                        row["maidens"],
                        row["runs"],
                        row["wickets"],
                        row["economy"],
                        row["no_balls"],
                        row["wides"],
                        row["dots"],
                        row["balls"],
                    )
                    for row in parsed["bowling"]
                ],
            )
        if parsed["wickets"]:
            self.insert_rows(
                "wickets",
                [
                    (
                        match_id,
                        row["innings"],
                        row["number"],
                        row["player_id"],
                        row["player_name"],
                        row["runs"],
                        row["overs"],
                    )
                    for row in parsed["wickets"]
                ],
            )
        if parsed["partnerships"]:
            self.insert_rows(
                "partnerships",
                [
                    (
                        match_id,
                        row["innings"],
                        row["number"],
                        row["batter1_id"],
                        row["batter1_name"],
                        row["batter1_runs"],
                        row["batter1_balls"],
                        row["batter2_id"],
                        row["batter2_name"],
                        row["batter2_runs"],
                        row["batter2_balls"],
                        row["runs"],
                        row["balls"],
                    )
                    for row in parsed["partnerships"]
                ],
            )
        if parsed["powerplays"]:
            self.insert_rows(
                "powerplays",
                [
                    (
                        match_id,
                        row["innings"],
                        row["number"],
                        row["kind"],
                        row["overs_from"],
                        row["overs_to"],
                        row["runs"],
                    )
                    for row in parsed["powerplays"]
                ],
            )
        if bundle["commentary"]:
            self.insert_rows(
                "commentary",
                [
                    (
                        match_id,
                        row["innings"],
                        row["sequence"],
                        row["over"],
                        row["ball_in_over"],
                        row["ball"],
                        row["event"],
                        row["batter_id"],
                        row["batter_name"],
                        row["bowler_id"],
                        row["bowler_name"],
                        row["legal_runs"],
                        row["total_runs"],
                        row["team_score"],
                        row["text"],
                    )
                    for row in bundle["commentary"]
                ],
            )
        self.insert_rows(
            "matches",
            [
                (
                    match_id,
                    match["series_id"],
                    venue_id,
                    match["home"]["id"],
                    match["away"]["id"],
                    match["category"],
                    match["format_code"],
                    match["status"],
                    match["match_desc"],
                    match["start_time"],
                    match["end_time"],
                    match["day_night"],
                    match["toss_text"],
                    match["toss_winner_id"],
                    match["toss_decision"],
                    match["result_text"],
                    match["status_text"],
                    match["umpire1"],
                    match["umpire2"],
                    match["umpire3"],
                    match["referee"],
                    match["info_time_text"],
                    bool(bundle["commentary"]),
                    datetime.now(timezone.utc).replace(tzinfo=None),
                )
            ],
        )
        landed = self.rows(
            f"SELECT match_id FROM {CATALOG}.matches WHERE match_id = {match_id}"
        )
        if not landed:
            raise RuntimeError(f"{match_id} did not land in {CATALOG}.matches")


def recent_finished() -> list[dict]:
    status, html = source.fetch(source.RECENT_URL)
    if status != 200:
        raise SystemExit(f"Recent matches page HTTP {status}")
    return [
        fixture
        for fixture in source.finished_fixtures(source.extract_recent_cards(html))
        if fixture["status"] == "finished"
    ]


def check_schema(store: BronzeStore) -> None:
    found: dict[str, set[str]] = {table: set() for table in COLUMNS}
    names = list(COLUMNS)
    listed = ", ".join(source.lit(name) for name in names)
    for table, column in store.rows(
        f"""
        SELECT table_name, column_name
        FROM cricsocc.information_schema.columns
        WHERE table_schema = 'bronze' AND table_name IN ({listed})
        """
    ):
        found[str(table)].add(str(column))
    missing = {
        table: [column for column in columns if column not in found[table]]
        for table, columns in COLUMNS.items()
        if any(column not in found[table] for column in columns)
    }
    if missing:
        raise SystemExit(f"Databricks is missing columns the daily insert writes: {missing}")
    print("bronze columns match the daily insert", flush=True)


def main() -> None:
    load_env()
    if "--list" in sys.argv:
        fixtures = recent_finished()
        for fixture in fixtures:
            print(f"{fixture['match_id']}\t{fixture['home']['name']} vs {fixture['away']['name']}")
        print(f"{len(fixtures)} finished")
        return
    store = BronzeStore()
    try:
        if "--check" in sys.argv:
            check_schema(store)
            return
        fixtures = recent_finished()
        known = store.existing_ids([fixture["match_id"] for fixture in fixtures])
        missing = [fixture for fixture in fixtures if fixture["match_id"] not in known]
        print(f"Recent finished {len(fixtures)}, already stored {len(known)}, new {len(missing)}", flush=True)
        failed = 0
        for fixture in missing:
            match_id = fixture["match_id"]
            label = f"{match_id} {fixture['home']['name']} vs {fixture['away']['name']}"
            try:
                bundle = build_match(match_id)
            except SkipMatch as skipped:
                print(f"skipped {label} {skipped}", flush=True)
                continue
            except QualityError as error:
                print(f"skipped {label}: {error}", file=sys.stderr, flush=True)
                continue
            except (FetchError, RuntimeError) as error:
                failed += 1
                print(f"failed {label}: {error}", file=sys.stderr, flush=True)
                continue
            try:
                store.save(bundle)
            except Exception as error:
                failed += 1
                print(f"failed {label}: {error}", file=sys.stderr, flush=True)
                continue
            print(
                f"stored {label} commentary {len(bundle['commentary'])} "
                f"dropped_notes {bundle['commentary_dropped']}",
                flush=True,
            )
        if failed:
            raise SystemExit(f"{failed} match(es) failed")
        total = store.rows(f"SELECT COUNT(*) FROM {CATALOG}.matches")[0][0]
        recent = store.rows(
            f"""
            SELECT match_id, status_text, collected_at
            FROM {CATALOG}.matches
            WHERE collected_at >= current_timestamp() - INTERVAL 2 DAYS
            ORDER BY collected_at DESC
            """
        )
        print(f"databricks {CATALOG}.matches total {total}", flush=True)
        print(f"databricks matches collected in last 2 days {len(recent)}", flush=True)
        for row in recent:
            print(f"in databricks {row[0]} {row[1]} {row[2]}", flush=True)
    finally:
        store.close()


if __name__ == "__main__":
    main()
