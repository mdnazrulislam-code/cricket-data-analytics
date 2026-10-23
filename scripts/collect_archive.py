#!/usr/bin/env python3
"""Collect Cricbuzz scorecard archives into Docker Postgres.

Each archive year is read first. Every series on that year is stored, then
each concluded match in those series. Match info, the scorecard, and ball
commentary are stored together or not at all.

Commentary rows are deliveries only. Preview lines, welcomes, and other
notes (ball number 0) are dropped. A concluded match with no commentary is
still stored.

Usage:
  python scripts/collect_archive.py
  python scripts/collect_archive.py --from-year 2026 --to-year 2026
  python scripts/collect_archive.py --match 170727
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parent))
from databricks_daily_finished import (  # noqa: E402
    concluded_status,
    extract_recent_cards,
    fetch,
    finished_fixtures,
    format_code,
    num,
    text,
)

ARCHIVE_INDEX = "https://www.cricbuzz.com/cricket-scorecard-archives"
ROOT = Path(__file__).resolve().parents[1]
DELAY = float(os.environ.get("ARCHIVE_DELAY", "0.3"))

FACT_ROW = re.compile(
    r'<div class="font-bold">([^<]+)</div>\s*(?:<div>([^<]+)</div>|<a[^>]*>\s*(?:<div>)?([^<]+))',
    re.I,
)
SQUAD_SPLIT = re.compile(
    r'class="(?:font-bold w-10/12|w-10/12 font-bold)">\s*([^<]+?)<!--\s*-->\s*squad',
    re.I,
)
SECTION_MARK = re.compile(r'font-bold">\s*(Players|Bench|Support Staff)\s*</div>', re.I)
PLAYER_TOKEN = re.compile(
    r'<span class="hover:underline">([^<]*(?:<!--.*?-->[^<]*)*)'
    r'|href="/profiles/(\d+)/[^"]*"[^>]*title="View Profile Of ([^"]+)"'
    r'|title="View Profile Of ([^"]+)"[^>]*href="/profiles/(\d+)/'
    r'|href="/profiles/(\d+)/[^"]*"[^>]*>\s*([^<]+)',
    re.I,
)
SERIES_MAP = re.compile(r'"(\w*Data)"\s*:\s*\{\s*"seriesMapProto"\s*:\s*', re.I)
MARKER = re.compile(r"\b[A-Z]\d+\$")
ROLE_MARK = re.compile(r"\s*\((?:c\s*&\s*wk|c|wk|w)\)", re.I)


class QualityError(Exception):
    pass


class FetchError(Exception):
    pass


class SkipMatch(Exception):
    pass


def pause() -> None:
    if DELAY > 0:
        time.sleep(DELAY)


def clean(value) -> str:
    return re.sub(r"\s+", " ", text(value)).strip(" ,")


def visible(fragment: str) -> str:
    return clean(re.sub(r"<!--.*?-->", " ", fragment, flags=re.S))


def plain_commentary(value) -> str:
    return clean(MARKER.sub(" ", text(value)))


def epoch(value) -> datetime | None:
    try:
        stamp = int(value)
    except (TypeError, ValueError):
        return None
    if stamp <= 0:
        return None
    if stamp < 10**11:
        stamp *= 1000
    try:
        parsed = datetime.fromtimestamp(stamp / 1000, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None
    if parsed.year < 1800 or parsed.year > datetime.now(timezone.utc).year + 2:
        return None
    return parsed


def category_name(key: str, series_name: str) -> str:
    if "women" in series_name.lower() or "women" in key.lower():
        return "women"
    if "league" in key.lower():
        return "league"
    if "domestic" in key.lower():
        return "domestic"
    if "international" in key.lower():
        return "international"
    return "other"


def blank(value) -> str | None:
    cleaned = clean(value)
    return cleaned or None


def whole(value) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def ball_in_over(over: float | None) -> int | None:
    if over is None:
        return None
    return int(f"{over:.1f}".split(".")[1])


def decimal(value) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def player_name(display: str) -> tuple[str, bool, bool]:
    captain = bool(re.search(r"\((?:c|c\s*&\s*wk)\)", display, re.I))
    keeper = bool(re.search(r"\((?:wk|w|c\s*&\s*wk)\)", display, re.I))
    name = ROLE_MARK.sub("", display)
    name = clean(name)
    return name, captain, keeper


def add_fact(facts: dict[str, str], label: str, value: str) -> None:
    label = clean(label)
    value = clean(value)
    if not label or not value:
        return
    if label not in facts:
        facts[label] = value
        return
    if facts[label] == value:
        return
    copy = 2
    while f"{label} ({copy})" in facts:
        copy += 1
    facts[f"{label} ({copy})"] = value


SKIP_FACTS = {"players", "bench", "support staff"}


def parse_facts(html: str) -> dict[str, str]:
    facts: dict[str, str] = {}
    for label, direct, linked in FACT_ROW.findall(html):
        if clean(label).casefold() in SKIP_FACTS:
            continue
        add_fact(facts, label, direct or linked)
    return facts


def parse_squads(html: str) -> list[dict]:
    parts = SQUAD_SPLIT.split(html)
    people = []
    for index in range(1, len(parts), 2):
        team_name = clean(parts[index])
        chunk = parts[index + 1]
        marks = list(SECTION_MARK.finditer(chunk))
        for mark_at, mark in enumerate(marks):
            group = {"players": "playing", "bench": "bench", "support staff": "support"}[mark.group(1).lower()]
            end = marks[mark_at + 1].start() if mark_at + 1 < len(marks) else len(chunk)
            section = chunk[mark.end() : end]
            pending = None
            seen: set[int] = set()
            order = 0
            for token in PLAYER_TOKEN.finditer(section):
                if token.group(1):
                    pending = visible(token.group(1))
                    continue
                if token.group(2) and token.group(3):
                    player_id, titled = int(token.group(2)), clean(token.group(3))
                elif token.group(4) and token.group(5):
                    titled, player_id = clean(token.group(4)), int(token.group(5))
                elif token.group(6) and token.group(7):
                    player_id, titled = int(token.group(6)), clean(token.group(7))
                else:
                    continue
                display = pending or titled
                pending = None
                if player_id in seen:
                    continue
                seen.add(player_id)
                name, captain, keeper = player_name(display)
                if not name:
                    continue
                order += 1
                people.append(
                    {
                        "team_name": team_name,
                        "player_id": player_id,
                        "name": name,
                        "group": group,
                        "order": order,
                        "captain": captain,
                        "keeper": keeper,
                    }
                )
    return people


def series_slugs(html: str) -> dict[int, str]:
    found = {}
    for series_id, slug in re.findall(r"/cricket-series/(\d+)/([a-z0-9-]+)", html, re.I):
        if slug.lower() == "matches":
            continue
        found.setdefault(int(series_id), slug)
    return found


def parse_year_series(html: str, archive_year: int) -> list[dict]:
    if '\\"' in html:
        html = html.replace('\\"', '"')
    slugs = series_slugs(html)
    decoder = json.JSONDecoder()
    rows = []
    seen: set[int] = set()
    for match in SERIES_MAP.finditer(html):
        try:
            payload, _ = decoder.raw_decode(html[match.end() :])
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, list):
            continue
        key = match.group(1)
        for block in payload:
            if not isinstance(block, dict):
                continue
            for item in block.get("series") or []:
                if not isinstance(item, dict):
                    continue
                series_id = num(item.get("id"))
                name = blank(item.get("name"))
                if not series_id or not name or series_id in seen:
                    continue
                seen.add(series_id)
                slug = slugs.get(series_id)
                path = f"/cricket-series/{series_id}/{slug}/matches" if slug else f"/cricket-series/{series_id}/matches"
                rows.append(
                    {
                        "series_id": series_id,
                        "name": name,
                        "category": category_name(key, name),
                        "start_time": epoch(item.get("startDt")),
                        "end_time": epoch(item.get("endDt")),
                        "source_path": path,
                        "archive_year": archive_year,
                    }
                )
    return rows


def cards_from(html: str) -> list[dict]:
    cards = extract_recent_cards(html)
    if cards:
        return cards
    if '\\"' in html:
        return []
    return extract_recent_cards(html.replace('"', '\\"'))


def team_of(raw) -> dict | None:
    if not isinstance(raw, dict):
        return None
    team_id = num(raw.get("teamId") or raw.get("id"))
    name = blank(raw.get("teamName") or raw.get("name"))
    if not team_id or not name:
        return None
    return {"id": team_id, "name": name, "short": blank(raw.get("teamSName") or raw.get("shortName"))}


def split_umpires(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def extras_of(raw) -> dict | None:
    if not isinstance(raw, dict):
        return None
    row = {
        "total": whole(raw.get("total")),
        "wides": whole(raw.get("wides")) or 0,
        "no_balls": whole(raw.get("no_balls") or raw.get("noBalls")) or 0,
        "byes": whole(raw.get("byes")) or 0,
        "leg_byes": whole(raw.get("legByes") or raw.get("leg_byes")) or 0,
        "penalty": whole(raw.get("penalty")) or 0,
    }
    if row["total"] is None:
        row["total"] = row["wides"] + row["no_balls"] + row["byes"] + row["leg_byes"] + row["penalty"]
    return row


def numbered(raw) -> list:
    if isinstance(raw, list):
        return raw
    if not isinstance(raw, dict):
        return []
    def sort_key(item):
        return num(re.sub(r"\D", "", item[0])) or 0
    return [value for _, value in sorted(raw.items(), key=sort_key)]


def delivery_lines(payload: dict, innings: int) -> tuple[list[dict], int]:
    """Return ball commentary and how many non-ball lines were dropped."""
    dropped = 0
    rows = []
    for block in payload.get("commentary") or []:
        if not isinstance(block, dict):
            continue
        items = [item for item in (block.get("commentaryList") or []) if isinstance(item, dict)]
        for original, item in enumerate(items):
            ball = whole(item.get("ballNbr")) or 0
            comment = plain_commentary(item.get("commText"))
            batter = item.get("batsmanStriker") if isinstance(item.get("batsmanStriker"), dict) else {}
            bowler = item.get("bowlerStriker") if isinstance(item.get("bowlerStriker"), dict) else {}
            batter_id = num(batter.get("batId"))
            bowler_id = num(bowler.get("bowlId"))
            if ball <= 0 or not comment or (not batter_id and not bowler_id):
                dropped += 1
                continue
            event = blank(item.get("event"))
            if event and event.upper() == "NONE":
                event = None
            over = decimal(item.get("overNumber"))
            rows.append(
                {
                    "original": original,
                    "innings": innings,
                    "over": over,
                    "ball": ball,
                    "ball_in_over": ball_in_over(over),
                    "event": event,
                    "batter_id": batter_id,
                    "batter_name": blank(batter.get("batName")),
                    "bowler_id": bowler_id,
                    "bowler_name": blank(bowler.get("bowlName")),
                    "legal_runs": whole(item.get("legalRuns")),
                    "total_runs": whole(item.get("totalRuns")),
                    "team_score": whole(item.get("batTeamScore")),
                    "text": comment,
                }
            )
    rows.sort(key=lambda row: (row["ball"], row["original"]))
    for sequence, row in enumerate(rows, start=1):
        row["sequence"] = sequence
    return rows, dropped


def parse_scorecard(match_id: int, raw: dict) -> dict:
    header = raw.get("matchHeader") if isinstance(raw.get("matchHeader"), dict) else {}
    venue_raw = header.get("venue") if isinstance(header.get("venue"), dict) else {}
    toss = header.get("tossResults") if isinstance(header.get("tossResults"), dict) else {}
    decision = blank(toss.get("decision"))
    decision_code = None
    if decision:
        lowered = decision.lower()
        if "bat" in lowered:
            decision_code = "bat"
        elif "bowl" in lowered or "field" in lowered:
            decision_code = "bowl"
    innings = []
    batting = []
    bowling = []
    wickets = []
    extras = []
    partnerships = []
    powerplays = []
    players: list[tuple[int, str]] = []
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
        innings.append(
            {
                "number": number,
                "batting_team_id": batting_id,
                "bowling_team_id": bowling_id,
                "runs": whole(score.get("runs")) or 0,
                "wickets": whole(score.get("wickets")) or 0,
                "overs": blank(score.get("overs")),
                "run_rate": decimal(score.get("runRate")),
                "time_score": blank(card.get("timeScore")) if not isinstance(card.get("timeScore"), (dict, list)) else None,
            }
        )
        extra = extras_of(card.get("extrasData") or score.get("extras"))
        if extra:
            extras.append({"innings": number, **extra})
        order = 0
        for batter in numbered(bat.get("batsmenData")):
            if not isinstance(batter, dict):
                continue
            player_id = num(batter.get("batId") or batter.get("id"))
            name = blank(batter.get("batName") or batter.get("name"))
            if not player_id or not name:
                continue
            order += 1
            players.append((player_id, name))
            dismissal = blank(batter.get("outDesc"))
            batting.append(
                {
                    "innings": number,
                    "player_id": player_id,
                    "name": name,
                    "order": order,
                    "runs": whole(batter.get("runs")) or 0,
                    "balls": whole(batter.get("balls")) or 0,
                    "dots": whole(batter.get("dots")),
                    "fours": whole(batter.get("fours")) or 0,
                    "sixes": whole(batter.get("sixes")) or 0,
                    "minutes": whole(batter.get("mins")),
                    "strike_rate": decimal(batter.get("strikeRate")),
                    "is_out": bool(dismissal) and not re.search(r"\bnot out\b", dismissal, re.I),
                    "dismissal": dismissal,
                    "is_captain": batter.get("isCaptain") is True,
                    "is_keeper": batter.get("isKeeper") is True,
                    "bowler_id": num(batter.get("bowlerId")),
                    "fielder1_id": num(batter.get("fielderId1")),
                    "fielder2_id": num(batter.get("fielderId2")),
                    "fielder3_id": num(batter.get("fielderId3")),
                    "wicket_code": blank(batter.get("wicketCode")),
                }
            )
        order = 0
        for bowler in numbered(bowl.get("bowlersData")):
            if not isinstance(bowler, dict):
                continue
            player_id = num(bowler.get("bowlerId") or bowler.get("bowlId") or bowler.get("id"))
            name = blank(bowler.get("bowlName") or bowler.get("name"))
            if not player_id or not name:
                continue
            order += 1
            players.append((player_id, name))
            bowling.append(
                {
                    "innings": number,
                    "player_id": player_id,
                    "name": name,
                    "order": order,
                    "overs": blank(bowler.get("overs")),
                    "maidens": whole(bowler.get("maidens")) or 0,
                    "runs": whole(bowler.get("runs")) or 0,
                    "wickets": whole(bowler.get("wickets")) or 0,
                    "economy": decimal(bowler.get("economy")),
                    "no_balls": whole(bowler.get("no_balls") or bowler.get("noBalls")) or 0,
                    "wides": whole(bowler.get("wides")) or 0,
                    "dots": whole(bowler.get("dots")),
                    "balls": whole(bowler.get("balls")),
                }
            )
        for wicket in numbered(card.get("wicketsData")):
            if not isinstance(wicket, dict):
                continue
            wicket_no = num(wicket.get("wktNbr") or wicket.get("wicketNbr"))
            name = blank(wicket.get("batName") or wicket.get("name"))
            if not wicket_no or not name:
                continue
            player_id = num(wicket.get("batId"))
            if player_id:
                players.append((player_id, name))
            wickets.append(
                {
                    "innings": number,
                    "number": wicket_no,
                    "player_id": player_id,
                    "player_name": name,
                    "runs": whole(wicket.get("wktRuns")) or 0,
                    "overs": blank(wicket.get("wktOver")),
                }
            )
        for spot, partnership in enumerate(numbered(card.get("partnershipsData")), start=1):
            if not isinstance(partnership, dict):
                continue
            left = blank(partnership.get("bat1Name"))
            right = blank(partnership.get("bat2Name"))
            if not left or not right:
                continue
            left_id = num(partnership.get("bat1Id"))
            right_id = num(partnership.get("bat2Id"))
            if left_id:
                players.append((left_id, left))
            if right_id:
                players.append((right_id, right))
            partnerships.append(
                {
                    "innings": number,
                    "number": spot,
                    "batter1_id": left_id,
                    "batter1_name": left,
                    "batter1_runs": whole(partnership.get("bat1Runs")) or 0,
                    "batter1_balls": whole(partnership.get("bat1balls") or partnership.get("bat1Balls")),
                    "batter2_id": right_id,
                    "batter2_name": right,
                    "batter2_runs": whole(partnership.get("bat2Runs")) or 0,
                    "batter2_balls": whole(partnership.get("bat2balls") or partnership.get("bat2Balls")),
                    "runs": whole(partnership.get("totalRuns")) or 0,
                    "balls": whole(partnership.get("totalBalls")),
                }
            )
        for spot, powerplay in enumerate(numbered(card.get("ppData")), start=1):
            if not isinstance(powerplay, dict):
                continue
            kind = blank(powerplay.get("ppType"))
            if not kind:
                continue
            powerplays.append(
                {
                    "innings": number,
                    "number": spot,
                    "kind": kind,
                    "overs_from": blank(powerplay.get("ppOversFrom")),
                    "overs_to": blank(powerplay.get("ppOversTo")),
                    "runs": whole(powerplay.get("runsScored")) or 0,
                }
            )
    awards = []
    for award_name, key in (("player of the match", "playersOfTheMatch"), ("player of the series", "playersOfTheSeries")):
        for person in header.get(key) or []:
            if not isinstance(person, dict):
                continue
            player_id = num(person.get("id"))
            name = blank(person.get("fullName") or person.get("name"))
            if not player_id or not name:
                continue
            players.append((player_id, name))
            awards.append(
                {
                    "award": award_name,
                    "player_id": player_id,
                    "player_name": name,
                    "team_name": blank(person.get("teamName")),
                }
            )
    status_text = blank(header.get("status") or raw.get("status"))
    return {
        "header": header,
        "status_text": status_text,
        "result_text": status_text,
        "venue": {
            "cricbuzz_id": num(venue_raw.get("id")),
            "name": blank(venue_raw.get("name") or venue_raw.get("ground")),
            "city": blank(venue_raw.get("city")),
            "country": blank(venue_raw.get("country")),
            "timezone": blank(venue_raw.get("timezone")),
            "latitude": decimal(venue_raw.get("latitude")),
            "longitude": decimal(venue_raw.get("longitude")),
        },
        "toss_winner_id": num(toss.get("tossWinnerId")),
        "toss_winner": blank(toss.get("tossWinnerName")),
        "toss_decision": decision_code,
        "home": team_of(header.get("team1")),
        "away": team_of(header.get("team2")),
        "innings": innings,
        "batting": batting,
        "bowling": bowling,
        "wickets": wickets,
        "extras": extras,
        "partnerships": partnerships,
        "powerplays": powerplays,
        "awards": awards,
        "players": players,
    }


def scorecard_problem(parsed: dict, status: str) -> str | None:
    if status == "finished" and not parsed["innings"]:
        return "finished match has no innings"
    extras_by_innings = {row["innings"]: row for row in parsed["extras"]}
    wickets_by_innings: dict[int, int] = {}
    for row in parsed["wickets"]:
        wickets_by_innings[row["innings"]] = wickets_by_innings.get(row["innings"], 0) + 1
    batters: dict[int, list[dict]] = {}
    for row in parsed["batting"]:
        batters.setdefault(row["innings"], []).append(row)
    for innings in parsed["innings"]:
        number = innings["number"]
        rows = batters.get(number, [])
        ids = [row["player_id"] for row in rows]
        if len(ids) != len(set(ids)):
            return f"innings {number} repeats a batter"
        batter_runs = sum(row["runs"] for row in rows)
        extra = extras_by_innings.get(number)
        extra_total = 0 if extra is None else extra["total"]
        if extra is None and batter_runs == innings["runs"]:
            extra_total = 0
        if batter_runs + extra_total != innings["runs"]:
            return (
                f"innings {number} runs {innings['runs']} != "
                f"batting {batter_runs} + extras {extra_total}"
            )
        if innings["runs"] > 0 and not innings["overs"]:
            return f"innings {number} has runs but no overs"
        for bowler in parsed["bowling"]:
            if bowler["innings"] == number and not bowler["overs"]:
                return f"innings {number} bowler {bowler['name']} has no overs"
        fallen = wickets_by_innings.get(number, 0)
        if fallen > innings["wickets"]:
            return f"innings {number} lists {fallen} wickets, score says {innings['wickets']}"
        for row in rows:
            if row["is_out"] and not row["dismissal"]:
                return f"innings {number} {row['name']} is out without a dismissal"
    return None


def apply_venue_facts(venue: dict, facts: dict[str, str]) -> None:
    capacity = facts.get("Capacity")
    if capacity and capacity.isdigit():
        venue["capacity"] = int(capacity)
    if facts.get("Ends"):
        venue["ends"] = facts["Ends"]
    if facts.get("Hosts To"):
        venue["hosts"] = facts["Hosts To"]
    stadium = facts.get("Stadium")
    if stadium and not venue.get("name"):
        venue["name"] = stadium
    city = facts.get("City")
    if city and not venue.get("city"):
        if "," in city and not venue.get("country"):
            left, right = [part.strip() for part in city.rsplit(",", 1)]
            venue["city"] = left or city
            venue["country"] = right or None
        else:
            venue["city"] = city


def column_problem(facts: dict[str, str], match: dict, venue: dict) -> str | None:
    if facts.get("Toss") and not match.get("toss_text"):
        return "toss was on the info page and was not stored"
    if facts.get("Umpires") and not match.get("umpire1"):
        return "umpires were on the info page and were not stored"
    if facts.get("3rd Umpire") and not match.get("umpire3"):
        return "third umpire was on the info page and was not stored"
    if facts.get("Referee") and not match.get("referee"):
        return "referee was on the info page and was not stored"
    if facts.get("Time") and not match.get("info_time_text"):
        return "time was on the info page and was not stored"
    if facts.get("Capacity") and facts["Capacity"].isdigit() and not venue.get("capacity"):
        return "capacity was on the info page and was not stored"
    if facts.get("Ends") and not venue.get("ends"):
        return "ends were on the info page and were not stored"
    if facts.get("Hosts To") and not venue.get("hosts"):
        return "hosts were on the info page and were not stored"
    if not facts:
        return None
    for label, value in facts.items():
        if not label or not value:
            return "match info contained a blank fact"
    return None


def attach_squads(people: list[dict], home: dict, away: dict) -> str | None:
    teams = {home["name"].casefold(): home["id"], away["name"].casefold(): away["id"]}
    for person in people:
        team_id = teams.get(person["team_name"].casefold())
        if team_id is None:
            return f"squad team {person['team_name']} is not in this match"
        person["team_id"] = team_id
        if not person["name"] or not person["player_id"]:
            return "squad player is missing a name or id"
    return None


def build_match(match_id: int) -> dict:
    status, body = fetch(f"https://www.cricbuzz.com/api/mcenter/scorecard/{match_id}")
    pause()
    if status != 200 or not body.strip():
        raise FetchError(f"scorecard HTTP {status}")
    try:
        raw = json.loads(body)
    except json.JSONDecodeError as error:
        raise FetchError(f"scorecard was not json: {error}") from error
    parsed = parse_scorecard(match_id, raw)
    header = parsed["header"]
    home = parsed["home"]
    away = parsed["away"]
    if not home or not away:
        raise QualityError("scorecard has no two teams")
    series_id = num(header.get("seriesId"))
    series_name = blank(header.get("seriesName"))
    if not series_id or not series_name:
        raise QualityError("scorecard has no series")
    state = text(header.get("state"))
    status_name = concluded_status(state, text(header.get("status")))
    complete = raw.get("isMatchComplete") is True or state.lower() == "complete"
    if status_name is None and complete:
        status_name = "finished"
    if status_name != "finished":
        raise SkipMatch(status_name or "not finished")
    problem = scorecard_problem(parsed, status_name)
    if problem:
        raise QualityError(problem)
    info_status, info_html = fetch(f"https://www.cricbuzz.com/cricket-match-facts/{match_id}")
    pause()
    facts = parse_facts(info_html) if info_status == 200 else {}
    squads = parse_squads(info_html) if info_status == 200 else []
    if info_status == 200 and not facts.get("Series") and not facts.get("Venue") and not facts.get("Toss"):
        raise QualityError("match info page did not yield its detail rows")
    venue = parsed["venue"]
    apply_venue_facts(venue, facts)
    umpires = split_umpires(facts.get("Umpires", ""))
    match = {
        "match_id": match_id,
        "series_id": series_id,
        "series_name": series_name,
        "category": category_name(text(header.get("matchType")), series_name),
        "format_code": format_code(text(header.get("matchFormat"))),
        "status": status_name,
        "match_desc": blank(header.get("matchDescription")),
        "start_time": epoch(header.get("matchStartTimestamp")),
        "end_time": epoch(header.get("matchCompleteTimestamp")),
        "day_night": header.get("dayNight") if isinstance(header.get("dayNight"), bool) else None,
        "toss_text": facts.get("Toss"),
        "toss_winner_id": parsed["toss_winner_id"],
        "toss_decision": parsed["toss_decision"],
        "result_text": parsed["result_text"],
        "status_text": parsed["status_text"],
        "umpire1": umpires[0] if umpires else None,
        "umpire2": umpires[1] if len(umpires) > 1 else None,
        "umpire3": facts.get("3rd Umpire"),
        "referee": facts.get("Referee"),
        "info_time_text": facts.get("Time"),
        "home": home,
        "away": away,
    }
    if not match["toss_text"] and parsed["toss_winner"] and parsed["toss_decision"]:
        action = "bat" if parsed["toss_decision"] == "bat" else "bowl"
        match["toss_text"] = f"{parsed['toss_winner']} won the toss and chose to {action}"
    problem = column_problem(facts, match, venue)
    if problem:
        raise QualityError(problem)
    if facts.get("Venue") and not venue.get("name"):
        raise QualityError("venue was on the info page and was not stored")
    if status_name == "finished" and not venue.get("name"):
        raise QualityError("finished match has no venue")
    problem = attach_squads(squads, home, away)
    if problem:
        raise QualityError(problem)
    if info_status == 200 and "Players" in info_html and not any(person["group"] == "playing" for person in squads):
        raise QualityError("playing squads were on the info page and were not stored")
    commentary = []
    dropped = 0
    for innings in parsed["innings"]:
        code, payload = fetch(f"https://www.cricbuzz.com/api/mcenter/{match_id}/full-commentary/{innings['number']}")
        pause()
        if code == 404 or not payload.strip():
            continue
        if code != 200:
            raise FetchError(f"commentary {innings['number']} HTTP {code}")
        try:
            lines, skipped = delivery_lines(json.loads(payload), innings["number"])
        except json.JSONDecodeError as error:
            raise FetchError(f"commentary {innings['number']} was not json: {error}") from error
        commentary.extend(lines)
        dropped += skipped
    for row in commentary:
        if row["ball"] <= 0 or not row["text"]:
            raise QualityError("commentary filter kept a non-delivery")
        if MARKER.search(row["text"]):
            raise QualityError("commentary text still contains a formatting token")
        if row["batter_id"] and row["batter_name"]:
            parsed["players"].append((row["batter_id"], row["batter_name"]))
        if row["bowler_id"] and row["bowler_name"]:
            parsed["players"].append((row["bowler_id"], row["bowler_name"]))
    return {
        "match": match,
        "venue": venue,
        "facts": facts,
        "squads": squads,
        "parsed": parsed,
        "commentary": commentary,
        "commentary_dropped": dropped,
        "source_path": f"/cricket-series/{series_id}/matches",
        "archive_year": whole(header.get("year")),
    }


SCHEMA = [
    "CREATE SCHEMA IF NOT EXISTS archive",
    """
    CREATE TABLE IF NOT EXISTS archive.series (
      series_id BIGINT PRIMARY KEY,
      name TEXT NOT NULL,
      category TEXT NOT NULL,
      start_time TIMESTAMPTZ,
      end_time TIMESTAMPTZ,
      source_path TEXT,
      CHECK (name <> ''),
      CHECK (category IN ('international', 'league', 'domestic', 'women', 'other'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS archive.series_years (
      series_id BIGINT NOT NULL REFERENCES archive.series (series_id),
      archive_year INT NOT NULL,
      PRIMARY KEY (series_id, archive_year)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS archive.teams (
      team_id BIGINT PRIMARY KEY,
      name TEXT NOT NULL,
      short_name TEXT,
      CHECK (name <> '')
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS archive.players (
      player_id BIGINT PRIMARY KEY,
      name TEXT NOT NULL,
      CHECK (name <> '')
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS archive.venues (
      venue_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
      cricbuzz_venue_id BIGINT UNIQUE,
      name TEXT NOT NULL,
      city TEXT,
      country TEXT,
      timezone TEXT,
      latitude DOUBLE PRECISION,
      longitude DOUBLE PRECISION,
      capacity INT,
      ends TEXT,
      hosts TEXT,
      CHECK (name <> ''),
      CHECK (capacity IS NULL OR capacity > 0)
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS venues_name_city_idx
      ON archive.venues (name, city) NULLS NOT DISTINCT
    """,
    """
    CREATE TABLE IF NOT EXISTS archive.matches (
      match_id BIGINT PRIMARY KEY,
      series_id BIGINT NOT NULL REFERENCES archive.series (series_id),
      venue_id BIGINT REFERENCES archive.venues (venue_id),
      home_team_id BIGINT NOT NULL REFERENCES archive.teams (team_id),
      away_team_id BIGINT NOT NULL REFERENCES archive.teams (team_id),
      category TEXT NOT NULL,
      format_code TEXT NOT NULL,
      status TEXT NOT NULL,
      match_desc TEXT,
      start_time TIMESTAMPTZ,
      end_time TIMESTAMPTZ,
      day_night BOOLEAN,
      toss_text TEXT,
      toss_winner_team_id BIGINT,
      toss_decision TEXT,
      result_text TEXT,
      status_text TEXT,
      umpire1 TEXT,
      umpire2 TEXT,
      umpire3 TEXT,
      referee TEXT,
      info_time_text TEXT,
      commentary_loaded BOOLEAN NOT NULL DEFAULT FALSE,
      collected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      CHECK (status IN ('finished', 'abandoned', 'cancelled')),
      CHECK (home_team_id <> away_team_id),
      CHECK (toss_decision IS NULL OR toss_decision IN ('bat', 'bowl'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS archive.match_facts (
      match_id BIGINT NOT NULL REFERENCES archive.matches (match_id) ON DELETE CASCADE,
      label TEXT NOT NULL,
      value TEXT NOT NULL,
      PRIMARY KEY (match_id, label),
      CHECK (label <> '' AND value <> '')
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS archive.awards (
      match_id BIGINT NOT NULL REFERENCES archive.matches (match_id) ON DELETE CASCADE,
      award TEXT NOT NULL,
      player_id BIGINT NOT NULL REFERENCES archive.players (player_id),
      player_name TEXT NOT NULL,
      team_name TEXT,
      PRIMARY KEY (match_id, award, player_id),
      CHECK (player_name <> '')
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS archive.squads (
      match_id BIGINT NOT NULL REFERENCES archive.matches (match_id) ON DELETE CASCADE,
      team_id BIGINT NOT NULL REFERENCES archive.teams (team_id),
      player_id BIGINT NOT NULL REFERENCES archive.players (player_id),
      squad_group TEXT NOT NULL,
      sort_order INT NOT NULL,
      is_captain BOOLEAN NOT NULL DEFAULT FALSE,
      is_keeper BOOLEAN NOT NULL DEFAULT FALSE,
      PRIMARY KEY (match_id, team_id, player_id),
      CHECK (squad_group IN ('playing', 'bench', 'support'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS archive.innings (
      match_id BIGINT NOT NULL REFERENCES archive.matches (match_id) ON DELETE CASCADE,
      innings_number INT NOT NULL,
      batting_team_id BIGINT,
      bowling_team_id BIGINT,
      runs INT NOT NULL,
      wickets INT NOT NULL,
      overs TEXT,
      run_rate DOUBLE PRECISION,
      time_score TEXT,
      PRIMARY KEY (match_id, innings_number),
      CHECK (wickets >= 0 AND wickets <= 10),
      CHECK (runs >= 0)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS archive.extras (
      match_id BIGINT NOT NULL,
      innings_number INT NOT NULL,
      total INT NOT NULL,
      wides INT NOT NULL,
      no_balls INT NOT NULL,
      byes INT NOT NULL,
      leg_byes INT NOT NULL,
      penalty INT NOT NULL,
      PRIMARY KEY (match_id, innings_number),
      FOREIGN KEY (match_id, innings_number) REFERENCES archive.innings (match_id, innings_number) ON DELETE CASCADE,
      CHECK (total >= 0)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS archive.batting (
      match_id BIGINT NOT NULL,
      innings_number INT NOT NULL,
      player_id BIGINT NOT NULL REFERENCES archive.players (player_id),
      bat_order INT NOT NULL,
      runs INT NOT NULL,
      balls INT NOT NULL,
      dots INT,
      fours INT NOT NULL,
      sixes INT NOT NULL,
      minutes INT,
      strike_rate DOUBLE PRECISION,
      is_out BOOLEAN NOT NULL,
      dismissal TEXT,
      is_captain BOOLEAN NOT NULL,
      is_keeper BOOLEAN NOT NULL,
      bowler_id BIGINT,
      fielder1_id BIGINT,
      fielder2_id BIGINT,
      fielder3_id BIGINT,
      wicket_code TEXT,
      PRIMARY KEY (match_id, innings_number, player_id),
      FOREIGN KEY (match_id, innings_number) REFERENCES archive.innings (match_id, innings_number) ON DELETE CASCADE,
      CHECK (runs >= 0 AND balls >= 0)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS archive.bowling (
      match_id BIGINT NOT NULL,
      innings_number INT NOT NULL,
      player_id BIGINT NOT NULL REFERENCES archive.players (player_id),
      bowl_order INT NOT NULL,
      overs TEXT NOT NULL,
      maidens INT NOT NULL,
      runs INT NOT NULL,
      wickets INT NOT NULL,
      economy DOUBLE PRECISION,
      no_balls INT NOT NULL,
      wides INT NOT NULL,
      dots INT,
      balls INT,
      PRIMARY KEY (match_id, innings_number, player_id),
      FOREIGN KEY (match_id, innings_number) REFERENCES archive.innings (match_id, innings_number) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS archive.wickets (
      match_id BIGINT NOT NULL,
      innings_number INT NOT NULL,
      wicket_number INT NOT NULL,
      player_id BIGINT,
      player_name TEXT NOT NULL,
      runs INT NOT NULL,
      overs TEXT,
      PRIMARY KEY (match_id, innings_number, wicket_number),
      FOREIGN KEY (match_id, innings_number) REFERENCES archive.innings (match_id, innings_number) ON DELETE CASCADE,
      CHECK (player_name <> '')
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS archive.partnerships (
      match_id BIGINT NOT NULL,
      innings_number INT NOT NULL,
      partnership_number INT NOT NULL,
      batter1_id BIGINT,
      batter1_name TEXT NOT NULL,
      batter1_runs INT NOT NULL,
      batter1_balls INT,
      batter2_id BIGINT,
      batter2_name TEXT NOT NULL,
      batter2_runs INT NOT NULL,
      batter2_balls INT,
      runs INT NOT NULL,
      balls INT,
      PRIMARY KEY (match_id, innings_number, partnership_number),
      FOREIGN KEY (match_id, innings_number) REFERENCES archive.innings (match_id, innings_number) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS archive.powerplays (
      match_id BIGINT NOT NULL,
      innings_number INT NOT NULL,
      powerplay_number INT NOT NULL,
      kind TEXT NOT NULL,
      overs_from TEXT,
      overs_to TEXT,
      runs INT NOT NULL,
      PRIMARY KEY (match_id, innings_number, powerplay_number),
      FOREIGN KEY (match_id, innings_number) REFERENCES archive.innings (match_id, innings_number) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS archive.commentary (
      match_id BIGINT NOT NULL REFERENCES archive.matches (match_id) ON DELETE CASCADE,
      innings_number INT NOT NULL,
      sequence_number INT NOT NULL,
      over_number DOUBLE PRECISION,
      ball_in_over INT,
      ball_number INT NOT NULL,
      event TEXT,
      batter_id BIGINT,
      batter_name TEXT,
      bowler_id BIGINT,
      bowler_name TEXT,
      legal_runs INT,
      total_runs INT,
      team_score INT,
      commentary_text TEXT NOT NULL,
      PRIMARY KEY (match_id, innings_number, sequence_number),
      CHECK (ball_number > 0),
      CHECK (commentary_text <> ''),
      CHECK (batter_id IS NOT NULL OR bowler_id IS NOT NULL)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS archive.rejections (
      match_id BIGINT PRIMARY KEY,
      series_id BIGINT,
      reason TEXT NOT NULL,
      rejected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      CHECK (reason <> '')
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS archive.series_done (
      series_id BIGINT PRIMARY KEY,
      finished_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    "CREATE INDEX IF NOT EXISTS matches_series_idx ON archive.matches (series_id, start_time)",
    "CREATE INDEX IF NOT EXISTS commentary_match_idx ON archive.commentary (match_id, innings_number, ball_number)",
]


def database_url() -> str:
    load_env()
    url = os.environ.get("ARCHIVE_DATABASE_URL", "").strip()
    if not url:
        raise SystemExit("Set ARCHIVE_DATABASE_URL to the Docker Postgres database")
    return url


def load_env() -> None:
    path = ROOT / ".env"
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def connect(retries: int = 30):
    url = database_url()
    last = None
    for _ in range(retries):
        try:
            conn = psycopg.connect(url, autocommit=True)
            for statement in SCHEMA:
                conn.execute(statement)
            return conn
        except psycopg.Error as error:
            last = error
            time.sleep(1)
    raise SystemExit(f"Postgres is not ready: {last}")


def upsert_team(conn, team: dict) -> None:
    conn.execute(
        """
        INSERT INTO archive.teams (team_id, name, short_name)
        VALUES (%s, %s, %s)
        ON CONFLICT (team_id) DO UPDATE
          SET name = EXCLUDED.name,
              short_name = COALESCE(EXCLUDED.short_name, archive.teams.short_name)
        """,
        (team["id"], team["name"], team.get("short")),
    )


def upsert_players(conn, players: list[tuple[int, str]]) -> None:
    rows = []
    seen = set()
    for player_id, name in players:
        if not player_id or not name or player_id in seen:
            continue
        seen.add(player_id)
        rows.append((player_id, name))
    if not rows:
        return
    conn.cursor().executemany(
        """
        INSERT INTO archive.players (player_id, name)
        VALUES (%s, %s)
        ON CONFLICT (player_id) DO UPDATE
          SET name = CASE
            WHEN length(EXCLUDED.name) > length(archive.players.name) THEN EXCLUDED.name
            ELSE archive.players.name
          END
        """,
        rows,
    )


def upsert_series(conn, series: dict) -> None:
    conn.execute(
        """
        INSERT INTO archive.series (series_id, name, category, start_time, end_time, source_path)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (series_id) DO UPDATE
          SET name = EXCLUDED.name,
              category = CASE
                WHEN EXCLUDED.category <> 'other' THEN EXCLUDED.category
                ELSE archive.series.category
              END,
              start_time = COALESCE(EXCLUDED.start_time, archive.series.start_time),
              end_time = COALESCE(EXCLUDED.end_time, archive.series.end_time),
              source_path = COALESCE(EXCLUDED.source_path, archive.series.source_path)
        """,
        (
            series["series_id"],
            series["name"],
            series["category"],
            series.get("start_time"),
            series.get("end_time"),
            series.get("source_path"),
        ),
    )
    if series.get("archive_year"):
        conn.execute(
            """
            INSERT INTO archive.series_years (series_id, archive_year)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
            """,
            (series["series_id"], series["archive_year"]),
        )


def upsert_venue(conn, venue: dict) -> int | None:
    if not venue.get("name"):
        return None
    if venue.get("cricbuzz_id"):
        found = conn.execute(
            """
            INSERT INTO archive.venues (
              cricbuzz_venue_id, name, city, country, timezone, latitude, longitude, capacity, ends, hosts
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (cricbuzz_venue_id) DO UPDATE
              SET name = EXCLUDED.name,
                  city = COALESCE(EXCLUDED.city, archive.venues.city),
                  country = COALESCE(EXCLUDED.country, archive.venues.country),
                  timezone = COALESCE(EXCLUDED.timezone, archive.venues.timezone),
                  latitude = COALESCE(EXCLUDED.latitude, archive.venues.latitude),
                  longitude = COALESCE(EXCLUDED.longitude, archive.venues.longitude),
                  capacity = COALESCE(EXCLUDED.capacity, archive.venues.capacity),
                  ends = COALESCE(EXCLUDED.ends, archive.venues.ends),
                  hosts = COALESCE(EXCLUDED.hosts, archive.venues.hosts)
            RETURNING venue_id
            """,
            (
                venue["cricbuzz_id"],
                venue["name"],
                venue.get("city"),
                venue.get("country"),
                venue.get("timezone"),
                venue.get("latitude"),
                venue.get("longitude"),
                venue.get("capacity"),
                venue.get("ends"),
                venue.get("hosts"),
            ),
        ).fetchone()
        return int(found[0])
    found = conn.execute(
        """
        INSERT INTO archive.venues (name, city, country, timezone, capacity, ends, hosts)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING venue_id
        """,
        (
            venue["name"],
            venue.get("city"),
            venue.get("country"),
            venue.get("timezone"),
            venue.get("capacity"),
            venue.get("ends"),
            venue.get("hosts"),
        ),
    ).fetchone()
    if found:
        return int(found[0])
    found = conn.execute(
        """
        SELECT venue_id FROM archive.venues
        WHERE name = %s AND city IS NOT DISTINCT FROM %s
        """,
        (venue["name"], venue.get("city")),
    ).fetchone()
    if not found:
        raise QualityError(f"venue {venue['name']} could not be stored")
    conn.execute(
        """
        UPDATE archive.venues
        SET country = COALESCE(%s, country),
            capacity = COALESCE(%s, capacity),
            ends = COALESCE(%s, ends),
            hosts = COALESCE(%s, hosts)
        WHERE venue_id = %s
        """,
        (venue.get("country"), venue.get("capacity"), venue.get("ends"), venue.get("hosts"), found[0]),
    )
    return int(found[0])


def known_ids(conn) -> set[int]:
    stored = {row[0] for row in conn.execute("SELECT match_id FROM archive.matches")}
    rejected = {row[0] for row in conn.execute("SELECT match_id FROM archive.rejections")}
    return stored | rejected


def reject(conn, match_id: int, series_id: int | None, reason: str) -> None:
    conn.execute(
        """
        INSERT INTO archive.rejections (match_id, series_id, reason)
        VALUES (%s, %s, %s)
        ON CONFLICT (match_id) DO UPDATE
          SET reason = EXCLUDED.reason, rejected_at = now()
        """,
        (match_id, series_id, reason[:500]),
    )


def audit(conn, bundle: dict) -> str | None:
    match_id = bundle["match"]["match_id"]
    uneven = conn.execute(
        """
        SELECT i.innings_number
        FROM archive.innings i
        LEFT JOIN archive.batting b USING (match_id, innings_number)
        LEFT JOIN archive.extras e USING (match_id, innings_number)
        WHERE i.match_id = %s
        GROUP BY i.innings_number, i.runs, e.total
        HAVING i.runs <> COALESCE(SUM(b.runs), 0) + COALESCE(e.total, 0)
        """,
        (match_id,),
    ).fetchone()
    if uneven:
        return f"stored innings {uneven[0]} does not add up"
    bad_comment = conn.execute(
        """
        SELECT sequence_number FROM archive.commentary
        WHERE match_id = %s
          AND (ball_number <= 0 OR btrim(commentary_text) = '')
        LIMIT 1
        """,
        (match_id,),
    ).fetchone()
    if bad_comment:
        return "stored commentary includes a note that is not a delivery"
    blank_fact = conn.execute(
        "SELECT label FROM archive.match_facts WHERE match_id = %s AND btrim(value) = ''",
        (match_id,),
    ).fetchone()
    if blank_fact:
        return f"match fact {blank_fact[0]} is blank"
    return None


def save_bundle(conn, bundle: dict) -> None:
    match = bundle["match"]
    parsed = bundle["parsed"]
    with conn.transaction():
        upsert_series(
            conn,
            {
                "series_id": match["series_id"],
                "name": match["series_name"],
                "category": match["category"],
                "start_time": None,
                "end_time": None,
                "source_path": bundle["source_path"],
                "archive_year": bundle.get("archive_year"),
            },
        )
        upsert_team(conn, match["home"])
        upsert_team(conn, match["away"])
        players = list(parsed["players"])
        for person in bundle["squads"]:
            players.append((person["player_id"], person["name"]))
        upsert_players(conn, players)
        venue_id = upsert_venue(conn, bundle["venue"])
        conn.execute("DELETE FROM archive.matches WHERE match_id = %s", (match["match_id"],))
        conn.execute("DELETE FROM archive.rejections WHERE match_id = %s", (match["match_id"],))
        conn.execute(
            """
            INSERT INTO archive.matches (
              match_id, series_id, venue_id, home_team_id, away_team_id, category, format_code,
              status, match_desc, start_time, end_time, day_night, toss_text, toss_winner_team_id,
              toss_decision, result_text, status_text, umpire1, umpire2, umpire3, referee,
              info_time_text, commentary_loaded
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                match["match_id"],
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
            ),
        )
        if bundle["facts"]:
            conn.cursor().executemany(
                "INSERT INTO archive.match_facts (match_id, label, value) VALUES (%s, %s, %s)",
                [(match["match_id"], label, value) for label, value in bundle["facts"].items()],
            )
        if bundle["squads"]:
            conn.cursor().executemany(
                """
                INSERT INTO archive.squads (
                  match_id, team_id, player_id, squad_group, sort_order, is_captain, is_keeper
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        match["match_id"],
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
            conn.cursor().executemany(
                """
                INSERT INTO archive.awards (match_id, award, player_id, player_name, team_name)
                VALUES (%s, %s, %s, %s, %s)
                """,
                [
                    (match["match_id"], row["award"], row["player_id"], row["player_name"], row["team_name"])
                    for row in parsed["awards"]
                ],
            )
        if parsed["innings"]:
            conn.cursor().executemany(
                """
                INSERT INTO archive.innings (
                  match_id, innings_number, batting_team_id, bowling_team_id, runs, wickets, overs, run_rate, time_score
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                [
                    (
                        match["match_id"],
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
            conn.cursor().executemany(
                """
                INSERT INTO archive.extras (
                  match_id, innings_number, total, wides, no_balls, byes, leg_byes, penalty
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                [
                    (
                        match["match_id"],
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
            conn.cursor().executemany(
                """
                INSERT INTO archive.batting (
                  match_id, innings_number, player_id, bat_order, runs, balls, dots, fours, sixes,
                  minutes, strike_rate, is_out, dismissal, is_captain, is_keeper, bowler_id,
                  fielder1_id, fielder2_id, fielder3_id, wicket_code
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                [
                    (
                        match["match_id"],
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
            conn.cursor().executemany(
                """
                INSERT INTO archive.bowling (
                  match_id, innings_number, player_id, bowl_order, overs, maidens, runs, wickets,
                  economy, no_balls, wides, dots, balls
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                [
                    (
                        match["match_id"],
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
            conn.cursor().executemany(
                """
                INSERT INTO archive.wickets (
                  match_id, innings_number, wicket_number, player_id, player_name, runs, overs
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                """,
                [
                    (
                        match["match_id"],
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
            conn.cursor().executemany(
                """
                INSERT INTO archive.partnerships (
                  match_id, innings_number, partnership_number, batter1_id, batter1_name, batter1_runs,
                  batter1_balls, batter2_id, batter2_name, batter2_runs, batter2_balls, runs, balls
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                [
                    (
                        match["match_id"],
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
            conn.cursor().executemany(
                """
                INSERT INTO archive.powerplays (
                  match_id, innings_number, powerplay_number, kind, overs_from, overs_to, runs
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                """,
                [
                    (
                        match["match_id"],
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
            conn.cursor().executemany(
                """
                INSERT INTO archive.commentary (
                  match_id, innings_number, sequence_number, over_number, ball_in_over, ball_number,
                  event, batter_id, batter_name, bowler_id, bowler_name, legal_runs, total_runs,
                  team_score, commentary_text
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                [
                    (
                        match["match_id"],
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
        problem = audit(conn, bundle)
        if problem:
            raise QualityError(problem)


def archive_years(start: int, end: int) -> list[int]:
    status, html = fetch(ARCHIVE_INDEX)
    if status != 200:
        raise SystemExit(f"Archive index HTTP {status}")
    years = sorted({int(year) for year in re.findall(r"scorecard-archives/(\d{4})", html)})
    chosen = [year for year in years if start <= year <= end]
    if not chosen:
        raise SystemExit(f"No archive years from {start} to {end}")
    return chosen


def store_match(conn, match_id: int, known: set[int], replace: bool) -> str:
    if match_id in known and not replace:
        return "skip"
    try:
        bundle = build_match(match_id)
    except QualityError as error:
        reject(conn, match_id, None, str(error))
        known.add(match_id)
        print(f"rejected {match_id}: {error}", file=sys.stderr, flush=True)
        return "rejected"
    except SkipMatch as skipped:
        print(f"skipped {match_id} {skipped}", flush=True)
        known.add(match_id)
        return "skip"
    except (FetchError, RuntimeError) as error:
        print(f"retry later {match_id}: {error}", file=sys.stderr, flush=True)
        return "retry"
    try:
        save_bundle(conn, bundle)
    except (QualityError, psycopg.Error) as error:
        reject(conn, match_id, bundle["match"]["series_id"], str(error))
        known.add(match_id)
        print(f"rejected {match_id}: {error}", file=sys.stderr, flush=True)
        return "rejected"
    known.add(match_id)
    match = bundle["match"]
    print(
        f"stored {match_id} {match['home']['name']} vs {match['away']['name']} "
        f"commentary {len(bundle['commentary'])} dropped_notes {bundle['commentary_dropped']}",
        flush=True,
    )
    return "stored"


def series_matches(series: dict) -> list[dict]:
    status, html = fetch("https://www.cricbuzz.com" + series["source_path"])
    pause()
    if status != 200:
        raise FetchError(f"series {series['series_id']} HTTP {status}")
    fixtures = []
    for fixture in finished_fixtures(cards_from(html)):
        if fixture["series_id"] != series["series_id"] or fixture["status"] != "finished":
            continue
        fixtures.append(fixture)
    return fixtures


def done_series_ids(conn) -> set[int]:
    return {row[0] for row in conn.execute("SELECT series_id FROM archive.series_done")}


def mark_series_done(conn, series_id: int) -> None:
    conn.execute(
        "INSERT INTO archive.series_done (series_id) VALUES (%s) ON CONFLICT DO NOTHING",
        (series_id,),
    )


def collect(conn, years: list[int], only_match: int | None, replace: bool, shard: tuple[int, int]) -> None:
    shard_index, shard_count = shard
    known = set() if replace and only_match else known_ids(conn)
    done = set() if only_match else done_series_ids(conn)
    if only_match:
        store_match(conn, only_match, known, replace=True)
        return
    stored = rejected = retried = 0
    for year in reversed(years):
        try:
            status, html = fetch(f"{ARCHIVE_INDEX}/{year}")
            pause()
        except RuntimeError as error:
            print(f"year {year} {error}", file=sys.stderr, flush=True)
            continue
        if status != 200:
            print(f"year {year} HTTP {status}", file=sys.stderr, flush=True)
            continue
        series_rows = parse_year_series(html, year)
        mine = [series for series in series_rows if series["series_id"] % shard_count == shard_index]
        for series in mine:
            upsert_series(conn, series)
        print(f"year {year} series {len(mine)}/{len(series_rows)} shard {shard_index + 1}/{shard_count}", flush=True)
        for series in mine:
            if series["series_id"] in done:
                print(f"series {series['series_id']} {series['name']} already complete", flush=True)
                continue
            try:
                fixtures = series_matches(series)
            except (FetchError, RuntimeError) as error:
                print(f"series {series['series_id']} {error}", file=sys.stderr, flush=True)
                continue
            pending = [fixture for fixture in fixtures if fixture["match_id"] not in known]
            print(
                f"series {series['series_id']} {series['name']} finished {len(fixtures)} "
                f"new {len(pending)} already stored {len(fixtures) - len(pending)}",
                flush=True,
            )
            series_retry = 0
            for fixture in pending:
                outcome = store_match(conn, fixture["match_id"], known, replace=False)
                stored += outcome == "stored"
                rejected += outcome == "rejected"
                retried += outcome == "retry"
                series_retry += outcome == "retry"
            if series_retry:
                print(f"series {series['series_id']} incomplete, will resume", flush=True)
                continue
            if not fixtures:
                print(f"series {series['series_id']} has no finished matches yet", flush=True)
                continue
            mark_series_done(conn, series["series_id"])
            done.add(series["series_id"])
        print(f"year {year} done stored {stored} rejected {rejected} retry {retried}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect the Cricbuzz scorecard archive into Docker Postgres")
    parser.add_argument("--from-year", type=int, default=1877)
    parser.add_argument("--to-year", type=int, default=datetime.now(timezone.utc).year)
    parser.add_argument("--match", type=int, default=0)
    parser.add_argument("--shard", default="1/1", help="Worker slice, 1/4 through 4/4")
    parser.add_argument("--list-series", action="store_true")
    return parser.parse_args()


def parse_shard(value: str) -> tuple[int, int]:
    try:
        part, total = value.split("/")
        worker = int(part)
        count = int(total)
    except ValueError:
        raise SystemExit("--shard must look like 1/4") from None
    if count < 1 or worker < 1 or worker > count:
        raise SystemExit("--shard must look like 1/4")
    return worker - 1, count


def main() -> None:
    args = parse_args()
    if args.list_series:
        years = archive_years(args.from_year, args.to_year)
        for year in reversed(years):
            status, html = fetch(f"{ARCHIVE_INDEX}/{year}")
            pause()
            rows = parse_year_series(html, year) if status == 200 else []
            print(f"{year}\t{len(rows)}\t{status}", flush=True)
        return
    conn = connect()
    try:
        shard = parse_shard(args.shard)
        if args.match:
            collect(conn, [], args.match, replace=True, shard=shard)
            return
        years = archive_years(args.from_year, args.to_year)
        print(f"archive {years[-1]}-{years[0]} ({len(years)} years) shard {args.shard}", flush=True)
        collect(conn, years, None, replace=False, shard=shard)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
