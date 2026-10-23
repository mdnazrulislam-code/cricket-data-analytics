"""Quality rules for the Cricbuzz archive load."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from collect_archive import (  # noqa: E402
    ball_in_over,
    column_problem,
    delivery_lines,
    parse_facts,
    parse_squads,
    player_name,
    scorecard_problem,
)

FACTS_HTML = """
<div class="font-bold">Toss</div><div>Sri Lanka won the toss and opt to Bowl</div>
<div class="font-bold">Umpires</div><div>Wayne Knights, Asif Yaqoob</div>
<div class="font-bold">3rd Umpire</div><div>Sharfuddoula Saikat</div>
<div class="font-bold">Capacity</div><div>15000</div>
<div class="font-bold">Ends</div><div>Pavilion End, Shell End</div>
<div class="font-bold">Hosts To</div><div>Pakistan, Rawalpindi</div>
<div class="font-bold">Players</div><div class="flex">ignored</div>
"""

SQUAD_HTML = """
<div class="font-bold w-10/12">Sri Lanka<!-- --> squad</div>
<div class="font-bold">Players</div>
<span class="hover:underline">Kusal Mendis<!-- --> (c)<!-- --> (wk)<!-- -->, <!-- --> </span>
<a href="/profiles/10939/kusal-mendis" title="View Profile Of Kusal Mendis">View profile</a>
<div class="font-bold">Bench</div>
<span class="hover:underline">Maheesh Theekshana<!-- -->, </span>
<a href="/profiles/22000/maheesh-theekshana" title="View Profile Of Maheesh Theekshana">View profile</a>
<div class="font-bold">Support Staff</div>
<a title="View Profile Of Gary Kirsten" href="/profiles/4601/gary-kirsten">Gary Kirsten<!-- -->, </a>
"""


def test_player_roles():
    name, captain, keeper = player_name("Kusal Mendis (c) (wk),")
    assert name == "Kusal Mendis"
    assert captain and keeper
    name, captain, keeper = player_name("Harry Brook (c)")
    assert name == "Harry Brook" and captain and not keeper


def test_facts_skip_empty_structure():
    facts = parse_facts(FACTS_HTML)
    assert facts["Toss"] == "Sri Lanka won the toss and opt to Bowl"
    assert facts["Umpires"] == "Wayne Knights, Asif Yaqoob"
    assert facts["Capacity"] == "15000"
    assert "Players" not in facts


def test_squads_keep_roles_and_groups():
    people = parse_squads(SQUAD_HTML)
    by_name = {person["name"]: person for person in people}
    assert by_name["Kusal Mendis"]["group"] == "playing"
    assert by_name["Kusal Mendis"]["captain"] and by_name["Kusal Mendis"]["keeper"]
    assert by_name["Kusal Mendis"]["player_id"] == 10939
    assert by_name["Maheesh Theekshana"]["group"] == "bench"
    assert by_name["Gary Kirsten"]["group"] == "support"
    assert by_name["Gary Kirsten"]["player_id"] == 4601


def test_commentary_drops_notes():
    payload = {
        "commentary": [
            {
                "commentaryList": [
                    {"commText": "Welcome back. The chase is about to begin.", "ballNbr": 0, "event": "NONE"},
                    {
                        "commText": "Janith Liyanage to Harry Brook, B0$ caught. B1$",
                        "ballNbr": 300,
                        "overNumber": 49.6,
                        "event": "WICKET",
                        "batsmanStriker": {"batId": 12201, "batName": "Harry Brook"},
                        "bowlerStriker": {"bowlId": 13694, "bowlName": "Janith Liyanage"},
                        "legalRuns": 0,
                        "totalRuns": 0,
                        "batTeamScore": 429,
                    },
                    {"commText": "That brings us to the end. Goodbye.", "ballNbr": 0, "event": "NONE"},
                ]
            }
        ]
    }
    rows, dropped = delivery_lines(payload, 1)
    assert dropped == 2
    assert len(rows) == 1
    assert rows[0]["text"] == "Janith Liyanage to Harry Brook, caught."
    assert rows[0]["ball"] == 300
    assert rows[0]["ball_in_over"] == 6
    assert "B0$" not in rows[0]["text"]


def test_score_must_add_up():
    parsed = {
        "innings": [{"number": 1, "runs": 429, "wickets": 1, "overs": "50"}],
        "batting": [
            {"innings": 1, "player_id": 1, "name": "A", "runs": 400, "is_out": True, "dismissal": "b X"},
            {"innings": 1, "player_id": 2, "name": "B", "runs": 16, "is_out": False, "dismissal": "not out"},
        ],
        "bowling": [{"innings": 1, "name": "X", "overs": "10"}],
        "extras": [{"innings": 1, "total": 13}],
        "wickets": [{"innings": 1}],
    }
    assert scorecard_problem(parsed, "finished") is None
    parsed["extras"][0]["total"] = 12
    assert scorecard_problem(parsed, "finished")


def test_info_columns_cannot_stay_blank():
    facts = {"Toss": "India won the toss", "Capacity": "15000", "Ends": "Pavilion End"}
    match = {"toss_text": None}
    venue = {}
    problem = column_problem(facts, match, venue)
    assert problem and "toss" in problem


def test_escaped_series_list():
    from collect_archive import parse_year_series

    html = (
        '{\\"domesticData\\":{\\"seriesMapProto\\":[{\\"date\\":\\"2026\\",\\"series\\":'
        '[{\\"id\\":10,\\"name\\":\\"Women Cup 2026\\",\\"startDt\\":\\"1791676800000\\",'
        '\\"endDt\\":\\"1803945600000\\"}]}]}}'
    )
    rows = parse_year_series(html, 2026)
    assert len(rows) == 1
    assert rows[0]["series_id"] == 10
    assert rows[0]["category"] == "women"
    assert rows[0]["start_time"] is not None


def test_ball_in_over():
    assert ball_in_over(13.2) == 2
    assert ball_in_over(None) is None


if __name__ == "__main__":
    test_player_roles()
    test_facts_skip_empty_structure()
    test_squads_keep_roles_and_groups()
    test_commentary_drops_notes()
    test_score_must_add_up()
    test_info_columns_cannot_stay_blank()
    test_escaped_series_list()
    test_ball_in_over()
    print("ok")
