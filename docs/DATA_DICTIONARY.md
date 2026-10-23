# Production data dictionary

Catalog `cricsocc`, schema `bronze`. Managed Delta tables on Databricks.

Source is Cricbuzz: archive series lists, match facts, scorecard API, and full commentary API. The daily GitHub job `scripts/databricks_sync.py` inserts only finished matches that are not already in `matches`. Existing `match_id` values are left unchanged.

Row counts below are the snapshot loaded on 23 October 2026. Later daily runs add finished matches only.

Databricks does not enforce the collector primary keys or checks. The grain and keys below are the production contract the insert path follows.

## Inventory

| Table | Grain | Rows | Logical key |
|---|---|---:|---|
| `teams` | One Cricbuzz team | 937 | `team_id` |
| `players` | One Cricbuzz player | 21,736 | `player_id` |
| `series` | One Cricbuzz series | 3,479 | `series_id` |
| `series_years` | Series listed on an archive year page | 3,753 | `series_id`, `archive_year` |
| `venues` | One ground | 786 | `venue_id` |
| `matches` | One finished match | 35,392 | `match_id` |
| `match_facts` | One non-empty fact label on a match info page | 419,461 | `match_id`, `label` |
| `awards` | One award for one player in a match | 26,739 | `match_id`, `award`, `player_id` |
| `squads` | One person in a match team list | 1,126,598 | `match_id`, `team_id`, `player_id` |
| `innings` | One innings of a match | 80,696 | `match_id`, `innings_number` |
| `extras` | Extra runs for one innings | 80,696 | `match_id`, `innings_number` |
| `batting` | One batter card in an innings | 894,199 | `match_id`, `innings_number`, `player_id` |
| `bowling` | One bowler card in an innings | 477,766 | `match_id`, `innings_number`, `player_id` |
| `wickets` | One fall-of-wicket row | 575,477 | `match_id`, `innings_number`, `wicket_number` |
| `partnerships` | One partnership row | 398,289 | `match_id`, `innings_number`, `partnership_number` |
| `powerplays` | One powerplay block | 41,408 | `match_id`, `innings_number`, `powerplay_number` |
| `commentary` | One legal delivery | 8,405,132 | `match_id`, `innings_number`, `sequence_number` |

Not in Databricks: `rejections` and `series_done`. Those were local collector state.

## Relationships

```
series ──< series_years
series ──< matches >── venues
             │
             ├──< match_facts
             ├──< awards >── players
             ├──< squads >── teams, players
             └──< innings
                    ├── extras (1:1)
                    ├──< batting >── players
                    ├──< bowling >── players
                    ├──< wickets
                    ├──< partnerships
                    ├──< powerplays
                    └──< commentary
```

Join matches to children on `match_id`. Join innings children on `match_id` and `innings_number`. Team and player ids are Cricbuzz ids, not Databricks-generated.

`venues.venue_id` is assigned at load time. Prefer `cricbuzz_venue_id` when it is present.

## Quality rules

- Only finished matches are stored. Abandoned, cancelled, live, and upcoming matches are skipped.
- Innings runs must equal batting runs plus extras total.
- Fall-of-wickets count may be less than the score wicket count on old cards. It must not be greater.
- Finished matches need a venue name.
- `start_time` is null when Cricbuzz sends a garbage epoch (year before 1800). The Date and Time fact strings are still stored in `match_facts`.
- Commentary keeps deliveries only: `ball_number > 0`, non-empty text, and a batter or bowler id. Preview, welcome, and goodbye notes are dropped. Older matches may have zero commentary rows.
- Every non-empty fact label from the match info page is stored. Blank source fields stay null. Values are not invented.

## Controlled values

| Column | Values |
|---|---|
| `series.category`, `matches.category` | `international`, `league`, `domestic`, `women`, `other` |
| `matches.format_code` | `test`, `odi`, `t20`, `t10`, `hundred`, or a cleaned Cricbuzz format string |
| `matches.status` | `finished` in this load |
| `matches.toss_decision` | `bat`, `bowl`, or null |
| `squads.squad_group` | `playing`, `bench`, `support` |
| `awards.award` | `player of the match`, `player of the series` |

Timestamps are UTC stored as `TIMESTAMP_NTZ`.

## `cricsocc.bronze.teams`

One team. Upserted on `team_id`.

| Column | Type | Null | Description |
|---|---|---|---|
| `team_id` | BIGINT | no | Cricbuzz team id. |
| `name` | STRING | no | Full team name. |
| `short_name` | STRING | yes | Short code when Cricbuzz has one. |

## `cricsocc.bronze.players`

One player. Name is replaced only when the new name is longer.

| Column | Type | Null | Description |
|---|---|---|---|
| `player_id` | BIGINT | no | Cricbuzz player id. |
| `name` | STRING | no | Display name from scorecard, squad, award, or commentary. |

## `cricsocc.bronze.series`

One competition. Category `other` does not overwrite a more specific stored category.

| Column | Type | Null | Description |
|---|---|---|---|
| `series_id` | BIGINT | no | Cricbuzz series id. |
| `name` | STRING | no | Series name. |
| `category` | STRING | no | `international`, `league`, `domestic`, `women`, or `other`. |
| `start_time` | TIMESTAMP_NTZ | yes | Series start when the archive list has it. |
| `end_time` | TIMESTAMP_NTZ | yes | Series end when the archive list has it. |
| `source_path` | STRING | yes | Cricbuzz path such as `/cricket-series/{id}/matches`. |

## `cricsocc.bronze.series_years`

Which archive year page listed the series. One series can appear in more than one year.

| Column | Type | Null | Description |
|---|---|---|---|
| `series_id` | BIGINT | no | `series.series_id`. |
| `archive_year` | INT | no | Year on `cricbuzz.com/cricket-scorecard-archives/{year}`. Years before 1800 are not written. |

## `cricsocc.bronze.venues`

One ground. New rows get the next `venue_id`. Match on `cricbuzz_venue_id` first, else name and city.

| Column | Type | Null | Description |
|---|---|---|---|
| `venue_id` | BIGINT | no | Load-time venue key used by `matches.venue_id`. |
| `cricbuzz_venue_id` | BIGINT | yes | Cricbuzz venue id when present. |
| `name` | STRING | no | Ground name. |
| `city` | STRING | yes | City. |
| `country` | STRING | yes | Country. |
| `timezone` | STRING | yes | Venue timezone from the scorecard. |
| `latitude` | DOUBLE | yes | Latitude. |
| `longitude` | DOUBLE | yes | Longitude. |
| `capacity` | INT | yes | Seated capacity when the fact is digits only. Text capacities stay in `match_facts`. |
| `ends` | STRING | yes | End names, for example `Pavilion End, Shell End`. |
| `hosts` | STRING | yes | Hosts text from the info page. |

## `cricsocc.bronze.matches`

One finished match. The daily job skips a `match_id` that is already here.

| Column | Type | Null | Description |
|---|---|---|---|
| `match_id` | BIGINT | no | Cricbuzz match id. |
| `series_id` | BIGINT | no | `series.series_id`. |
| `venue_id` | BIGINT | yes | `venues.venue_id`. |
| `home_team_id` | BIGINT | no | Home `teams.team_id`. |
| `away_team_id` | BIGINT | no | Away `teams.team_id`. |
| `category` | STRING | no | Same controlled list as `series.category`. |
| `format_code` | STRING | no | Match format. |
| `status` | STRING | no | `finished` for stored rows. |
| `match_desc` | STRING | yes | Fixture line, for example `3rd Match`. |
| `start_time` | TIMESTAMP_NTZ | yes | Scheduled start in UTC. Null when the source timestamp is invalid. |
| `end_time` | TIMESTAMP_NTZ | yes | Completion time in UTC. |
| `day_night` | BOOLEAN | yes | Day/night flag from the scorecard. |
| `toss_text` | STRING | yes | Full toss sentence from the info page. |
| `toss_winner_team_id` | BIGINT | yes | Team that won the toss. |
| `toss_decision` | STRING | yes | `bat` or `bowl`. |
| `result_text` | STRING | yes | Result sentence. |
| `status_text` | STRING | yes | Cricbuzz status line. |
| `umpire1` | STRING | yes | First on-field umpire. |
| `umpire2` | STRING | yes | Second on-field umpire. |
| `umpire3` | STRING | yes | Third umpire. |
| `referee` | STRING | yes | Match referee. |
| `info_time_text` | STRING | yes | Time fact as written, including local and GMT. |
| `commentary_loaded` | BOOLEAN | no | True when at least one delivery row was stored. |
| `collected_at` | TIMESTAMP_NTZ | no | UTC time this match row was written. |

## `cricsocc.bronze.match_facts`

Every non-empty label on `/cricket-match-facts/{match_id}`. Playing XI, bench, and support staff are stored in `squads`, not here.

Common labels: `Match`, `Series`, `Venue`, `Stadium`, `City`, `Date`, `Time`, `Toss`, `Umpires`, `3rd Umpire`, `Referee`, `Capacity`, `Ends`, `Hosts To`, `Also Hosts`.

| Column | Type | Null | Description |
|---|---|---|---|
| `match_id` | BIGINT | no | `matches.match_id`. |
| `label` | STRING | no | Fact heading exactly as shown. |
| `value` | STRING | no | Fact value exactly as shown. Never blank. |

## `cricsocc.bronze.awards`

| Column | Type | Null | Description |
|---|---|---|---|
| `match_id` | BIGINT | no | `matches.match_id`. |
| `award` | STRING | no | `player of the match` or `player of the series`. |
| `player_id` | BIGINT | no | `players.player_id`. |
| `player_name` | STRING | no | Name as printed on the award. |
| `team_name` | STRING | yes | Team name when Cricbuzz sends it. |

## `cricsocc.bronze.squads`

Playing XI, bench, and support staff from the match info page. One person appears once per match and team.

| Column | Type | Null | Description |
|---|---|---|---|
| `match_id` | BIGINT | no | `matches.match_id`. |
| `team_id` | BIGINT | no | `teams.team_id`. |
| `player_id` | BIGINT | no | `players.player_id`. Support staff still use a Cricbuzz profile id. |
| `squad_group` | STRING | no | `playing`, `bench`, or `support`. |
| `sort_order` | INT | no | Order on the page, starting at 1 within the group. |
| `is_captain` | BOOLEAN | no | True when the name is marked `(c)` or `(c & wk)`. |
| `is_keeper` | BOOLEAN | no | True when the name is marked `(wk)` or `(c & wk)`. |

## `cricsocc.bronze.innings`

| Column | Type | Null | Description |
|---|---|---|---|
| `match_id` | BIGINT | no | `matches.match_id`. |
| `innings_number` | INT | no | 1-based innings id from Cricbuzz. |
| `batting_team_id` | BIGINT | yes | Batting `teams.team_id`. |
| `bowling_team_id` | BIGINT | yes | Bowling `teams.team_id`. |
| `runs` | INT | no | Innings total. Equals batting runs plus extras. |
| `wickets` | INT | no | Wickets fallen, 0 to 10. |
| `overs` | STRING | yes | Overs faced, for example `49.5`. |
| `run_rate` | DOUBLE | yes | Innings run rate. |
| `time_score` | STRING | yes | Time-of-innings text when present. |

## `cricsocc.bronze.extras`

One extras row per innings. A zero extras row is written when the card omits extras and batting already equals the innings total.

| Column | Type | Null | Description |
|---|---|---|---|
| `match_id` | BIGINT | no | `matches.match_id`. |
| `innings_number` | INT | no | `innings.innings_number`. |
| `total` | INT | no | Extra runs. |
| `wides` | INT | no | Wides. |
| `no_balls` | INT | no | No-balls. |
| `byes` | INT | no | Byes. |
| `leg_byes` | INT | no | Leg-byes. |
| `penalty` | INT | no | Penalty runs. |

## `cricsocc.bronze.batting`

| Column | Type | Null | Description |
|---|---|---|---|
| `match_id` | BIGINT | no | `matches.match_id`. |
| `innings_number` | INT | no | `innings.innings_number`. |
| `player_id` | BIGINT | no | `players.player_id`. |
| `bat_order` | INT | no | Position in the batting card. |
| `runs` | INT | no | Runs scored. |
| `balls` | INT | no | Balls faced. |
| `dots` | INT | yes | Dot balls. |
| `fours` | INT | no | Fours. |
| `sixes` | INT | no | Sixes. |
| `minutes` | INT | yes | Minutes batted. |
| `strike_rate` | DOUBLE | yes | Strike rate. |
| `is_out` | BOOLEAN | no | True when dismissed. |
| `dismissal` | STRING | yes | Dismissal text. |
| `is_captain` | BOOLEAN | no | Captain on this card. |
| `is_keeper` | BOOLEAN | no | Wicket-keeper on this card. |
| `bowler_id` | BIGINT | yes | Bowler credited with the wicket. |
| `fielder1_id` | BIGINT | yes | First fielder on the dismissal. |
| `fielder2_id` | BIGINT | yes | Second fielder. |
| `fielder3_id` | BIGINT | yes | Third fielder. |
| `wicket_code` | STRING | yes | Cricbuzz wicket code. |

## `cricsocc.bronze.bowling`

| Column | Type | Null | Description |
|---|---|---|---|
| `match_id` | BIGINT | no | `matches.match_id`. |
| `innings_number` | INT | no | `innings.innings_number`. |
| `player_id` | BIGINT | no | `players.player_id`. |
| `bowl_order` | INT | no | Position in the bowling card. |
| `overs` | STRING | no | Overs bowled. |
| `maidens` | INT | no | Maiden overs. |
| `runs` | INT | no | Runs conceded. |
| `wickets` | INT | no | Wickets taken. |
| `economy` | DOUBLE | yes | Economy rate. |
| `no_balls` | INT | no | No-balls. |
| `wides` | INT | no | Wides. |
| `dots` | INT | yes | Dot balls. |
| `balls` | INT | yes | Balls bowled. |

## `cricsocc.bronze.wickets`

Fall of wicket. Old cards may omit the tenth wicket.

| Column | Type | Null | Description |
|---|---|---|---|
| `match_id` | BIGINT | no | `matches.match_id`. |
| `innings_number` | INT | no | `innings.innings_number`. |
| `wicket_number` | INT | no | Wicket number, starting at 1. |
| `player_id` | BIGINT | yes | Dismissed batter when an id is present. |
| `player_name` | STRING | no | Dismissed batter name. |
| `runs` | INT | no | Team score at the fall. |
| `overs` | STRING | yes | Overs at the fall. |

## `cricsocc.bronze.partnerships`

| Column | Type | Null | Description |
|---|---|---|---|
| `match_id` | BIGINT | no | `matches.match_id`. |
| `innings_number` | INT | no | `innings.innings_number`. |
| `partnership_number` | INT | no | Partnership order in the innings. |
| `batter1_id` | BIGINT | yes | First batter id. |
| `batter1_name` | STRING | no | First batter name. |
| `batter1_runs` | INT | no | Runs for the first batter in this stand. |
| `batter1_balls` | INT | yes | Balls for the first batter in this stand. |
| `batter2_id` | BIGINT | yes | Second batter id. |
| `batter2_name` | STRING | no | Second batter name. |
| `batter2_runs` | INT | no | Runs for the second batter in this stand. |
| `batter2_balls` | INT | yes | Balls for the second batter in this stand. |
| `runs` | INT | no | Partnership runs. |
| `balls` | INT | yes | Partnership balls. |

## `cricsocc.bronze.powerplays`

| Column | Type | Null | Description |
|---|---|---|---|
| `match_id` | BIGINT | no | `matches.match_id`. |
| `innings_number` | INT | no | `innings.innings_number`. |
| `powerplay_number` | INT | no | Powerplay order in the innings. |
| `kind` | STRING | no | Cricbuzz powerplay type, for example mandatory. |
| `overs_from` | STRING | yes | Start over. |
| `overs_to` | STRING | yes | End over. |
| `runs` | INT | no | Runs in the block. |

## `cricsocc.bronze.commentary`

Legal deliveries only. `ball_number` is always greater than 0. Text has Cricbuzz `B0$` / `B1$` tokens stripped.

| Column | Type | Null | Description |
|---|---|---|---|
| `match_id` | BIGINT | no | `matches.match_id`. |
| `innings_number` | INT | no | `innings.innings_number`. |
| `sequence_number` | INT | no | Delivery order in that innings after sorting by ball. |
| `over_number` | DOUBLE | yes | Over as a decimal, for example `49.5`. |
| `ball_in_over` | INT | yes | Ball within the over, from the fractional over. |
| `ball_number` | INT | no | Cricbuzz ball number. Always `> 0`. |
| `event` | STRING | yes | Event flag such as `FOUR`, `SIX`, `WICKET`. |
| `batter_id` | BIGINT | yes | Striker id. Required if `bowler_id` is null. |
| `batter_name` | STRING | yes | Striker name. |
| `bowler_id` | BIGINT | yes | Bowler id. Required if `batter_id` is null. |
| `bowler_name` | STRING | yes | Bowler name. |
| `legal_runs` | INT | yes | Legal runs off the ball. |
| `total_runs` | INT | yes | Total runs off the ball, including extras. |
| `team_score` | INT | yes | Batting team score after the ball. |
| `commentary_text` | STRING | no | Delivery text. Never empty. |
