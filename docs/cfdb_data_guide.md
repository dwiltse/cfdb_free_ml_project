College Football Data | Starter Pack Guide

Version 1.0 – July 2025

Created by CFBD (CollegeFootballData.com)

College Football Data Starter Pack

(cid:4097)(cid:62680) Data Dictionary

This document summarizes the structure and purpose of each CSV file included

in the Starter Pack. It’s designed to help you navigate the dataset and understand

how to join and use the data effectively.

(cid:4097)(cid:63981) Metadata Files

conferences.csv

•

name: Full name of the conference

•

abbreviation: Short label (e.g., SEC, Big Ten)

•

division: NCAA division (typically "fbs")

teams.csv

Master reference of all teams with metadata. Includes classification, conference,

home venue info, location, elevation, timezone, and IDs.

(cid:4097)(cid:62431)(cid:65535)(cid:65295) Game-Level Data

games.csv

Master list of games from 1869–present. Includes team IDs, points, Elo ratings,

win probabilities, venue info, and game status metadata.

game_stats/YYYY.csv

Box score-style stats per team per game. Includes completions, sacks, turnovers,

time of possession, total yards, and efficiency metrics.

(cid:4097)(cid:62666) Season-Level Stats

season_stats/YYYY.csv

Raw season-long team stats. First downs, turnovers, penalties, rushing/passing

splits, and opponent stats.

advanced_season_stats/YYYY.csv

Advanced, derived season metrics. EPA, explosiveness, success rates, down

splits, field position, havoc, and efficiency.

(cid:4097)(cid:63980) Game-Level Advanced Stats

advanced_game_stats/YYYY.csv

Per-team, per-game advanced stats derived from play-by-play. Includes

explosiveness, success rate, EPA, line yards, opportunity stats.

(cid:4097)(cid:62724) Play & Drive Data

drives/drives_YYYY.csv

One row per drive. Periods, starting/ending yard lines, score changes, result

types.

plays/YYYY/SEASONTYPE_WEEK_plays.csv

Play-by-play data. Down, distance, yardage, play type, and a custom PPA value

for each play.

(cid:4097)(cid:63977) Relationships & Integration Tips

•

Use gameId to join plays → drives → games → advanced stats

•

Use team_id or school to join team stats with teams.csv

•

Normalize time columns like possessionTime to seconds for analysis

Thank you for supporting this project on Patreon! Your support helps keep this

data available and growing. If you have questions, feedback, or ideas, hop into the

Discord or join one of our monthly Office Hours sessions.

For updates, new tools, or additional notebooks, check your Patreon feed or visit

collegefootballdata.com.


