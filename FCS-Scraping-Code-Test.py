## NCAA FCS Football Scraping Code ##

## Load Relevant Libraries ##

import requests
import json
import urllib.parse
import pandas as pd

## Specify Base URL and Parameters ##

base = "https://sdataprod.ncaa.com/"

season = 2024
week = 1

params = {
    "meta": "GetContests_web",
    "queryName": "GetContests_web",
    "extensions": '{"persistedQuery":{"version":1,"sha256Hash":"7287cda610a9326931931080cb3a604828febe6fe3c9016a7e4a36db99efdb7c"}}',
    "variables": '{"sportCode":"MFB","division":12,"seasonYear":2024,"month":null,"contestDate":null,"week":1}'
}

url = base + "?" + urllib.parse.urlencode(params)

headers = {"User-Agent": "Mozilla/5.0"}

resp = requests.get(url, headers=headers)
resp.raise_for_status()

data = resp.json()

contests = data["data"]["contests"]

games = []

for c in contests:
    game_id = c["contestId"]
    relative_url = c["url"]
    full_url = "https://www.ncaa.com" + relative_url
    games.append((game_id, full_url))

print(games)

def fetch_pbp(contest_id: str):
    base = "https://sdataprod.ncaa.com/"
    
    params = {
        "meta": "NCAA_GetGamecenterPbpFootballById_web",
        "extensions": '{"persistedQuery":{"version":1,"sha256Hash":"47928f2cabc7a164f0de0ed535a623bdf5a852cce7c30d6a6972a38609ba46a2"}}',
        "variables": f'{{"contestId":"{contest_id}","staticTestEnv":null}}'
    }
    
    url = base + "?" + urllib.parse.urlencode(params)
    headers = {"User-Agent": "Mozilla/5.0"}
    
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    
    return resp.json()

# Example usage with McNeese State vs Tarleton State (8/24/2024) #

pbp_data = fetch_pbp("6306733")
print(pbp_data.keys())

# Now, pbp_data contains the play-by-play data for the specified contest ID.
# However, it is in a nested json format. We will need to flatten it for 
# easier usage and analysis using pandas. #

# Test Code Modification #

pbp_plays = pbp_data['data']['playbyplay']['periods'][0]

# Flatten the top-level list into rows 
df = pd.json_normalize(pbp_plays, record_path=['playbyplayStats'],sep=".")

# Explode the nested 'plays' column to create individual rows for each play
df = df.explode('plays', ignore_index=True)

# 3) Normalize the dicts in 'plays' into columns and join back
plays_cols = pd.json_normalize(df["plays"], sep=".")
df = pd.concat([df.drop(columns=["plays"]), plays_cols.add_prefix("play.")], axis=1)

# Bring in to=level period info 
df['periodnumber'] = pbp_plays.get('periodNumber')

df["periodDisplay"] = pbp_plays.get("periodDisplay")

# Optional: clean column names
df = df.rename(columns={
    "__typename": "rowType",
    "clock": "team.clock",
    "teamId": "teamId",
    "play.__typename": "play.type",
    "play.playText": "play.text",
    "play.driveText": "play.drive",
    "play.homeScore": "homeScore",
    "play.visitorScore": "visitorScore",
    "play.clock": "play.clock"
})

# Extract Team Level Data in a similar manner 

team1 = pd.DataFrame([pbp_data['data']['playbyplay']['teams'][0]])
team2 = pd.DataFrame([pbp_data['data']['playbyplay']['teams'][1]])

# Merge team names into main dataframe based on teamId

df['teamId'] = df['teamId'].astype(str).str.strip()
team1['teamId'] = team1['teamId'].astype(str).str.strip()
team2['teamId'] = team2['teamId'].astype(str).str.strip()

# When left joining, add suffixes to distinguish between the two teams
# if team1['isHome'] is True, then suffix _home else _away

if team1['isHome'].iloc[0] == True:
    team1 = team1.drop(columns=['isHome']).add_suffix('_home')
    team1_cols = team1.columns.tolist()
    team2 = team2.drop(columns=['isHome']).add_suffix('_away')
    team2_cols = team2.columns.tolist()
else:
    team1 = team1.drop(columns=['isHome']).add_suffix('_away')
    team1_cols = team1.columns.tolist()
    team2 = team2.drop(columns=['isHome']).add_suffix('_home')
    team2_cols = team2.columns.tolist()

df = df.merge(team1, left_on='teamId', right_on='teamId_home', how='left')
df = df.merge(team2, left_on='teamId', right_on='teamId_away', how='left')

# Where NAs exist in team name columns, fill them in from the other teams
# Since I have team1_cols and team2_cols lists, I can loop through them


# For each pair of home/away columns, fill NaN with the other column's value
for col in df.columns:
    if col.endswith("_home"):
        away_col = col.replace("_home", "_away")
        if away_col in df.columns:
            df[col] = df[col].fillna(df[away_col])
    elif col.endswith("_away"):
        home_col = col.replace("_away", "_home")
        if home_col in df.columns:
            df[col] = df[col].fillna(df[home_col])

    

# Create new play_type column based on play.type. 
# First, if the play.text contains "kickoff", label as "kickoff"

def determine_play_type(row):
    if pd.isna(row['play.text']):
        return 'unknown'
    text = row['play.text'].lower()
    if 'kickoff' in text:
        return 'kickoff'
    elif 'punt' in text:
        return 'punt'
    elif 'field goal attempt' in text:
        return 'field goal attempt'
    elif 'pass complete' in text or 'pass incomplete' in text or 'sacked' in text:
        return 'pass'
    elif 'rush' in text or 'run' in text:
        return 'rush'
    elif 'penalty' in text:
        return 'penalty'
    else:
        return 'other'

df['play_type'] = df.apply(determine_play_type, axis=1)

# If home or visitor score is missing, delete row 

df = df.dropna(subset=['homeScore', 'visitorScore'])

# If team.clock is <empty>, delete row

df = df[df['team.clock'].str.strip() != '']

# If play.text contains "drive start", delete row

df = df[~df['play.text'].str.contains('drive start', case=False, na=False)]

# Add a down column based on play.drive column (e.g., 1 and 10 at 25 implies that down = 1)


def extract_down(row):
    drive = row.get('play.drive')
    if not isinstance(drive, str) or not drive.strip():
        return None

    parts = drive.split(' and ')
    if parts:
        try:
            return int(parts[0])
        except (ValueError, TypeError):
            return None
        return None


df['down'] = df.apply(extract_down, axis=1)

# Do same for yards_to_go column


def extract_yards_to_go(row):
    drive = row.get('play.drive')
    if not isinstance(drive, str) or not drive.strip():
        return None

    parts = drive.split(' and ')
    if len(parts) > 1:
        try:
            # parts[1] looks like '10 at 35', so split by ' at ' and take first
            return int(parts[1].split(' at ')[0])
        except (ValueError, TypeError):
            return None
        return None


df['yards_to_go'] = df.apply(extract_yards_to_go, axis=1)

# Create a possessing_team column based on teamId and the two teams' IDs

def get_possessing_team(row):
    team_id = row['teamId']
    if team_id == row['teamId_home']:
        return row['nameShort_home']
    elif team_id == row['teamId_away']:
        return row['nameShort_away']
    else:
        return None

df['possessing_team'] = df.apply(get_possessing_team, axis=1)
