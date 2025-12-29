## NCAA FCS Football Scraping Code ##

## Load Relevant Libraries ##

import requests
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
    "variables": '{"sportCode":"MFB","division":12,"seasonYear":{2024},"month":null,"contestDate":null,"week":{1}}'
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