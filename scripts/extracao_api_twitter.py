"""Stand-alone extraction script (before moving the logic into an Airflow hook).

Calls the mock Twitter API at https://labdados.com for the last 24 hours and
prints every page as formatted JSON. No real token is required by the mock;
BEARER_TOKEN is read from the environment only to mirror the real Twitter API.

    python scripts/extracao_api_twitter.py
"""
import json
import os
from datetime import datetime, timedelta, timezone

import requests

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"  # UTC

now = datetime.now(timezone.utc)
end_time = now.strftime(TIMESTAMP_FORMAT)
start_time = (now - timedelta(days=1)).strftime(TIMESTAMP_FORMAT)
query = "datascience"

tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
user_fields = "expansions=author_id&user.fields=id,name,username,created_at"
url_raw = (
    f"https://labdados.com/2/tweets/search/recent?query={query}"
    f"&{tweet_fields}&{user_fields}&start_time={start_time}&end_time={end_time}"
)

headers = {"Authorization": f"Bearer {os.environ.get('BEARER_TOKEN', '')}"}

json_response = requests.get(url_raw, headers=headers, timeout=30).json()
print(json.dumps(json_response, indent=4, sort_keys=True))

# Follow pagination while the API returns a next_token
while "next_token" in json_response.get("meta", {}):
    url = f"{url_raw}&next_token={json_response['meta']['next_token']}"
    json_response = requests.get(url, headers=headers, timeout=30).json()
    print(json.dumps(json_response, indent=4, sort_keys=True))
