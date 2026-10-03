from datetime import datetime, timedelta
import os
import requests
import json

# Set the time zone and timestamp format
time_zone = datetime.now().astimezone().tzname()
TIMESTAMP_FORMAT = f"%Y-%m-%dT%H:%M:%S.00{time_zone}:00"

# Define the start and end time for the query
end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
start_time = (datetime.now() + timedelta(days=-1)).date().strftime(TIMESTAMP_FORMAT)
query = "data science"

# Define the tweet fields and user fields to retrieve
tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
user_fields = "expansions=author_id&user.fields=id,name,username,created_at"

# Construct the URL for the Twitter API request
url_raw = f"https://labdados.com/2/tweets/search/recent?query={query}&{tweet_fields}&{user_fields}&start_time={start_time}&end_time={end_time}"

# Make the request to the Twitter API
bearer_token = os.environ.get("BEARER_TOKEN")
headers = {"Authorization": "Bearer {}".format(bearer_token)}
response = requests.request("GET", url_raw, headers=headers)

# Print the response in a formatted way
json_response = response.json()
print(json.dumps(json_response, indent=4, sort_keys=True))

# Paginate through results if there are more pages
while "next_token" in json_response.get("meta", {}):
    next_token = json_response["meta"]["next_token"]
    url_raw = f"https://labdados.com/2/tweets/search/recent?query={query}&{tweet_fields}&{user_fields}&start_time={start_time}&end_time={end_time}&next_token={next_token}"
    response = requests.request("GET", url_raw, headers=headers)
    json_response = response.json()
    print(json.dumps(json_response, indent=4, sort_keys=True))