"""Custom Airflow hook for a Twitter-like "recent search" API.

The course uses a public mock of the Twitter v2 API (https://labdados.com) that
returns fictitious tweets, so no real Twitter/X credentials are needed.
Point the connection at it before running:
    export AIRFLOW_CONN_TWITTER_DEFAULT="http://labdados.com"
"""
import json
from datetime import datetime, timedelta, timezone

import requests
from airflow.providers.http.hooks.http import HttpHook

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"
MAX_PAGES = 100  # safety limit for pagination


class TwitterHook(HttpHook):
    def __init__(self, end_time, start_time, query, conn_id=None):
        self.end_time = end_time
        self.start_time = start_time
        self.query = query
        self.conn_id = conn_id or "twitter_default"
        super().__init__(http_conn_id=self.conn_id)

    def create_url(self):
        tweet_fields = (
            "tweet.fields=author_id,conversation_id,created_at,id,"
            "in_reply_to_user_id,public_metrics,lang,text"
        )
        user_fields = "expansions=author_id&user.fields=id,name,username,created_at"
        return (
            f"{self.base_url}/2/tweets/search/recent?query={self.query}"
            f"&{tweet_fields}&{user_fields}"
            f"&start_time={self.start_time}&end_time={self.end_time}"
        )

    def connect_to_endpoint(self, url, session):
        request = requests.Request("GET", url)
        prep = session.prepare_request(request)
        self.log.info("URL: %s", url)
        return self.run_and_check(session, prep, {})

    def paginate(self, url_raw, session):
        """Follow `meta.next_token` until there are no more pages (or MAX_PAGES)."""
        pages = []
        json_response = self.connect_to_endpoint(url_raw, session).json()
        pages.append(json_response)

        while "next_token" in json_response.get("meta", {}) and len(pages) < MAX_PAGES:
            next_token = json_response["meta"]["next_token"]
            url = f"{url_raw}&next_token={next_token}"
            json_response = self.connect_to_endpoint(url, session).json()
            pages.append(json_response)

        return pages

    def run(self):
        session = self.get_conn()  # also sets self.base_url from the connection
        url_raw = self.create_url()
        return self.paginate(url_raw, session)


if __name__ == "__main__":
    # Quick manual test (needs AIRFLOW_CONN_TWITTER_DEFAULT, see module docstring).
    now = datetime.now(timezone.utc)
    end_time = now.strftime(TIMESTAMP_FORMAT)
    start_time = (now - timedelta(days=1)).strftime(TIMESTAMP_FORMAT)

    for pg in TwitterHook(end_time, start_time, "datascience").run():
        print(json.dumps(pg, indent=4, sort_keys=True))
