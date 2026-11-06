"""Custom operator: calls TwitterHook and writes one JSON page per line to `file_path`."""
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Make `hook/` importable when this file is loaded by the DAG or run directly.
sys.path.append(str(Path(__file__).resolve().parent.parent))

from airflow.sdk import BaseOperator  # noqa: E402

from hook.twitter_hook import TIMESTAMP_FORMAT, TwitterHook  # noqa: E402


class TwitterOperator(BaseOperator):
    # These attributes accept Jinja templates such as {{ ds }}.
    template_fields = ("file_path", "start_time", "end_time", "query")

    def __init__(self, file_path, end_time, start_time, query, **kwargs):
        self.file_path = file_path
        self.end_time = end_time
        self.start_time = start_time
        self.query = query
        super().__init__(**kwargs)

    def create_parent_folder(self):
        Path(self.file_path).parent.mkdir(parents=True, exist_ok=True)

    def execute(self, context):
        self.create_parent_folder()
        with open(self.file_path, "w", encoding="utf-8") as output_file:
            for pg in TwitterHook(self.end_time, self.start_time, self.query).run():
                json.dump(pg, output_file, ensure_ascii=False)
                output_file.write("\n")


if __name__ == "__main__":
    # Manual test: writes datalake/twitter_datascience/extract_date=<today>/... in the current folder.
    now = datetime.now(timezone.utc)
    today = now.date().isoformat()
    op = TwitterOperator(
        task_id="test_run",
        file_path=f"datalake/twitter_datascience/extract_date={today}/datascience_{today}.json",
        query="datascience",
        start_time=(now - timedelta(days=1)).strftime(TIMESTAMP_FORMAT),
        end_time=now.strftime(TIMESTAMP_FORMAT),
    )
    op.execute(context={})
