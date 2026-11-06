"""Checks that run without network access: pytest tests/"""
import json
import sys
from pathlib import Path
from unittest import mock

import pytest
from airflow.dag_processing.dagbag import DagBag

ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))
SAMPLE = next((ROOT / "sample_data" / "datalake").rglob("*.json"))

from hook.twitter_hook import TwitterHook  # noqa: E402
from operators.twitter_operator import TwitterOperator  # noqa: E402


def test_no_import_errors():
    bag = DagBag(dag_folder=str(ROOT / "dags"))
    assert bag.import_errors == {}
    task = bag.dags["TwitterTest"].get_task("test_run")
    assert "extract_date={{ ds }}" in task.file_path
    assert Path(task.file_path).is_absolute()


def test_create_url():
    hook = TwitterHook("2026-09-18T00:00:00.00Z", "2026-09-17T00:00:00.00Z", "datascience")
    hook.base_url = "http://labdados.com"
    url = hook.create_url()
    assert url.startswith("http://labdados.com/2/tweets/search/recent?query=datascience&")
    assert "start_time=2026-09-17T00:00:00.00Z&end_time=2026-09-18T00:00:00.00Z" in url


def test_paginate_follows_next_token():
    pages = [{"data": [1], "meta": {"next_token": "abc"}}, {"data": [2], "meta": {}}]
    responses = iter(mock.Mock(json=mock.Mock(return_value=p)) for p in pages)
    hook = TwitterHook("e", "s", "q")
    with mock.patch.object(TwitterHook, "connect_to_endpoint", side_effect=lambda url, s: next(responses)) as c:
        result = hook.paginate("http://x/2/tweets?query=q", session=None)
    assert result == pages
    assert c.call_args_list[1].args[0].endswith("&next_token=abc")


def test_operator_writes_one_json_per_line(tmp_path):
    pages = [json.loads(line) for line in SAMPLE.read_text(encoding="utf-8").splitlines()]
    out = tmp_path / "extract_date=2026-09-17" / "datascience_20260917.json"
    op = TwitterOperator(task_id="t", file_path=str(out), query="q", start_time="s", end_time="e")
    with mock.patch.object(TwitterHook, "run", return_value=pages):
        op.execute(context={})
    lines = out.read_text(encoding="utf-8").splitlines()
    assert len(lines) == len(pages)
    assert all("data" in json.loads(line) for line in lines)


@pytest.mark.parametrize("field", ["data", "includes"])
def test_sample_has_api_shape(field):
    first = json.loads(SAMPLE.read_text(encoding="utf-8").splitlines()[0])
    assert field in first
