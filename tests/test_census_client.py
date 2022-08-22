import pytest
import responses
from pydantic import SecretStr
from responses import matchers

from prefect_census.census_client import CensusClient
from prefect_census.credentials import CensusCredentials
from prefect_census.exceptions import CensusAPIFailureException


def test_census_client_construction():

    client = CensusClient(credentials=CensusCredentials(access_token=SecretStr("foo")))

    assert client.credentials.access_token.get_secret_value() == "foo"


@responses.activate
def test_get_sync_run_raises():
    api_url = "https://app.getcensus.com/api/v1/sync_runs/1234567890"
    responses.add(method=responses.GET, url=api_url, status=123)

    msg_match = "There was an error while calling Census API"
    with pytest.raises(CensusAPIFailureException, match=msg_match):
        creds = CensusCredentials(access_token=SecretStr("foo"))
        client = CensusClient(credentials=creds)

        client.get_sync_run(sync_run_id=1234567890)


@responses.activate
def test_get_sync_run_failed_raises():
    api_url = "https://app.getcensus.com/api/v1/sync_runs/1234567890"
    responses.add(
        method=responses.GET,
        url=api_url,
        status=200,
        json={
            "status": "success",
            "data": {"status": "failed", "error_message": "failed!"},
        },
    )

    msg_match = "Census API failure: failed!"
    with pytest.raises(CensusAPIFailureException, match=msg_match):
        creds = CensusCredentials(access_token=SecretStr("foo"))
        client = CensusClient(credentials=creds)

        client.get_sync_run(sync_run_id=1234567890)


@responses.activate
def test_get_sync_run_succeed():
    api_url = "https://app.getcensus.com/api/v1/sync_runs/1234567890"
    responses.add(
        method=responses.GET,
        url=api_url,
        status=200,
        json={
            "status": "success",
            "data": {"id": 1234567890, "sync_id": 1234, "status": "completed"},
        },
    )

    creds = CensusCredentials(access_token=SecretStr("foo"))
    client = CensusClient(credentials=creds)

    response = client.get_sync_run(sync_run_id=1234567890)

    assert response["data"] == {
        "id": 1234567890,
        "sync_id": 1234,
        "status": "completed",
    }


@responses.activate
def test_trigger_sync_run_raises():
    api_url = "https://app.getcensus.com/api/v1/syncs/1234/trigger"
    responses.add(method=responses.POST, url=api_url, status=123)

    msg_match = "There was an error while calling Census API"
    with pytest.raises(CensusAPIFailureException, match=msg_match):
        creds = CensusCredentials(access_token=SecretStr("foo"))
        client = CensusClient(credentials=creds)

        client.trigger_sync_run(sync_id=1234)


@responses.activate
def test_trigger_sync_run_respond_with_error_raises():
    api_url = "https://app.getcensus.com/api/v1/syncs/1234/trigger"
    responses.add(
        method=responses.POST,
        url=api_url,
        status=200,
        json={"status": "error", "message": "panic!"},
    )

    msg_match = "Census API responded with error: panic!"
    with pytest.raises(CensusAPIFailureException, match=msg_match):
        creds = CensusCredentials(access_token=SecretStr("foo"))
        client = CensusClient(credentials=creds)

        client.trigger_sync_run(sync_id=1234)


@responses.activate
def test_trigger_sync_run_full_sync_respond_with_error_raises():
    api_url = "https://app.getcensus.com/api/v1/syncs/1234/trigger"
    params = {"force_full_sync": True}
    responses.add(
        method=responses.POST,
        url=api_url,
        status=200,
        match=[matchers.query_param_matcher(params)],
        json={"status": "error", "message": "panic!"},
    )

    msg_match = "Census API responded with error: panic!"
    with pytest.raises(CensusAPIFailureException, match=msg_match):
        creds = CensusCredentials(access_token=SecretStr("foo"))
        client = CensusClient(credentials=creds)

        client.trigger_sync_run(sync_id=1234, force_full_sync=True)


@responses.activate
def test_trigger_sync_run_succeed():
    api_url = "https://app.getcensus.com/api/v1/syncs/1234/trigger"
    params = {"force_full_sync": True}
    responses.add(
        method=responses.POST,
        url=api_url,
        status=200,
        match=[matchers.query_param_matcher(params)],
        json={"status": "success", "data": {"sync_run_id": 1234567890}},
    )

    creds = CensusCredentials(access_token=SecretStr("foo"))
    client = CensusClient(credentials=creds)

    response = client.trigger_sync_run(sync_id=1234, force_full_sync=True)

    assert response == {"status": "success", "data": {"sync_run_id": 1234567890}}


@responses.activate
def test_trigger_sync_with_wait_failed_sync_run_raises():
    sync_id = 1234
    trigger_sync_api_url = f"https://app.getcensus.com/api/v1/syncs/{sync_id}/trigger"
    params = {"force_full_sync": False}

    sync_run_id = 1234567890
    sync_run_api_url = f"https://app.getcensus.com/api/v1/sync_runs/{sync_run_id}"

    responses.add(
        method=responses.POST,
        url=trigger_sync_api_url,
        status=200,
        json={"status": "success", "data": {"sync_run_id": sync_run_id}},
        match=[matchers.query_param_matcher(params)],
    )

    responses.add(
        method=responses.GET,
        url=sync_run_api_url,
        status=200,
        json={
            "status": "success",
            "data": {"status": "failed", "error_message": "failed!"},
        },
    )

    msg_match = "Census API failure: failed!"
    with pytest.raises(CensusAPIFailureException, match=msg_match):
        creds = CensusCredentials(access_token=SecretStr("foo"))
        client = CensusClient(credentials=creds)

        client.trigger_sync_run(sync_id=sync_id, wait_for_sync_run_completed=True)

    assert (
        responses.assert_call_count(f"{trigger_sync_api_url}?force_full_sync=False", 1)
        is True
    )
    assert responses.assert_call_count(sync_run_api_url, 1) is True


@responses.activate
def test_trigger_sync_run_with_wait_succeed():
    sync_id = 1234
    trigger_sync_api_url = f"https://app.getcensus.com/api/v1/syncs/{sync_id}/trigger"
    params = {"force_full_sync": False}

    sync_run_id = 1234567890
    sync_run_api_url = f"https://app.getcensus.com/api/v1/sync_runs/{sync_run_id}"

    responses.add(
        method=responses.POST,
        url=trigger_sync_api_url,
        status=200,
        json={"status": "success", "data": {"sync_run_id": sync_run_id}},
        match=[matchers.query_param_matcher(params)],
    )

    responses.add(
        method=responses.GET,
        url=sync_run_api_url,
        status=200,
        json={
            "status": "success",
            "data": {"status": "completed", "records_processed": 1},
        },
    )

    creds = CensusCredentials(access_token=SecretStr("foo"))
    client = CensusClient(credentials=creds)

    client.trigger_sync_run(sync_id=sync_id, wait_for_sync_run_completed=True)

    assert (
        responses.assert_call_count(f"{trigger_sync_api_url}?force_full_sync=False", 1)
        is True
    )
    assert responses.assert_call_count(sync_run_api_url, 1) is True


@responses.activate
def test_trigger_sync_run_with_wait_after_wait_succeed():
    sync_id = 1234
    trigger_sync_api_url = f"https://app.getcensus.com/api/v1/syncs/{sync_id}/trigger"
    params = {"force_full_sync": False}

    sync_run_id = 1234567890
    sync_run_api_url = f"https://app.getcensus.com/api/v1/sync_runs/{sync_run_id}"

    responses.add(
        method=responses.POST,
        url=trigger_sync_api_url,
        status=200,
        json={"status": "success", "data": {"sync_run_id": sync_run_id}},
        match=[matchers.query_param_matcher(params)],
    )

    responses.add(
        method=responses.GET,
        url=sync_run_api_url,
        status=200,
        json={"status": "success", "data": {"status": "working"}},
    )

    responses.add(
        method=responses.GET,
        url=sync_run_api_url,
        status=200,
        json={
            "status": "success",
            "data": {"status": "completed", "records_processed": 1},
        },
    )

    creds = CensusCredentials(access_token=SecretStr("foo"))
    client = CensusClient(credentials=creds)

    client.trigger_sync_run(sync_id=sync_id, wait_for_sync_run_completed=True)

    assert (
        responses.assert_call_count(f"{trigger_sync_api_url}?force_full_sync=False", 1)
        is True
    )
    assert responses.assert_call_count(sync_run_api_url, 2) is True
