import pytest
import responses
from prefect import flow
from pydantic import SecretStr

from prefect_census.credentials import CensusCredentials
from prefect_census.exceptions import CensusAPIFailureException
from prefect_census.tasks import trigger_sync_run


def test_trigger_sync_run_raises():
    @flow(name="failing_flow")
    def test_flow():
        creds = CensusCredentials(access_token=SecretStr("foo"))
        return trigger_sync_run(credentials=creds, sync_id=1234)

    with pytest.raises(CensusAPIFailureException):
        test_flow()


@responses.activate
def test_trigger_sync_no_wait_succeed():
    api_url = "https://app.getcensus.com/api/v1/syncs/1234/trigger"
    responses.add(
        method=responses.POST,
        url=api_url,
        status=200,
        json={"status": "success", "data": {"sync_run_id": 1234567890}},
    )

    @flow(name="no_wait_success_flow")
    def test_flow():
        creds = CensusCredentials(access_token=SecretStr("foo"))
        return trigger_sync_run(credentials=creds, sync_id=1234)

    response = test_flow()

    assert response == {"status": "success", "data": {"sync_run_id": 1234567890}}


@responses.activate
def test_trigger_sync_with_wait_succeed():
    sync_id = 1234
    trigger_sync_api_url = f"https://app.getcensus.com/api/v1/syncs/{sync_id}/trigger"

    sync_run_id = 1234567890
    sync_run_api_url = f"https://app.getcensus.com/api/v1/sync_runs/{sync_run_id}"

    responses.add(
        method=responses.POST,
        url=trigger_sync_api_url,
        status=200,
        json={"status": "success", "data": {"sync_run_id": sync_run_id}},
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

    @flow(name="wait_flow")
    def test_flow():
        creds = CensusCredentials(access_token=SecretStr("foo"))
        return trigger_sync_run(
            credentials=creds, sync_id=sync_id, wait_for_sync_run_completed=True
        )

    response = test_flow()

    assert response == {
        "status": "success",
        "data": {"status": "completed", "records_processed": 1},
    }
