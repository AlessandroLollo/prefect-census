"""
Collection of tasks to interact with Census APIs.
More details about Census APIs can be found in the [official docs]
    (https://docs.getcensus.com/basics/api).
"""
from typing import Dict

from prefect import task

from prefect_census.census_client import CensusClient
from prefect_census.credentials import CensusCredentials


@task
def trigger_sync_run(
    credentials: CensusCredentials,
    sync_id: int,
    force_full_sync: bool = False,
    wait_for_sync_run_completed: bool = False,
    poll_status_every_n_seconds: int = CensusClient.get_min_poll_status_every_n_seconds(),  # noqa
) -> Dict:
    """
    This task triggers a new Sync run and, optionally, wait for it to complete.

    Args:
        credentials: Census credentials.
        sync_id: The identifier of the Sync.
        force_full_sync: Whether to run the sync in full refresh or not.
            Defaults to `False`.
        wait_for_sync_run_completed: Whether to wait for the sync
            run to complete or not. Defaults to `False`.
        poll_status_every_n_seconds: The number of seconds to wait
            between API calls.
    """
    client = CensusClient(credentials=credentials)
    return client.trigger_sync_run(
        sync_id=sync_id,
        force_full_sync=force_full_sync,
        wait_for_sync_run_completed=wait_for_sync_run_completed,
        poll_status_every_n_seconds=poll_status_every_n_seconds,
    )
