"""This is an example tasks module"""
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
) -> Dict:
    """
    TODO
    """
    client = CensusClient(credentials=credentials)
    return client.trigger_sync_run(
        sync_id=sync_id,
        force_full_sync=force_full_sync,
        wait_for_sync_run_completed=wait_for_sync_run_completed,
    )
