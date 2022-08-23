"""
An object that can be used to interact with Census APIs.
"""
from time import sleep
from typing import Dict, Optional

from requests import Session
from requests.auth import HTTPBasicAuth

from prefect_census.credentials import CensusCredentials
from prefect_census.exceptions import CensusAPIFailureException


class CensusClient:
    """
    Class that represents a Census client that can be used
    to interact with Census APIs.
    """

    # Census API base url
    __CENSUS_API_URL = "https://app.getcensus.com/api"

    # Census API version
    __CENSUS_API_VERSION = "v1"

    def __init__(self, credentials: CensusCredentials) -> None:
        self.credentials = credentials

    def __get_base_url(self) -> str:
        """
        Returns Census API base url.
        Returns:
            Census base URL.
        """
        return f"{self.__CENSUS_API_URL}/{self.__CENSUS_API_VERSION}"

    def __get_sync_run_url(self, sync_run_id: int) -> str:
        """
        Return the URL of the sync run given its identifier

        Returns:
            Census Sync Run URL
        """
        return f"{self.__get_base_url()}/sync_runs/{sync_run_id}"

    def __get_trigger_sync_run_url(self, sync_id: int) -> str:
        """
        Return the URL to trigger a sync given its identifier

        Returns:
            Census Trigger Sync URL
        """
        return f"{self.__get_base_url()}/syncs/{sync_id}/trigger"

    def __get_session(self) -> Session:
        """
        Returns a `requests.Session` object that can be used in subsequent API calls.

        Returns:
            Session object configured with the proper headers.
        """
        session = Session()
        session.auth = HTTPBasicAuth(
            username="bearer", password=self.credentials.access_token.get_secret_value()
        )

        return session

    def __call_api(
        self, api_url: str, params: Optional[Dict], http_method: str
    ) -> Dict:
        """
        Make an API call to the URL using the specified parameters and HTTP method.

        Args:
            api_url: The URL of the API to call.
            params: Optional parameters to pass to the GET API call.
            http_method: String representing the HTTP method
                to use to make the API call.

        Raises:
            `CensusAPIFailureException` if the response code is not 200.

        Returns:
            The API JSON response.
        """

        session = self.__get_session()
        http_fn = session.get if http_method == "GET" else session.post
        with http_fn(url=api_url, params=params) as response:
            if response.status_code != 200:
                err = f"There was an error while calling Census API: {response.reason}"
                raise CensusAPIFailureException(err)

            data = response.json()

            if data["status"] == "error":
                err = data["message"]
                msg = f"Census API responded with error: {err}"
                raise CensusAPIFailureException(msg)

            return response.json()

    def get_sync_run(self, sync_run_id: int) -> Dict:
        """
        Get a Sync Run given its identifier.

        Args:
            sync_run_id: The identifier of the sync run to retrieve.

        Returns:
            The JSON response of the [Census Sync Run API]
                (https://docs.getcensus.com/basics/api/sync-runs#get-sync_runs-id)
        """
        url = self.__get_sync_run_url(sync_run_id=sync_run_id)
        response = self.__call_api(
            api_url=url,
            params=None,
            http_method="GET",
        )

        if response["data"]["status"] == "failed":
            err = response["data"]["error_message"]
            msg = f"Census API failure: {err}"
            raise CensusAPIFailureException(msg)

        return response

    def trigger_sync_run(
        self,
        sync_id: int,
        force_full_sync: bool = False,
        wait_for_sync_run_completed: bool = False,
    ) -> Dict:
        """
        Trigger a new Sync Run given the Sync identifier.

        Args:
            sync_id: The identifier of the Sync to trigger.
            force_full_sync: Whether the sync should run in full refresh mode or not.
                Defaults to `False`.
            wait_for_sync_run_completed: Whether to wait for the sync run
                to complete or not. Defaults to `False`.

        Returns:
            If `wait_for_sync_run_completed` is `False` then returns the JSON response
                of the [Census Trigger Sync Run API]
                    (https://docs.getcensus.com/basics/api/syncs#post-syncs-id-trigger).
                Otherwise, returns the JSON response of the
                [Census Sync Run API]
                    (https://docs.getcensus.com/basics/api/sync-runs#get-sync_runs-id).
        """
        params = {"force_full_sync": force_full_sync} if force_full_sync else None
        url = self.__get_trigger_sync_run_url(sync_id=sync_id)
        response = self.__call_api(
            api_url=url,
            params=params,
            http_method="POST",
        )

        if wait_for_sync_run_completed:

            wait_time = 10
            sync_run_completed = False
            sync_run_id = response["data"]["sync_run_id"]

            while not sync_run_completed:

                sync_run_response = self.get_sync_run(sync_run_id=sync_run_id)
                sync_run_status = sync_run_response["data"]["status"]

                if sync_run_status == "working":
                    sleep(wait_time)
                elif sync_run_status == "completed":
                    response = sync_run_response
                    sync_run_completed = True

        return response
