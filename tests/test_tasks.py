from prefect import flow

from prefect_census.tasks import (
    goodbye_prefect_census,
    hello_prefect_census,
)


def test_hello_prefect_census():
    @flow
    def test_flow():
        return hello_prefect_census()

    result = test_flow()
    assert result == "Hello, prefect-census!"


def goodbye_hello_prefect_census():
    @flow
    def test_flow():
        return goodbye_prefect_census()

    result = test_flow()
    assert result == "Goodbye, prefect-census!"
