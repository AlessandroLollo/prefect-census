"""This is an example flows module"""
from prefect import flow

from prefect_census.tasks import (
    goodbye_prefect_census,
    hello_prefect_census,
)


@flow
def hello_and_goodbye():
    """
    Sample flow that says hello and goodbye!
    """
    print(hello_prefect_census)
    print(goodbye_prefect_census)
    return "Done"
