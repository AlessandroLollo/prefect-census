"""Census credentials block"""
from prefect.blocks.core import Block
from pydantic import SecretStr


class CensusCredentials(Block):
    """
    Block used to manage authentication with Census.
    Args:
        access_token (SecretStr): The Access token to use to connect to Census.
    Example:
        Load stored Stitch credentials
        ```python
        from prefect_census.credentials import CensusCredentials
        census_credentials_block = CensusCredentials.load("BLOCK_NAME")
        ```
    """  # noqa E501

    access_token: SecretStr

    _block_type_name = "Census Credentials"

    _logo_url = "http://TODO.foo"  # noqa
