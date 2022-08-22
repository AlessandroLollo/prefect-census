from pydantic import SecretStr

from prefect_census.credentials import CensusCredentials


def test_credentials_construction():
    cred = CensusCredentials(access_token=SecretStr("foo"))

    assert cred.access_token.get_secret_value() == "foo"
