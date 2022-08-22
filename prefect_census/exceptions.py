"""
Exceptions to be used when interacting with Census APIs.
"""


class CensusAPIFailureException(Exception):
    """
    Exception to raise when a Census API return a response code != 200.
    """

    pass
