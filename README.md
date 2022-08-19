# prefect-census

<a href="https://pypi.python.org/pypi/prefect-census/" alt="PyPI Version">
    <img src="https://badge.fury.io/py/prefect-census.svg" /></a>
<a href="https://github.com/AlessandroLollo/prefect-census/" alt="Stars">
    <img src="https://img.shields.io/github/stars/AlessandroLollo/prefect-census" /></a>
<a href="https://pepy.tech/badge/prefect-census/" alt="Downloads">
    <img src="https://pepy.tech/badge/prefect-census" /></a>
<a href="https://github.com/AlessandroLollo/prefect-census/pulse" alt="Activity">
    <img src="https://img.shields.io/github/commit-activity/m/AlessandroLollo/prefect-census" /></a>
<a href="https://github.com/AlessandroLollo/prefect-census/graphs/contributors" alt="Contributors">
    <img src="https://img.shields.io/github/contributors/AlessandroLollo/prefect-census" /></a>
<br>
<a href="https://prefect-community.slack.com" alt="Slack">
    <img src="https://img.shields.io/badge/slack-join_community-red.svg?logo=slack" /></a>
<a href="https://discourse.prefect.io/" alt="Discourse">
    <img src="https://img.shields.io/badge/discourse-browse_forum-red.svg?logo=discourse" /></a>

## Welcome!

Collections of tasks to interact with Census (Reverse ETL)

## Getting Started

### Python setup

Requires an installation of Python 3.7+.

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2.0. For more information about how to use Prefect, please refer to the [Prefect documentation](https://orion-docs.prefect.io/).

### Installation

Install `prefect-census` with `pip`:

```bash
pip install prefect-census
```

### Write and run a flow

```python
from prefect import flow
from prefect_census.tasks import (
    goodbye_prefect_census,
    hello_prefect_census,
)


@flow
def example_flow():
    hello_prefect_census
    goodbye_prefect_census

example_flow()
```

## Resources

If you encounter any bugs while using `prefect-census`, feel free to open an issue in the [prefect-census](https://github.com/AlessandroLollo/prefect-census) repository.

If you have any questions or issues while using `prefect-census`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).

## Development

If you'd like to install a version of `prefect-census` for development, clone the repository and perform an editable install with `pip`:

```bash
git clone https://github.com/AlessandroLollo/prefect-census.git

cd prefect-census/

pip install -e ".[dev]"

# Install linting pre-commit hooks
pre-commit install
```
