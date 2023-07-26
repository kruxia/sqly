import json
import os
from pathlib import Path

from setuptools import find_packages, setup

PATH = Path(os.path.dirname(os.path.abspath(__file__)))

with open(PATH / "setup.json") as f:
    CONFIG = json.load(f)

with open(PATH / "README.md") as f:
    README = f.read()


if __name__ == "__main__":
    setup(
        long_description=README,
        long_description_content_type="text/markdown",
        packages=find_packages(exclude=["contrib", "docs", "tests"]),
        include_package_data=True,
        install_requires=[],
        extras_require={
            "dev": [
                "black~=23.3.0",
                "ipython~=8.12.0",
                "isort~=5.12.0",
                "twine~=4.0.2",
            ],
            "docs": [
                "sqly[migration]",
                "mkdocs~=1.4.3",
                "mkdocs-autorefs~=0.4.1",
                "mkdocs-click~=0.8.0",
                "mkdocs-material~=9.1.19",
                "mkdocs-material-extensions~=1.1.1",
                "mkdocstrings~=0.22.0",
                "mkdocstrings-python~=1.2.1",
            ],
            "test": [
                "sqly[migration]",
                "black~=23.3.0",
                "flake8~=6.0.0",
                "mypy",
                "pytest~=7.3.1",
                "pytest-click~=1.1.0",
                "pytest-cov~=4.0.0",
                # all the supported adaptors have to be installed to test
                "psycopg[binary]~=3.1.9",
                # "mysqlclient~=2.2.0",
                # # TODO: Remove these???
                # "SQLAlchemy~=1.4.49",
                # "asyncpg~=0.28.0",
                # "databases~=0.7.0",
            ],
            "migration": [
                "click~=8.1.3",
                "networkx~=3.1",
                "PyYAML~=6.0",
            ],
            # postgresql DB interfaces
            "psycopg": [
                "psycopg[binary]~=3.1.9",
            ],
            # "mysql": [
            #     "mysqlclient~=2.2.0",
            # ]
            # # all others can be done via ODBC
            # "pyodbc": [
            #     "pyodbc~=4.0.39",
            # ],
        },
        **CONFIG
    )
