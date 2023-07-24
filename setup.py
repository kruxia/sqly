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
            "test": [
                "black~=23.3.0",
                "flake8~=6.0.0",
                "pytest~=7.3.1",
                "pytest-click~=1.1.0",
                "pytest-cov~=4.0.0",
                # all the supported adaptors have to be installed to test
                "sqlalchemy",
                "databases",
                "psycopg[binary]~=3.1.9",
                "asyncpg~=0.27.0",
                "psycopg~=3.1.9",
            ],
            "migration": [
                "click~=8.1.3",
                "networkx~=3.1",
                "PyYAML~=6.0",
                # for now, require psycopg for migrations. Relax this requirement later.
                "psycopg~=3.1.9",
            ],
            # # postgresql DB interfaces
            "asyncpg": [
                "asyncpg~=0.27.0",
            ],
            "psycopg": [
                "psycopg[binary]~=3.1.9",
            ],
            # # all others can be done via ODBC
            # "pyodbc": [
            #     "pyodbc~=4.0.39",
            # ],
            # # DB interface packages that wrap the DB-API.
            "sqlalchemy": [
                "sqlalchemy",
            ],
            "databases": [
                "databases",
            ],
        },
        **CONFIG
    )
