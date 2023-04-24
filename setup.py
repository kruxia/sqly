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
        install_requires=[
            "click~=8.1.3",
            "networkx~=3.1",
            "pydantic~=1.10.7",
            "PyYAML~=6.0",
        ],
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
                "pytest-cov~=4.0.0",
            ],
            "asyncpg": [
                "asyncpg~=0.27.0",
            ],
            "psycopg2": [
                "psycopg2-binary~=2.9.6",
            ],
            "pyodbc": [
                "pyodbc~=4.0.39",
            ],
            # The following packages wrap the DB-API with their own interfaces.
            "sqlalchemy": [
                "sqlalchemy",
            ],
            "databases": [
                "databases",
            ],
        },
        **CONFIG
    )
