import json
import os
import re
from codecs import open

from setuptools import find_packages, setup

PATH = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(PATH, "setup.json"), encoding="utf-8") as f:
    CONFIG = json.load(f)

with open(os.path.join(PATH, "README.md"), encoding="utf-8") as f:
    README = f.read()


def req_from_file(filename):
    """
    Return a list of requirements, suitable for setup.py install, from a pip -r req.txt file
    """
    txt = open(filename, "rb").read().decode("utf-8")
    lines = re.split(r"[\n\r]+", txt)
    return [line.split("#")[0].strip() for line in lines if line and line[0:2] != "-r"]


if __name__ == "__main__":
    setup(
        long_description=README,
        long_description_content_type="text/markdown",
        packages=find_packages(exclude=["contrib", "docs", "tests"]),
        include_package_data=True,
        install_requires=[
            "click~=8.1.3",
            "networkx~=2.8.8",
            "pydantic~=1.10.2",
            "PyYAML~=6.0",
        ],
        extras_require={
            "dev": [
                "black~=22.12.0",
                "ipython~=8.7.0",
                "isort~=5.11.3",
                "twine~=4.0.2",
            ],
            "test": [
                "black~=22.12.0",
                "flake8~=6.0.0",
                "pytest~=7.2.0",
                "pytest-cov~=4.0.0",
            ],
            "asyncpg": [
                "asyncpg~=0.27.0",
            ],
            "psycopg2": [
                "psycopg2-binary~=2.9.5",
            ],
            "pyodbc": [
                "pyodbc~=4.0.35",
            ],
            "sqlalchemy": [
                "sqlalchemy~=1.4.45",
            ],
        },
        **CONFIG
    )
