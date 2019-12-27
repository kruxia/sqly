import json
import os
from codecs import open

from setuptools import find_packages, setup

PATH = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(PATH, "setup.json"), encoding='utf-8') as f:
    CONFIG = json.load(f)

with open(os.path.join(PATH, 'README.md'), encoding='utf-8') as f:
    README = f.read()

if __name__ == '__main__':
    setup(
        long_description=README,
        long_description_content_type='text/markdown',
        packages=find_packages(exclude=['contrib', 'docs', 'tests']),
        include_package_data=True,
        **CONFIG
    )
