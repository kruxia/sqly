import json
import os
import re
from codecs import open

from setuptools import find_packages, setup

PATH = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(PATH, "setup.json"), encoding='utf-8') as f:
    CONFIG = json.load(f)

with open(os.path.join(PATH, 'README.md'), encoding='utf-8') as f:
    README = f.read()


def req_from_file(filename):
    """
    Return a list of requirements, suitable for setup.py install, from a pip -r req.txt file
    """
    txt = open(filename, 'rb').read().decode('utf-8')
    lines = re.split(r'[\n\r]+', txt)
    return [line.split('#')[0].strip() for line in lines if line and line[0:2] != '-r']


if __name__ == '__main__':
    setup(
        long_description=README,
        long_description_content_type='text/markdown',
        packages=find_packages(exclude=['contrib', 'docs', 'tests']),
        include_package_data=True,
        install_requires=req_from_file(os.path.join(PATH, 'req/install.txt')),
        **CONFIG
    )
