#!/bin/bash
set -eux

PACKAGE_PATH=$(dirname $(dirname $0))
cd $PACKAGE_PATH
rm -rf "$PACKAGE_PATH/dist"
python setup.py sdist bdist_wheel --universal
twine upload dist/* --verbose
rm -rf "$PACKAGE_PATH/build"
rm -rf "$PACKAGE_PATH/dist"
rm -rf "$PACKAGE_PATH/*.egg-info"
