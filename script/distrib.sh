#!/bin/bash

cd `dirname $0`; 
PACKAGE_PATH=`pwd`
rm -rf "$PACKAGE_PATH/dist"
python setup.py sdist bdist_wheel --universal
twine upload dist/*
rm -rf "$PACKAGE_PATH/build"
rm -rf "$PACKAGE_PATH/dist"
rm -rf "$PACKAGE_PATH/*.egg-info"
