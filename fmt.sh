#!/bin/bash
PATHS="."
isort -q -rc $PATHS
black -q $PATHS
flake8
