#!/bin/bash
set -eux
isort --check ${@:-sqly}
black --check ${@:-sqly}
flake8 ${@:-sqly}
# mypy -p ${@:-sqly}
