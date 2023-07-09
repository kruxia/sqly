#!/bin/bash

isort --check ${@:-.}
black --check ${@:-.}
flake8 ${@:-.}
