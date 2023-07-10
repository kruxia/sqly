#!/bin/bash

isort -q --check ${@:-.}
black -q --check ${@:-.}
flake8 ${@:-.}
