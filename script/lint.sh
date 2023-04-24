#!/bin/bash

black --check ${@:-.}
flake8 ${@:-.}