#!/bin/bash

isort ${@:-.}
black ${@:-.}