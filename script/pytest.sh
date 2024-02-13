#!/bin/bash
set -eu

psql $POSTGRESQL_URL -c "drop database testapp" || true
psql $POSTGRESQL_URL -c "create database testapp" || true

pytest $@ || true

rm -f $(dirname $(dirname $0))/testapp/migrations/*