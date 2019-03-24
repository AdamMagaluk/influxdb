#!/usr/bin/env bash
#
# script to start influxdb and compile influxdb-java with all tests.
#
set -e

DEFAULT_INFLUXDB_VERSION="1.7"

INFLUXDB_VERSION="${INFLUXDB_VERSION:-$DEFAULT_INFLUXDB_VERSION}"

echo "Run tests with influxdb-${INFLUXDB_VERSION}"
docker kill influxdb || true
docker rm influxdb || true
docker pull influxdb:${INFLUXDB_VERSION}-alpine || true
docker run \
       --detach \
       --name influxdb \
       --publish 8086:8086 \
       --publish 8089:8089/udp \
       --volume ${PWD}/influxdb.conf:/etc/influxdb/influxdb.conf \
       influxdb:${INFLUXDB_VERSION}-alpine

