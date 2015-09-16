#!/bin/bash
#
# Remove generated artifacts
#

DIR="$( cd "$( dirname "$0" )" && pwd )"

echo "making sure the REST server was shutdown"
$DIR/rest-server-stop.sh

echo "removing .pyc files"
rm -f testcases/*.pyc

echo "Removing $DIR/target"
rm -rf $DIR/target