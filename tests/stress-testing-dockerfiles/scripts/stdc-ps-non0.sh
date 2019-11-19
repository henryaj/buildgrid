#!/bin/bash
set -u
docker-compose -f $STDC ps | grep -v "Exit 0"
