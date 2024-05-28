#!/bin/bash

echo "Stopping Docker Compose"
docker compose stop

echo "Removing Containers"
docker compose rm -f

echo "Removing results"
rm Results/*.csv
