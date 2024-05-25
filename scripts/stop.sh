#!/bin/bash

echo "Stopping Docker Compose"
docker compose --env-file ./config/.env stop

echo "Removing Containers"
docker compose --env-file ./config/.env rm -f
