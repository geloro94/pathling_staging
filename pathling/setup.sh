#!/usr/bin/bash

echo "Setting up Pathling FHIR server"
docker compose --project-name pathling-data-extraction up -d --wait
echo "Done"