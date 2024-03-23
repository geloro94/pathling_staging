#!/usr/bin/bash

keep_volumes=false
help=false

while getopts vh flag
do
    case "${flag}" in
        v) keep_volumes=true;;
        h) help=true;;
    esac
done

if [ "$help" = true ]; then
  echo "Usage: bash remove.sh [flags]"
  echo "Flags:"
  echo "  -v: Don't remove volumes after container removal"
  echo "  -h: Show this help message"
  exit 0
fi

volumes=" --volumes"
echo "Removing pathling server"
if [ "$keep_volumes" = true ]; then
  docker compose --project-name pathling-data-extraction down
else
  docker compose --project-name pathling-data-extraction down --volumes
fi
echo "Done"
