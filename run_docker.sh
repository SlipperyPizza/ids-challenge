#!/bin/bash

# Exit immediately if a command fails
set -e

IMAGE_NAME="data-analysis-app"

echo "--- Building the Docker image named '$IMAGE_NAME' ---"
docker build -t $IMAGE_NAME .

echo ""
echo "--- Running the container to execute the Python script ---"
# The '--rm' flag automatically removes the container after it finishes
docker run --rm $IMAGE_NAME

echo ""
echo "✅ Script has finished running inside the container."