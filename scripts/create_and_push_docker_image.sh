#!/bin/bash
docker build -t streampunk:0.0.1 -f scripts/Dockerfile .

az acr login -n nemoregistrysandbox

#az acr list

docker tag streampunk:0.0.1 nemoregistrysandbox.azurecr.io/streampunk:0.0.1

docker push nemoregistrysandbox.azurecr.io/streampunk:0.0.1

#az acr repository list -n nemoregistrysandbox
