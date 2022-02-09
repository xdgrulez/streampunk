#!/bin/bash
docker build -t streampunk:0.0.1 -f scripts/Dockerfile .

az acr login -n <ACR name>

#az acr list

docker tag streampunk:0.0.1 <ACR name>.azurecr.io/streampunk:0.0.1

docker push <ACR name>.azurecr.io/streampunk:0.0.1

#az acr repository list -n <ACR name>
