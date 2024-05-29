#!/bin/bash

set -e 
set -o pipefail

NAME_USER="lkmschulz2"
NAME_IMAGE="sparkbench"
VER_IMAGE="latest"

URI_IMAGE="${NAME_USER}/${NAME_IMAGE}:${VER_IMAGE}"

source "$HOME/.sdkman/bin/sdkman-init.sh"
sbt package

docker login

docker build --platform linux/amd64 -t ${URI_IMAGE} -f docker/Dockerfile .
docker push ${URI_IMAGE}
