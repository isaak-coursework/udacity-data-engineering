#!/bin/bash

IMAGE_TAG=glue-2.0
CONTAINER_NAME=glue-udacity

display_help() {
   # Display Help
   echo "======================================"
   echo "   Glue Local Dev Env"
   echo "======================================"
   echo "Syntax: ./glue-local.sh [command]"
   echo
   echo "---commands---"
   echo "help                   Print CLI help"
   echo "build                  Build Image Locally"
   echo "                  "
   echo "bash                   Open up a bash terminal inside container"
   echo
}

run_with_entrypoint() {
   source .env && docker run --rm -it \
      --volume "$PWD"/jobs:/root/aws-glue-libs/jobs \
      --env AWS_ACCESS_KEY_ID \
      --env AWS_SECRET_ACCESS_KEY \
      --env AWS_REGION="us-east-1" \
      --name $CONTAINER_NAME \
      --entrypoint "$1" \
      $IMAGE_TAG
}

case "$1" in
build)
   docker build -t $IMAGE_TAG .
   ;;
pyspark)
    run_with_entrypoint "./bin/gluepyspark"
   ;;
bash)
    run_with_entrypoint "/bin/bash"
   ;;
help)
   display_help
   ;;
*)
   echo "No command specified, displaying help"
   display_help
   ;;
esac
