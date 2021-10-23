#!/bin/bash

# Fake a redshift instance (just a local postgres container!)

display_help() {
    # Display Help
    echo "=================================================="
    echo "  Fake Redshift! (It's just PostGres, sssshhhh)"
    echo "=================================================="
    echo "Syntax: ./glue-local.sh [command]"
    echo
    echo "---commands---"
    echo "help             Print CLI help"
    echo "get-image        Pull the latest PostGres image from DockerHub"
    echo "start            Run and expose the database!"
    echo "stop             Spin down the database"
    echo
}

case "$1" in

get-image)
    docker pull postgres
;;
create)
    docker run -d \
    -p 5432:5432 \
    --name fake-redshift \
    -e POSTGRES_PASSWORD=postgres \
    postgres
;;
start)
    docker start fake-redshift
    ;;
stop)
    docker stop fake-redshift
    ;;
help)
    display_help
;;
*)
    echo "No command specified, displaying help"
    display_help
;;

esac
