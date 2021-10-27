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
    echo "create           Start up and expose the database!"
    echo "start            Run an instance you have stopped"
    echo "stop             Spin down the database"
    echo "destroy          Delete the DB container"
    echo "prompt           Start an interactive psql prompt in the container"
    echo "execute          Execute a sql file - must be in sql/ dir"
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
    --volume "$PWD"/sql:/sql \
    postgres

    sleep 3
    docker exec -it fake-redshift psql -U postgres -c "CREATE DATABASE dev;"
    ;;
start)
    docker start fake-redshift
    ;;
stop)
    docker stop fake-redshift
    ;;
destroy)
    docker rm -f fake-redshift
    ;;
prompt)
    docker exec -it fake-redshift \
        psql \
        -U postgres \
        -d dev
    ;;
execute)
    docker exec -it fake-redshift \
        psql \
        -U postgres \
        -d dev \
        -f "$2"
    ;;
help)
    display_help
    ;;
*)
    echo "No command specified, displaying help"
    display_help
    ;;

esac
