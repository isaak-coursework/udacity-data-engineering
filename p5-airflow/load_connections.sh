#!/bin/bash

set -euo pipefail

# shellcheck disable=1091
. dwh.env

# Update Connections

REDSHIFT_CONN_ID="redshift"

if ./airflow.sh connections get "$REDSHIFT_CONN_ID"; then
    ./airflow.sh connections delete "$REDSHIFT_CONN_ID"
fi

./airflow.sh connections add "$REDSHIFT_CONN_ID" \
    --conn-type "$DB_PROTOCOL" \
    --conn-login "$DB_USER" \
    --conn-password "$DB_PASSWORD" \
    --conn-host "$DB_HOST" \
    --conn-port "$DB_PORT" \
    --conn-schema "$DB_NAME" \
    --conn-extra '{"keepalives_idle": "60", "redshift": true}'


# Update Variables

REDSHIFT_ARN_VAR_NAME="redshift_arn"

if ./airflow.sh variables get "$REDSHIFT_ARN_VAR_NAME"; then
    ./airflow.sh variables delete "$REDSHIFT_ARN_VAR_NAME"
fi   

./airflow.sh variables set "$REDSHIFT_ARN_VAR_NAME" "$REDSHIFT_ARN"