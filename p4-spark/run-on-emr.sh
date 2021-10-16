#!/bin/bash

# shellcheck source=/dev/null
source .env
export AWS_PROFILE=$MY_AWS_PROFILE_NAME

aws s3api create-bucket \
--bucket p4-spark \
--region "$REGION"

aws s3 cp etl.py s3://p4-spark/

aws emr create-cluster \
--applications Name=Hadoop Name=Spark \
--ec2-attributes '{
    "InstanceProfile": "EMR_EC2_DefaultRole",
    "SubnetId": "'"$SUBNET_ID"'"
}' \
--release-label emr-5.33.1 \
--log-uri 's3n://p4-spark/emr-logs/' \
--steps '[
    {
        "Args": [
            "spark-submit",
            "--deploy-mode",
            "cluster",
            "s3://p4-spark/etl.py"
        ],
        "Type": "CUSTOM_JAR",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "Jar": "command-runner.jar",
        "Properties": "",
        "Name": "p4-spark"
    }
]' \
--instance-count 3 \
--configurations '[
    {
        "Classification": "spark",
        "Properties": {}
    }
]' \
--auto-terminate \
--service-role EMR_DefaultRole \
--enable-debugging \
--name 'p4-spark-cluster' \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--region "$REGION"