#!/bin/bash

export AWS_PROFILE=udacity

# Create EMR Cluster
aws emr create-cluster \
--name spark \
--use-default-roles \
--release-label emr-5.28.0 \
--instance-count 3 \
--applications Name=Spark  \
--ec2-attributes KeyName="$SPARK_SSH_KEY",SubnetId="$SUBNET_ID" \
--instance-type m5.xlarge \
--profile udacity \
--auto-terminate

# SSH into EMR cluster
aws emr ssh --cluster-id j-1WSPHLOMJ3OPI --key-pair-file ~/spark-cluster.pem