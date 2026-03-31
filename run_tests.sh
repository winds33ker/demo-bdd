#!/bin/bash

# Run BDD tests with runtime parameters

CLUSTER_NAME=${1:-"eks-karpenter-upgrade"}
REGION=${2:-"us-west-2"}

echo "Running BDD tests with parameters:"
echo "  Cluster: $CLUSTER_NAME"
echo "  Region: $REGION"
echo "  Services: $SERVICES"
echo ""

# Run tests with parameters
behave \
  -D cluster_name="$CLUSTER_NAME" \
  -D region="$REGION" \
  features/

# Examples:
# ./run_tests.sh my-cluster us-east-1 "sqs,s3,lambda"
# behave -D services="ec2,vpc,iam" features/services.feature