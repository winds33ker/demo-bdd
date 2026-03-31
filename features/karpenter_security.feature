Feature: Karpenter Security Configuration Validation
  As a DevOps engineer
  I want to validate Karpenter security configurations
  So that the EKS cluster meets security requirements

  Background:
    Given I have access to EKS cluster "eks-karpenter-upgrade" in region "us-west-2" for security validation

  Scenario: Validate Karpenter deployment in EKS cluster
    When I check the Karpenter deployment
    Then Karpenter should be deployed and running
    And Karpenter controller pods should be in running state

  Scenario: Validate Karpenter CRDs are not in default namespace
    When I check Karpenter Custom Resource Definitions
    Then EC2NodeClasses should not be in default namespace
    And NodePools should not be in default namespace

  Scenario: Validate Karpenter IRSA does not have wildcard permissions
    When I check Karpenter IRSA permissions
    Then the IAM role should not have wildcard permissions
    And permissions should follow least privilege principle

  Scenario: Validate Karpenter controller is not using any secrets
    When I check Karpenter controller configuration
    Then the controller should not be using any Kubernetes secrets
    And no secret volumes should be mounted

  Scenario: Validate SQS used for Karpenter interruption is encrypted using SSE KMS key
    When I check the SQS queue configuration for Karpenter
    Then the SQS queue should be encrypted with KMS
    And encryption should use customer managed KMS key

  Scenario: Validate communication between Karpenter and SQS is secure
    When I check the SQS queue configuration for Karpenter
    Then all communication should use HTTPS/TLS

  Scenario: Validate SQS queue has least privilege access policy for EventBridge
    When I check SQS queue access policy
    Then EventBridge should have minimal required permissions
    And no overly permissive policies should exist

  Scenario: Validate EventBridge rule listens to spot interruption events and sends to SQS
    When I simulate a spot interruption warning event
    Then EventBridge rule should capture the event
    And the event should be forwarded to the correct SQS queue

  Scenario: Validate EKS cluster is deployed in private VPC without internet access
    When I check the EKS cluster VPC configuration
    Then the EKS cluster should be in a private VPC
    And the VPC should not have an internet gateway attached
    And the VPC should not have NAT gateways for internet access
    And all EKS worker node subnets should be private subnets
    And the cluster endpoint should be private or restricted