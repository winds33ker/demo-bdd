Feature: Comprehensive Spot Interruption Lifecycle Testing
  As a platform engineer
  I want to test the complete spot interruption lifecycle with all components
  So that I can validate every step from AWS event to pod recovery

  Background:
    Given I have access to EKS cluster "eks-karpenter-upgrade" in region "us-west-2" for spot interruption testing
    And I set the namespace to "math-compute-sqs-app"
    And I configure SQS queue "karpenter-eks-karpenter-upgrade" for spot interruption monitoring

  Scenario: Complete spot interruption lifecycle test
    # Setup monitoring and trigger interruption
    Given the math-compute-sqs-app is running on a spot instance in namespace "math-compute-sqs-app"
    And I capture the initial pod and node state
    And I start comprehensive monitoring for all components
    When I trigger AWS FIS spot interruption with 2-minute grace period
    
    # Comprehensive validation and analysis
    Then I should validate the complete spot interruption lifecycle
    And I should analyze timing delays and calculate total processing time
    And I should cleanup test resources