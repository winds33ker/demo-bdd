Feature: AWS Services Validation

  Scenario Outline: Validate AWS services in different regions
    Given I fetch AWS services for region "<region>"
    When I filter for "<services>" services
    Then I should find resources for each service
    And all services in "<services>" should be available

    Examples:
      | region         | services           |
      | us-east-1      | sqs,events,kms,eks |
      | us-east-2      | sqs,events,kms,eks |
      | us-west-2      | sqs,events,kms,eks |
      | eu-central-1   | sqs,events,kms,eks |
      | eu-west-1      | sqs,events,kms,eks |
      | eu-west-2      | sqs,events,kms,eks |
      | ap-south-1     | sqs,events,kms,eks |
      | ap-northeast-1 | sqs,events,kms,eks |

  Scenario: Validate Karpenter version is 1.5.0
    Given I have access to EKS cluster "eks-karpenter-upgrade" in region "us-west-2" for services validation
    When I check the Karpenter controller deployment
    Then I should validate that Karpenter version is "1.5.0"
    And I should verify Karpenter controller is running and healthy