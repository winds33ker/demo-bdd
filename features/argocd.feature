Feature: AWS Services Validation

  Scenario Outline: Validate AWS services in different regions
    Given I fetch AWS services for region "us-east-1"
    When I filter for "eks" services
    Then I should find resources for each service
    And all services in "eks" should be available

    Examples:
      | region         | services           |
      | us-east-1      | eks |

  Scenario: Validate ArgoCD version is v3.3.0
    Given I have access to EKS cluster "eks-dev-cluster-3" in region "us-east-1" for services validation
    When I check the ArgoCD controller deployment
    Then I should validate that ArgoCD version is "v3.3.0"
    And I should verify ArgoCD controller is running and healthy