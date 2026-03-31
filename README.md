# BDD Test Framework for EKS Platform

Comprehensive BDD (Behavior-Driven Development) test suite for validating EKS cluster configurations, Karpenter functionality, AWS services, and spot interruption resilience.

## Overview

This test framework provides automated validation for:
- **Karpenter Security**: Deployment validation, CRD placement, IRSA permissions
- **Spot Interruption Testing**: AWS FIS-based real spot interruption simulation
- **AWS Services Validation**: Service availability across regions
- **Event Monitoring**: Comprehensive timing analysis and event capture

## Directory Structure

```
bdd-test/
├── features/                    # Gherkin feature files
│   ├── karpenter_security.feature
│   ├── spot_interruption.feature
│   └── aws_services.feature
├── steps/                       # Step implementations
│   ├── karpenter_steps.py      # Security validation steps
│   ├── spot_interruption_steps.py  # FIS spot testing steps
│   └── services_steps.py       # AWS services validation steps
├── py27/                        # Python 2.7 compatible versions
│   └── karpenter_steps_py27.py
├── results/                     # Test results and logs
└── behave.ini                   # Behave configuration
```

## Prerequisites

### Required Tools
- Python 3.7+ (or Python 2.7 for legacy support)
- behave framework: `pip install behave`
- boto3: `pip install boto3`
- kubectl configured for your EKS cluster
- AWS CLI configured with appropriate permissions

### AWS Permissions Required
- EKS cluster access
- IAM policy read permissions
- FIS experiment permissions (for spot testing)
- EventBridge and SQS access (for event monitoring)
- EC2 instance management (for spot interruption)

### FIS Setup (for Spot Interruption Tests)
Create FIS IAM role with permissions:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "ec2:StopInstances",
                "ec2:StartInstances",
                "ec2:DescribeInstances"
            ],
            "Resource": "*"
        }
    ]
}
```

## Running Tests

### Basic Usage

```bash
# Run all tests
behave

# Run specific feature
behave features/karpenter_security.feature
behave features/spot_interruption.feature
behave features/aws_services.feature

# Run with custom parameters
behave -D cluster_name=my-cluster -D region=us-west-2 -D namespace=my-app
```

### Test Categories

#### 1. Karpenter Security Validation
```bash
# Test Karpenter deployment and security configuration
behave features/karpenter_security.feature

# Parameters:
# - cluster_name: EKS cluster name
# - region: AWS region
```

**Validates:**
- Karpenter deployment status
- Controller pod health
- CRD placement (not in default namespace)
- IRSA permissions (no wildcard permissions)

#### 2. Spot Interruption Testing
```bash
# Test spot interruption resilience with AWS FIS
behave features/spot_interruption.feature

# Parameters:
# - cluster_name: EKS cluster name
# - region: AWS region
# - namespace: Application namespace
```

**Features:**
- Real AWS FIS spot interruption simulation
- EventBridge and SQS event monitoring
- Karpenter response timing analysis
- Pod lifecycle event capture
- Complete recovery time measurement

#### 3. AWS Services Validation
```bash
# Validate AWS service availability
behave features/aws_services.feature

# Parameters:
# - region: AWS region to validate
# - services: Comma-separated list of services
```

### Advanced Usage

#### Parameterized Testing
```bash
# Test multiple clusters
behave -D cluster_name=cluster-1 features/spot_interruption.feature
behave -D cluster_name=cluster-2 features/spot_interruption.feature

# Test different regions
behave -D region=us-east-1 features/aws_services.feature
behave -D region=us-west-2 features/aws_services.feature

# Test specific services
behave -D services="eks,ec2,iam" features/aws_services.feature
```

#### Python 2.7 Compatibility
```bash
# Use Python 2.7 compatible steps
cp py27/karpenter_steps_py27.py steps/karpenter_steps_py27.py
# Update feature files to use py27 steps if needed
```

## Test Results

### Log Files
- `results/karpenter_security_test_results.log` - Security validation results
- `results/spot_interruption_test_results.log` - Spot interruption test results  
- `results/aws_services_test_results.log` - AWS services validation results

### Result Format
```
[2024-01-15 10:30:45] Test Scenario Name
  → Detailed test step result with timing information
  → ✓ Success indicators
  → ⚠ Warning indicators  
  → 🔴 Error indicators with timing analysis
```

## Feature Examples

### Karpenter Security Feature
```gherkin
Feature: Karpenter Security Configuration Validation
  Scenario: Validate Karpenter deployment security
    Given I have access to EKS cluster "my-cluster" in region "us-west-2" for security validation
    When I check the Karpenter deployment
    Then Karpenter should be deployed and running
    And Karpenter controller pods should be in running state
    When I check Karpenter Custom Resource Definitions
    Then EC2NodeClasses should not be in default namespace
    And NodePools should not be in default namespace
    When I check Karpenter IRSA permissions
    Then Karpenter IRSA should not have wildcard permissions
```

### Spot Interruption Feature
```gherkin
Feature: AWS FIS Spot Interruption Testing
  Scenario: Test spot interruption with complete recovery measurement
    Given I have access to EKS cluster "my-cluster" in region "us-west-2"
    And I set the namespace to "math-compute-sqs-app"
    When I check the math-compute-sqs-app deployment in namespace "math-compute-sqs-app"
    Then the application should be running on a spot instance
    When I create AWS FIS experiment to simulate spot interruption
    Then I should capture EventBridge and SQS events
    And I should capture Karpenter and pod timing events
    And I should measure complete recovery time
    And the recovery should complete within acceptable time limits
```

## Timing Analysis Features

### Spot Interruption Timing
- **Signal Detection**: SIGTERM/SIGKILL timing capture
- **PreStop Hooks**: Graceful shutdown timing
- **Karpenter Response**: Node replacement timing
- **Recovery Measurement**: End-to-end recovery time
- **Event Timeline**: Chronological event analysis

### Performance Thresholds
- Karpenter detection: < 30 seconds
- Node provisioning: < 5 minutes  
- Complete recovery: < 10 minutes
- Pod rescheduling: < 5 minutes

## Troubleshooting

### Common Issues

**1. FIS Permission Errors**
```bash
# Ensure FIS role exists and has proper permissions
aws iam get-role --role-name AWSFISIAMRole-1760016814597
```

**2. Kubectl Access Issues**
```bash
# Update kubeconfig
aws eks update-kubeconfig --region us-west-2 --name my-cluster
```

**3. Missing Dependencies**
```bash
# Install required packages
pip install behave boto3
```

**4. Test Failures**
- Check log files in `results/` directory
- Verify AWS permissions
- Ensure cluster and applications are running
- Check network connectivity

### Debug Mode
```bash
# Run with verbose output
behave --verbose features/spot_interruption.feature

# Run specific scenario
behave --name "Test spot interruption" features/spot_interruption.feature
```

## Best Practices

### Test Environment
- Use dedicated test clusters for spot interruption testing
- Ensure applications have proper graceful shutdown handling
- Configure appropriate resource limits and requests
- Set up monitoring and alerting for test environments

### Security
- Use least-privilege IAM policies
- Rotate AWS credentials regularly
- Avoid hardcoding sensitive information
- Use temporary credentials when possible

### Maintenance
- Regularly update test scenarios
- Review and update timing thresholds
- Clean up FIS experiments after testing
- Monitor test execution times and optimize as needed

## Integration

### CI/CD Pipeline Integration
```yaml
# Example GitHub Actions workflow
- name: Run BDD Tests
  run: |
    behave -D cluster_name=${{ env.CLUSTER_NAME }} \
           -D region=${{ env.AWS_REGION }} \
           features/karpenter_security.feature
```

### Monitoring Integration
- Export test results to monitoring systems
- Set up alerts for test failures
- Track test execution trends
- Monitor recovery time metrics