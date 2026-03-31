import boto3
import subprocess
import json
from datetime import datetime
from behave import given, when, then

def log_to_file(scenario_name, message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("results/aws_services_test_results.log", "a") as f:
        f.write("[{0}] {1}\n".format(timestamp, scenario_name))
        f.write("  → {0}\n".format(message))

def log_feature_start(feature_name):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("results/aws_services_test_results.log", "w") as f:
        f.write("{0}\n".format("=" * 80))
        f.write("FEATURE: {0}\n".format(feature_name))
        f.write("Started: {0}\n".format(timestamp))
        f.write("{0}\n\n".format("=" * 80))

def log_scenario_start(scenario_name):
    with open("results/aws_services_test_results.log", "a") as f:
        f.write("\n{0}\n".format("-" * 60))
        f.write("SCENARIO: {0}\n".format(scenario_name))
        f.write("{0}\n".format("-" * 60))

@given('I fetch AWS services for region "{region}"')
def step_fetch_aws_services(context, region):
    actual_region = context.config.userdata.get('region', region)
    context.region = actual_region
    context.scenario_name = context.scenario.name
    
    if not hasattr(context, 'services_feature_logged'):
        log_feature_start("AWS Services Validation")
        context.services_feature_logged = True
    
    log_scenario_start(context.scenario_name)
    log_to_file(context.scenario_name, "Validating AWS services availability in region {0}".format(actual_region))
    
    try:
        # Use service-specific API calls instead of resource-explorer-2
        context.available_services = set()
        
        # Test each service individually
        service_tests = {
            'sqs': lambda: boto3.client('sqs', region_name=actual_region).list_queues(),
            'events': lambda: boto3.client('events', region_name=actual_region).list_rules(Limit=1),
            'kms': lambda: boto3.client('kms', region_name=actual_region).list_keys(Limit=1),
            'eks': lambda: boto3.client('eks', region_name=actual_region).list_clusters()
        }
        
        for service_name, test_func in service_tests.items():
            try:
                test_func()
                context.available_services.add(service_name)
                log_to_file(context.scenario_name, "✓ Service '{0}' is available".format(service_name))
            except Exception as e:
                log_to_file(context.scenario_name, "✗ Service '{0}' not available: {1}".format(service_name, str(e)))
        
        log_to_file(context.scenario_name, "Found {0} available AWS services in region {1}".format(
            len(context.available_services), actual_region))
        
    except Exception as e:
        log_to_file(context.scenario_name, "Error checking AWS services: {0}".format(str(e)))
        raise

@when('I filter for "{services}" services')
def step_filter_services(context, services):
    services_to_check = context.config.userdata.get('services', services)
    context.requested_services = [s.strip().lower() for s in services_to_check.split(',')]
    
    log_to_file(context.scenario_name, "Checking services: {0}".format(context.requested_services))
    
    context.found_services = []
    context.missing_services = []
    
    for service in context.requested_services:
        if service in context.available_services:
            context.found_services.append(service)
        else:
            context.missing_services.append(service)
    
    log_to_file(context.scenario_name, "Found {0} services available".format(len(context.found_services)))

@then('I should find resources for each service')
def step_validate_resources_found(context):
    assert len(context.found_services) > 0, "No services found"
    log_to_file(context.scenario_name, "Validation passed: {0} services available".format(len(context.found_services)))

@then('all services in "{services}" should be available')
def step_validate_all_services(context, services):
    if context.missing_services:
        log_to_file(context.scenario_name, "Missing services: {0}".format(context.missing_services))
        assert False, "Services not available in region {0}: {1}".format(context.region, context.missing_services)
    
    for service in context.found_services:
        log_to_file(context.scenario_name, "✓ Service '{0}' is available".format(service))
    
    log_to_file(context.scenario_name, "All {0} services validated successfully".format(len(context.found_services)))

# Karpenter version validation steps
@given('I have access to EKS cluster "{cluster_name}" in region "{region}" for services validation')
def step_connect_to_eks_cluster(context, cluster_name, region):
    """Connect to EKS cluster for Karpenter validation"""
    context.cluster_name = cluster_name
    context.region = region
    context.scenario_name = context.scenario.name
    
    if not hasattr(context, 'services_feature_logged'):
        log_feature_start("AWS Services Validation")
        context.services_feature_logged = True
    
    log_scenario_start(context.scenario_name)
    
    # Update kubeconfig
    try:
        cmd = ["aws", "eks", "update-kubeconfig", "--region", region, "--name", cluster_name]
        subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        
        log_to_file(context.scenario_name, "Connected to EKS cluster {0} in region {1}".format(cluster_name, region))
        
    except subprocess.CalledProcessError as e:
        log_to_file(context.scenario_name, "Failed to connect to EKS cluster: {0}".format(str(e)))
        raise

@when('I check the Karpenter controller deployment')
def step_check_karpenter_deployment(context):
    """Check Karpenter controller deployment"""
    try:
        log_to_file(context.scenario_name, "Checking Karpenter controller deployment...")
        
        # Get Karpenter deployment
        cmd = "kubectl get deployment karpenter -n karpenter -o json"
        try:
            result = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            raise Exception("Failed to get Karpenter deployment: {0}".format(str(e)))
        
        deployment_data = json.loads(result)
        context.karpenter_deployment = deployment_data
        
        # Extract deployment info
        deployment_name = deployment_data['metadata']['name']
        namespace = deployment_data['metadata']['namespace']
        replicas = deployment_data['status'].get('replicas', 0)
        ready_replicas = deployment_data['status'].get('readyReplicas', 0)
        
        log_to_file(context.scenario_name, "Found Karpenter deployment: {0}".format(deployment_name))
        log_to_file(context.scenario_name, "Namespace: {0}".format(namespace))
        log_to_file(context.scenario_name, "Replicas: {0}/{1}".format(ready_replicas, replicas))
        
        # Get container image to extract version
        containers = deployment_data['spec']['template']['spec']['containers']
        karpenter_container = None
        
        for container in containers:
            if container['name'] == 'controller':
                karpenter_container = container
                break
        
        if not karpenter_container:
            raise Exception("Karpenter controller container not found in deployment")
        
        context.karpenter_image = karpenter_container['image']
        log_to_file(context.scenario_name, "Karpenter image: {0}".format(context.karpenter_image))
        
    except Exception as e:
        log_to_file(context.scenario_name, "Error checking Karpenter deployment: {0}".format(str(e)))
        raise

@then('I should validate that Karpenter version is "{expected_version}"')
def step_validate_karpenter_version(context, expected_version):
    """Validate Karpenter version"""
    try:
        log_to_file(context.scenario_name, "Validating Karpenter version is {0}...".format(expected_version))
        
        # Extract version from image tag
        image = context.karpenter_image
        
        # Handle different image formats
        if ':' in image:
            # Format: public.ecr.aws/karpenter/karpenter:1.5.0
            version_tag = image.split(':')[-1]
        else:
            # Handle cases where version might be in a different format
            raise Exception("Cannot extract version from image: {0}".format(image))
        
        # Clean up version tag (remove any prefixes like 'v' and handle digest)
        actual_version = version_tag.split('@')[0]
        if actual_version.startswith('v'):
            actual_version = actual_version[1:]
        
        log_to_file(context.scenario_name, "Expected version: {0}".format(expected_version))
        log_to_file(context.scenario_name, "Actual version: {0}".format(actual_version))
        
        if actual_version == expected_version:
            log_to_file(context.scenario_name, "✓ Karpenter version validation passed: {0}".format(actual_version))
            context.karpenter_version_valid = True
        else:
            log_to_file(context.scenario_name, "✗ Karpenter version mismatch: expected {0}, got {1}".format(
                expected_version, actual_version))
            context.karpenter_version_valid = False
            assert False, "Karpenter version mismatch: expected {0}, got {1}".format(expected_version, actual_version)
            
    except Exception as e:
        log_to_file(context.scenario_name, "Error validating Karpenter version: {0}".format(str(e)))
        context.karpenter_version_valid = False
        raise

@then('I should verify Karpenter controller is running and healthy')
def step_verify_karpenter_health(context):
    """Verify Karpenter controller is running and healthy"""
    try:
        log_to_file(context.scenario_name, "Verifying Karpenter controller health...")
        
        deployment = context.karpenter_deployment
        
        # Check deployment status
        replicas = deployment['status'].get('replicas', 0)
        ready_replicas = deployment['status'].get('readyReplicas', 0)
        available_replicas = deployment['status'].get('availableReplicas', 0)
        
        log_to_file(context.scenario_name, "Deployment status:")
        log_to_file(context.scenario_name, "  Total replicas: {0}".format(replicas))
        log_to_file(context.scenario_name, "  Ready replicas: {0}".format(ready_replicas))
        log_to_file(context.scenario_name, "  Available replicas: {0}".format(available_replicas))
        
        # Verify all replicas are ready and available
        if replicas > 0 and ready_replicas == replicas and available_replicas == replicas:
            log_to_file(context.scenario_name, "✓ Karpenter controller is running and healthy")
            context.karpenter_healthy = True
        else:
            log_to_file(context.scenario_name, "✗ Karpenter controller is not fully healthy")
            context.karpenter_healthy = False
            assert False, "Karpenter controller not healthy: {0}/{1} replicas ready".format(ready_replicas, replicas)
        
        # Additional health check - get pod status
        cmd = "kubectl get pods -n karpenter -l app.kubernetes.io/name=karpenter -o json"
        try:
            result = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
            
            pods_data = json.loads(result)
            pods = pods_data.get('items', [])
            
            running_pods = 0
            for pod in pods:
                pod_name = pod['metadata']['name']
                pod_phase = pod['status'].get('phase', 'Unknown')
                
                log_to_file(context.scenario_name, "  Pod {0}: {1}".format(pod_name, pod_phase))
                
                if pod_phase == 'Running':
                    # Check container readiness
                    container_statuses = pod['status'].get('containerStatuses', [])
                    all_ready = all(status.get('ready', False) for status in container_statuses)
                    
                    if all_ready:
                        running_pods += 1
                        log_to_file(context.scenario_name, "    ✓ All containers ready")
                    else:
                        log_to_file(context.scenario_name, "    ⚠ Some containers not ready")
            
            log_to_file(context.scenario_name, "Running and ready pods: {0}/{1}".format(running_pods, len(pods)))
            
            if running_pods == len(pods) and running_pods > 0:
                log_to_file(context.scenario_name, "✓ All Karpenter pods are running and ready")
            else:
                log_to_file(context.scenario_name, "⚠ Not all Karpenter pods are running and ready")
                
        except subprocess.CalledProcessError:
            log_to_file(context.scenario_name, "⚠ Could not retrieve pod status")
        
    except Exception as e:
        log_to_file(context.scenario_name, "Error verifying Karpenter health: {0}".format(str(e)))
        context.karpenter_healthy = False
        raise