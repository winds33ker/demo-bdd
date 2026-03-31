import boto3
import subprocess
import json
from datetime import datetime
from behave import given, when, then

def log_to_file(scenario_name, message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("results/aws_services_test_results.log", "a") as f:
        f.write(f"[{timestamp}] {scenario_name}\n")
        f.write(f"  → {message}\n")

def log_feature_start(feature_name):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("results/aws_services_test_results.log", "w") as f:
        f.write(f"{'='*80}\n")
        f.write(f"FEATURE: {feature_name}\n")
        f.write(f"Started: {timestamp}\n")
        f.write(f"{'='*80}\n\n")

def log_scenario_start(scenario_name):
    with open("results/aws_services_test_results.log", "a") as f:
        f.write(f"\n{'-'*60}\n")
        f.write(f"SCENARIO: {scenario_name}\n")
        f.write(f"{'-'*60}\n")

@given('I fetch AWS services for region "{region}"')
def step_fetch_aws_services(context, region):
    actual_region = context.config.userdata.get('region', region)
    context.region = actual_region
    context.scenario_name = context.scenario.name
    
    if not hasattr(context, 'services_feature_logged'):
        log_feature_start("AWS Services Validation")
        context.services_feature_logged = True
    
    log_scenario_start(context.scenario_name)
    log_to_file(context.scenario_name, f"Validating AWS services availability in region {actual_region}")
    
    try:
        resource_explorer = boto3.client('resource-explorer-2', region_name=actual_region)
        
        response = resource_explorer.list_supported_resource_types(MaxResults=1000)
        
        context.supported_resource_types = response.get('ResourceTypes', [])
        context.available_services = set()
        
        for resource_type in context.supported_resource_types:
            service = resource_type.get('Service', '')
            if service:
                context.available_services.add(service.lower())
        
        log_to_file(context.scenario_name, f"Found {len(context.available_services)} available AWS services in region {actual_region}")
        
    except Exception as e:
        log_to_file(context.scenario_name, f"Error fetching AWS services: {str(e)}")
        raise

@when('I filter for "{services}" services')
def step_filter_services(context, services):
    services_to_check = context.config.userdata.get('services', services)
    context.requested_services = [s.strip().lower() for s in services_to_check.split(',')]
    
    log_to_file(context.scenario_name, f"Checking services: {context.requested_services}")
    
    context.found_services = []
    context.missing_services = []
    
    for service in context.requested_services:
        if service in context.available_services:
            context.found_services.append(service)
        else:
            context.missing_services.append(service)
    
    log_to_file(context.scenario_name, f"Found {len(context.found_services)} services available")

@then('I should find resources for each service')
def step_validate_resources_found(context):
    assert len(context.found_services) > 0, "No services found"
    log_to_file(context.scenario_name, f"Validation passed: {len(context.found_services)} services available")

@then('all services in "{services}" should be available')
def step_validate_all_services(context, services):
    if context.missing_services:
        log_to_file(context.scenario_name, f"Missing services: {context.missing_services}")
        assert False, f"Services not available in region {context.region}: {context.missing_services}"
    
    for service in context.found_services:
        log_to_file(context.scenario_name, f"✓ Service '{service}' is available")
    
    log_to_file(context.scenario_name, f"All {len(context.found_services)} services validated successfully")

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
        subprocess.run([
            "aws", "eks", "update-kubeconfig", 
            "--region", region, 
            "--name", cluster_name
        ], check=True, capture_output=True)
        
        log_to_file(context.scenario_name, f"Connected to EKS cluster {cluster_name} in region {region}")
        
    except subprocess.CalledProcessError as e:
        log_to_file(context.scenario_name, f"Failed to connect to EKS cluster: {str(e)}")
        raise

@when('I check the Karpenter controller deployment')
def step_check_karpenter_deployment(context):
    """Check Karpenter controller deployment"""
    try:
        log_to_file(context.scenario_name, "Checking Karpenter controller deployment...")
        
        # Get Karpenter deployment
        cmd = "kubectl get deployment karpenter -n karpenter -o json"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise Exception(f"Failed to get Karpenter deployment: {result.stderr}")
        
        deployment_data = json.loads(result.stdout)
        context.karpenter_deployment = deployment_data
        
        # Extract deployment info
        deployment_name = deployment_data['metadata']['name']
        namespace = deployment_data['metadata']['namespace']
        replicas = deployment_data['status'].get('replicas', 0)
        ready_replicas = deployment_data['status'].get('readyReplicas', 0)
        
        log_to_file(context.scenario_name, f"Found Karpenter deployment: {deployment_name}")
        log_to_file(context.scenario_name, f"Namespace: {namespace}")
        log_to_file(context.scenario_name, f"Replicas: {ready_replicas}/{replicas}")
        
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
        log_to_file(context.scenario_name, f"Karpenter image: {context.karpenter_image}")
        
    except Exception as e:
        log_to_file(context.scenario_name, f"Error checking Karpenter deployment: {str(e)}")
        raise

@then('I should validate that Karpenter version is "{expected_version}"')
def step_validate_karpenter_version(context, expected_version):
    """Validate Karpenter version"""
    try:
        log_to_file(context.scenario_name, f"Validating Karpenter version is {expected_version}...")
        
        # Extract version from image tag
        image = context.karpenter_image
        
        # Handle different image formats
        if ':' in image:
            # Format: public.ecr.aws/karpenter/karpenter:1.5.0
            version_tag = image.split(':')[1]
        else:
            # Handle cases where version might be in a different format
            raise Exception(f"Cannot extract version from image: {image}")
        
        # Clean up version tag (remove any prefixes like 'v')
        actual_version = version_tag.split('@')[0]
        
        log_to_file(context.scenario_name, f"Expected version: {expected_version}")
        log_to_file(context.scenario_name, f"Actual version: {actual_version}")
        
        if actual_version == expected_version:
            log_to_file(context.scenario_name, f"✓ Karpenter version validation passed: {actual_version}")
            context.karpenter_version_valid = True
        else:
            log_to_file(context.scenario_name, f"✗ Karpenter version mismatch: expected {expected_version}, got {actual_version}")
            context.karpenter_version_valid = False
            assert False, f"Karpenter version mismatch: expected {expected_version}, got {actual_version}"
            
    except Exception as e:
        log_to_file(context.scenario_name, f"Error validating Karpenter version: {str(e)}")
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
        
        log_to_file(context.scenario_name, f"Deployment status:")
        log_to_file(context.scenario_name, f"  Total replicas: {replicas}")
        log_to_file(context.scenario_name, f"  Ready replicas: {ready_replicas}")
        log_to_file(context.scenario_name, f"  Available replicas: {available_replicas}")
        
        # Verify all replicas are ready and available
        if replicas > 0 and ready_replicas == replicas and available_replicas == replicas:
            log_to_file(context.scenario_name, "✓ Karpenter controller is running and healthy")
            context.karpenter_healthy = True
        else:
            log_to_file(context.scenario_name, "✗ Karpenter controller is not fully healthy")
            context.karpenter_healthy = False
            assert False, f"Karpenter controller not healthy: {ready_replicas}/{replicas} replicas ready"
        
        # Additional health check - get pod status
        cmd = "kubectl get pods -n karpenter -l app.kubernetes.io/name=karpenter -o json"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            pods_data = json.loads(result.stdout)
            pods = pods_data.get('items', [])
            
            running_pods = 0
            for pod in pods:
                pod_name = pod['metadata']['name']
                pod_phase = pod['status'].get('phase', 'Unknown')
                
                log_to_file(context.scenario_name, f"  Pod {pod_name}: {pod_phase}")
                
                if pod_phase == 'Running':
                    # Check container readiness
                    container_statuses = pod['status'].get('containerStatuses', [])
                    all_ready = all(status.get('ready', False) for status in container_statuses)
                    
                    if all_ready:
                        running_pods += 1
                        log_to_file(context.scenario_name, f"    ✓ All containers ready")
                    else:
                        log_to_file(context.scenario_name, f"    ⚠ Some containers not ready")
            
            log_to_file(context.scenario_name, f"Running and ready pods: {running_pods}/{len(pods)}")
            
            if running_pods == len(pods) and running_pods > 0:
                log_to_file(context.scenario_name, "✓ All Karpenter pods are running and ready")
            else:
                log_to_file(context.scenario_name, "⚠ Not all Karpenter pods are running and ready")
        
    except Exception as e:
        log_to_file(context.scenario_name, f"Error verifying Karpenter health: {str(e)}")
        context.karpenter_healthy = False
        raise
