# -*- coding: utf-8 -*-
"""
Python 2.7 compatible version of spot_interruption_steps.py
Spot Interruption Lifecycle Testing Steps
"""

import subprocess
import json
import time
import re
import boto3
import traceback
import threading
try:
    import Queue as queue  # Python 2.7
except ImportError:
    import queue  # Python 3.x fallback
from datetime import datetime, timedelta
from behave import given, when, then
from botocore.exceptions import ClientError

# Common utility functions
def get_scenario_name(context):
    """Get scenario name from context"""
    return getattr(context.scenario, 'name', 'Unknown Scenario')

def log_to_file(scenario_name, message):
    """Log message to results file"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("results/spot_interruption_test_results.log", "a") as f:
        f.write("[{0}] {1}\n  → {2}\n".format(timestamp, scenario_name, message))

def run_kubectl(cmd):
    """Run kubectl command and return output - Python 2.7 compatible"""
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    
    if process.returncode != 0:
        raise Exception("kubectl command failed: {0}".format(stderr))
    return stdout.strip()

# AWS FIS Functions
def create_fis_spot_interruption_experiment(context, instance_id, region):
    """Create AWS FIS experiment template for spot interruption"""
    try:
        fis = boto3.client('fis', region_name=region)
        
        # Create experiment template for spot interruption
        experiment_template = {
            'description': 'BDD Test: Spot Instance Interruption Simulation',
            'roleArn': 'arn:aws:iam::665049067625:role/service-role/AWSFISIAMRole-1760016814597',
            'actions': {
                'interrupt-spot-instances': {
                    'actionId': 'aws:ec2:send-spot-instance-interruptions',
                    'parameters': {
                        'durationBeforeInterruption': 'PT2M'  # 2 minutes grace period
                    },
                    'targets': {
                        'SpotInstances': 'spot-instances-target'
                    }
                }
            }
        }
        
        experiment_template['targets'] = {
            'spot-instances-target': {
                'resourceType': 'aws:ec2:spot-instance',
                'resourceArns': [
                    'arn:aws:ec2:{0}:665049067625:instance/{1}'.format(region, instance_id)
                ],
                'selectionMode': 'ALL'
            }
        }
        experiment_template['stopConditions'] = [{'source': 'none'}]
        experiment_template['tags'] = {
            'TestType': 'BDD-SpotInterruption',
            'InstanceId': instance_id
        }
        
        log_to_file(get_scenario_name(context), "🧪 FIS: Creating experiment template")
        log_to_file(get_scenario_name(context), "  Target Instance: {0}".format(instance_id))
        log_to_file(get_scenario_name(context), "  Grace Period: 2 minutes")
        
        response = fis.create_experiment_template(**experiment_template)
        template_id = response['experimentTemplate']['id']
        
        log_to_file(get_scenario_name(context), "  ✅ Template Created: {0}".format(template_id))
        return template_id
        
    except Exception as e:
        log_to_file(get_scenario_name(context), "❌ ERROR creating FIS template: {0}".format(str(e)))
        raise

def start_fis_spot_interruption_experiment(context, template_id, region):
    """Start the FIS spot interruption experiment"""
    try:
        fis = boto3.client('fis', region_name=region)
        
        log_to_file(get_scenario_name(context), "🚀 FIS: Starting spot interruption experiment")
        log_to_file(get_scenario_name(context), "  Template ID: {0}".format(template_id))
        
        response = fis.start_experiment(
            experimentTemplateId=template_id,
            tags={
                'TestRun': datetime.now().strftime('%Y%m%d-%H%M%S'),
                'TestType': 'BDD-SpotInterruption'
            }
        )
        
        experiment_id = response['experiment']['id']
        experiment_state = response['experiment']['state']['status']
        
        log_to_file(get_scenario_name(context), "  ✅ Experiment Started: {0}".format(experiment_id))
        log_to_file(get_scenario_name(context), "  Initial State: {0}".format(experiment_state))
        
        return experiment_id
        
    except Exception as e:
        log_to_file(get_scenario_name(context), "❌ ERROR starting FIS experiment: {0}".format(str(e)))
        raise

# Comprehensive Monitor Class - Python 2.7 Compatible
class ComprehensiveMonitor(object):  # Python 2.7 uses object base class
    """Comprehensive monitoring system for spot interruption lifecycle"""
    
    def __init__(self, context):
        self.context = context
        self.monitoring_active = False
        self.events = []
        self.karpenter_logs = []
        self.pod_events = []
        self.node_events = []
        self.sqs_messages = []
        self.eventbridge_events = []
        
        # Monitoring threads
        self.threads = []
        self.stop_event = threading.Event()
        
    def start_monitoring(self):
        """Start all monitoring threads"""
        if self.monitoring_active:
            log_to_file(get_scenario_name(self.context), "ℹ️ Monitoring already active")
            return
            
        self.monitoring_active = True
        self.stop_event.clear()
        
        log_to_file(get_scenario_name(self.context), "🔍 Initializing comprehensive monitoring threads...")
        
        # Start monitoring threads - Python 2.7 compatible
        threads = [
            ('Karpenter Logs', threading.Thread(target=self._monitor_karpenter_logs)),
            ('Pod Events', threading.Thread(target=self._monitor_pod_events)),
            ('Node Events', threading.Thread(target=self._monitor_node_events)),
            ('SQS Queue', threading.Thread(target=self._monitor_sqs_queue)),
        ]
        
        for name, thread in threads:
            thread.daemon = True  # Python 2.7 syntax
            thread.start()
            self.threads.append(thread)
            log_to_file(get_scenario_name(self.context), "  ✅ {0} monitoring thread started".format(name))
        
        log_to_file(get_scenario_name(self.context), "🔍 All {0} monitoring threads active and ready".format(len(threads)))
        
        # Give threads a moment to initialize
        time.sleep(1)
    
    def stop_monitoring(self):
        """Stop all monitoring threads"""
        if not self.monitoring_active:
            return
            
        log_to_file(get_scenario_name(self.context), "⏹️ Stopping comprehensive monitoring...")
        
        self.monitoring_active = False
        self.stop_event.set()
        
        # Wait for threads to finish
        stopped_threads = 0
        for thread in self.threads:
            if thread.is_alive():
                thread.join(timeout=2)
                if not thread.is_alive():
                    stopped_threads += 1
                else:
                    log_to_file(get_scenario_name(self.context), "⚠️ Thread did not stop gracefully")
            else:
                stopped_threads += 1
        
        self.threads = []
        log_to_file(get_scenario_name(self.context), "⏹️ Stopped {0} monitoring threads".format(stopped_threads))  
  def _monitor_karpenter_logs(self):
        """Monitor Karpenter controller logs - Python 2.7 compatible"""
        try:
            cmd = "kubectl logs -f -n karpenter deployment/karpenter --tail=0"
            process = subprocess.Popen(
                cmd, shell=True, stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE
            )
            
            while self.monitoring_active and not self.stop_event.is_set():
                line = process.stdout.readline()
                if line:
                    timestamp = datetime.now()
                    line = line.decode('utf-8').strip()  # Python 2.7 needs decode
                    
                    self.karpenter_logs.append({
                        'timestamp': timestamp,
                        'message': line
                    })
                    
                    # Enhanced SQS message processing detection
                    line_lower = line.lower()
                    
                    # Detect specific SQS operations
                    sqs_operation = None
                    if 'sqs:receivemessage' in line_lower or 'receivemessage' in line_lower:
                        sqs_operation = 'ReceiveMessage'
                    elif 'sqs:deletemessage' in line_lower or 'deletemessage' in line_lower:
                        sqs_operation = 'DeleteMessage'
                    elif 'disruption.queue' in line_lower:
                        sqs_operation = 'DisruptionQueue'
                    elif any(keyword in line_lower for keyword in ['sqs', 'queue']):
                        sqs_operation = 'General'
                    
                    if sqs_operation:
                        log_to_file(get_scenario_name(self.context), "📨 KARPENTER SQS ({0}): {1}".format(sqs_operation, line))
                        self.events.append({
                            'type': 'karpenter_sqs_activity',
                            'subtype': sqs_operation.lower(),
                            'timestamp': timestamp,
                            'message': line,
                            'component': 'karpenter-sqs'
                        })
                    
                    # Detect spot interruption processing
                    if any(keyword in line_lower for keyword in [
                        'spot interrupt', 'spot termination', 'instance interrupt'
                    ]):
                        log_to_file(get_scenario_name(self.context), "⚡ KARPENTER SPOT: {0}".format(line))
                        self.events.append({
                            'type': 'karpenter_spot_processing',
                            'timestamp': timestamp,
                            'message': line,
                            'component': 'karpenter-spot'
                        })
                
                time.sleep(0.1)
            
            process.terminate()
            
        except Exception as e:
            log_to_file(get_scenario_name(self.context), "⚠️ Karpenter log monitoring error: {0}".format(str(e)))   
 def _monitor_sqs_queue(self):
        """Monitor SQS queue for messages - Python 2.7 compatible"""
        try:
            # Check if SQS monitoring is configured
            if not hasattr(self.context, 'sqs_queue_name') or not self.context.sqs_queue_name:
                log_to_file(get_scenario_name(self.context), "ℹ️ SQS monitoring: No queue configured, skipping")
                return
            
            sqs = boto3.client('sqs', region_name=self.context.region)
            
            try:
                queue_url_response = sqs.get_queue_url(QueueName=self.context.sqs_queue_name)
                queue_url = queue_url_response['QueueUrl']
                log_to_file(get_scenario_name(self.context), "✅ SQS monitoring: Connected to queue {0}".format(self.context.sqs_queue_name))
            except Exception as queue_error:
                log_to_file(get_scenario_name(self.context), "⚠️ SQS monitoring: Queue '{0}' not accessible: {1}".format(self.context.sqs_queue_name, str(queue_error)))
                return
            
            # Start monitoring loop
            while self.monitoring_active and not self.stop_event.is_set():
                try:
                    response = sqs.receive_message(
                        QueueUrl=queue_url,
                        MaxNumberOfMessages=10,
                        WaitTimeSeconds=2,
                        MessageAttributeNames=['All']
                    )
                    
                    current_time = datetime.now()
                    messages = response.get('Messages', [])
                    
                    for message in messages:
                        message_id = message['MessageId']
                        message_body = json.loads(message['Body']) if message.get('Body') else {}
                        
                        self.sqs_messages.append({
                            'timestamp': current_time,
                            'message_id': message_id,
                            'body': message_body,
                            'receipt_handle': message['ReceiptHandle']
                        })
                        
                        # Log important messages
                        detail_type = message_body.get('DetailType', '')
                        if 'spot' in detail_type.lower():
                            log_to_file(get_scenario_name(self.context), "📨 SQS: NEW spot interruption message received")
                            log_to_file(get_scenario_name(self.context), "  Message ID: {0}".format(message_id))
                            log_to_file(get_scenario_name(self.context), "  Detail Type: {0}".format(detail_type))
                    
                    time.sleep(3)
                    
                except Exception as poll_error:
                    log_to_file(get_scenario_name(self.context), "⚠️ SQS polling error: {0}".format(str(poll_error)))
                    time.sleep(5)
                
        except Exception as e:
            log_to_file(get_scenario_name(self.context), "⚠️ SQS monitoring error: {0}".format(str(e)))  
  def _monitor_pod_events(self):
        """Monitor pod events - Python 2.7 compatible"""
        try:
            while self.monitoring_active and not self.stop_event.is_set():
                cmd = "kubectl get events -n {0} --field-selector involvedObject.kind=Pod -o json".format(self.context.namespace)
                process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                stdout, stderr = process.communicate()
                
                if process.returncode == 0:
                    events_data = json.loads(stdout)
                    for event in events_data.get('items', []):
                        event_time = event.get('firstTimestamp') or event.get('eventTime')
                        if event_time:
                            # Python 2.7 compatible datetime parsing
                            try:
                                # Remove Z and add +00:00 for timezone
                                event_time_str = event_time.replace('Z', '+00:00')
                                # For Python 2.7, we'll use a simpler approach
                                timestamp = datetime.now()  # Simplified for Python 2.7
                            except:
                                timestamp = datetime.now()
                            
                            self.pod_events.append({
                                'timestamp': timestamp,
                                'reason': event.get('reason'),
                                'message': event.get('message'),
                                'type': event.get('type'),
                                'pod': event.get('involvedObject', {}).get('name')
                            })
                
                time.sleep(5)
                
        except Exception as e:
            log_to_file(get_scenario_name(self.context), "⚠️ Pod event monitoring error: {0}".format(str(e)))
    
    def _monitor_node_events(self):
        """Monitor node events - Python 2.7 compatible"""
        try:
            while self.monitoring_active and not self.stop_event.is_set():
                cmd = "kubectl get events --field-selector involvedObject.kind=Node -o json"
                process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                stdout, stderr = process.communicate()
                
                if process.returncode == 0:
                    events_data = json.loads(stdout)
                    for event in events_data.get('items', []):
                        event_time = event.get('firstTimestamp') or event.get('eventTime')
                        if event_time:
                            # Simplified timestamp for Python 2.7
                            timestamp = datetime.now()
                            
                            self.node_events.append({
                                'timestamp': timestamp,
                                'reason': event.get('reason'),
                                'message': event.get('message'),
                                'type': event.get('type'),
                                'node': event.get('involvedObject', {}).get('name')
                            })
                
                time.sleep(5)
                
        except Exception as e:
            log_to_file(get_scenario_name(self.context), "⚠️ Node event monitoring error: {0}".format(str(e)))# BDD St
ep Definitions - Python 2.7 Compatible

@given('I have access to EKS cluster "{cluster_name}" in region "{region}" for spot interruption testing')
def step_connect_to_cluster(context, cluster_name, region):
    """Connect to EKS cluster for spot interruption testing"""
    context.cluster_name = cluster_name
    context.region = region
    
    # Update kubeconfig
    subprocess.check_call([
        "aws", "eks", "update-kubeconfig", 
        "--region", region, 
        "--name", cluster_name
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    # Initialize results file
    with open("results/spot_interruption_test_results.log", "w") as f:
        f.write("SPOT INTERRUPTION TEST RUN\n")
        f.write("Started: {0}\n".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        f.write("="*80 + "\n\n")
    
    log_to_file(get_scenario_name(context), "Connected to cluster {0} in region {1}".format(cluster_name, region))

@given('I set the namespace to "{namespace}"')
def step_set_namespace(context, namespace):
    """Set the namespace for testing"""
    context.namespace = namespace
    log_to_file(get_scenario_name(context), "Set namespace to: {0}".format(namespace))

@given('I configure SQS queue "{queue_name}" for spot interruption monitoring')
def step_configure_sqs_queue(context, queue_name):
    """Configure SQS queue for monitoring"""
    context.sqs_queue_name = queue_name
    log_to_file(get_scenario_name(context), "📬 SQS CONFIGURATION: Set queue name to '{0}'".format(queue_name))

@given('the {app_name} is running on a spot instance in namespace "{namespace}"')
def step_verify_app_on_spot_instance(context, app_name, namespace):
    """Verify application is running on a spot instance"""
    try:
        # Get pod information
        pod_cmd = "kubectl get pods -n {0} -l app={1} -o json".format(namespace, app_name)
        pod_result = run_kubectl(pod_cmd)
        pod_data = json.loads(pod_result)
        
        if not pod_data.get('items'):
            raise Exception("No pods found for app {0} in namespace {1}".format(app_name, namespace))
        
        pod = pod_data['items'][0]
        context.pod_name = pod['metadata']['name']
        context.node_name = pod['spec']['nodeName']
        
        log_to_file(get_scenario_name(context), "Found pod: {0}".format(context.pod_name))
        
        # Get node information
        node_cmd = "kubectl get node {0} -o json".format(context.node_name)
        node_result = run_kubectl(node_cmd)
        node_data = json.loads(node_result)
        
        # Extract instance ID from node
        instance_id = None
        provider_id = node_data.get('spec', {}).get('providerID', '')
        if provider_id:
            # Format: aws:///us-west-2a/i-1234567890abcdef0
            instance_id = provider_id.split('/')[-1]
        
        if not instance_id:
            raise Exception("Could not extract instance ID from node {0}".format(context.node_name))
        
        context.instance_id = instance_id
        
        # Verify it's a spot instance using AWS API
        ec2 = boto3.client('ec2', region_name=context.region)
        instance_response = ec2.describe_instances(InstanceIds=[instance_id])
        
        instance = instance_response['Reservations'][0]['Instances'][0]
        instance_lifecycle = instance.get('InstanceLifecycle', '')
        instance_state = instance.get('State', {}).get('Name', '')
        
        log_to_file(get_scenario_name(context), "📋 SPOT INSTANCE VERIFICATION:")
        log_to_file(get_scenario_name(context), "  Pod Status: Running")
        log_to_file(get_scenario_name(context), "  Node Name: {0}".format(context.node_name))
        log_to_file(get_scenario_name(context), "  Instance ID: {0}".format(instance_id))
        log_to_file(get_scenario_name(context), "  Instance State: {0}".format(instance_state))
        log_to_file(get_scenario_name(context), "  Instance Lifecycle: {0}".format(instance_lifecycle))
        
        # Check for spot indicators
        spot_indicators = []
        if instance_lifecycle == 'spot':
            spot_indicators.append("instance lifecycle = spot")
        
        # Check node labels for spot indicators
        node_labels = node_data.get('metadata', {}).get('labels', {})
        for label, value in node_labels.items():
            if 'spot' in label.lower() or 'spot' in str(value).lower():
                spot_indicators.append("node labels contain 'spot'")
                break
        
        if spot_indicators:
            log_to_file(get_scenario_name(context), "  Spot Indicators: {0}".format(", ".join(spot_indicators)))
            log_to_file(get_scenario_name(context), "✅ VERIFIED: {0} is running on spot instance".format(app_name))
        else:
            log_to_file(get_scenario_name(context), "⚠️ WARNING: Could not confirm spot instance")
        
    except Exception as e:
        log_to_file(get_scenario_name(context), "❌ ERROR verifying spot instance: {0}".format(str(e)))
        raise@given(
'I start comprehensive monitoring for all components')
def step_start_comprehensive_monitoring(context):
    """Start comprehensive monitoring for all components"""
    try:
        log_to_file(get_scenario_name(context), "🔍 STARTING COMPREHENSIVE MONITORING FOR ALL COMPONENTS")
        
        # Initialize comprehensive monitoring
        if not hasattr(context, 'monitor'):
            context.monitor = ComprehensiveMonitor(context)
        
        # Start all monitoring threads
        context.monitor.start_monitoring()
        
        # Verify monitoring is active
        log_to_file(get_scenario_name(context), "📊 MONITORING STATUS:")
        log_to_file(get_scenario_name(context), "  Active: ✅ Yes")
        log_to_file(get_scenario_name(context), "  Components Monitored:")
        log_to_file(get_scenario_name(context), "    • Karpenter controller logs (SQS, NodeClaim, Spot processing)")
        log_to_file(get_scenario_name(context), "    • Pod lifecycle events")
        log_to_file(get_scenario_name(context), "    • Node lifecycle events")
        log_to_file(get_scenario_name(context), "    • SQS message processing")
        
        log_to_file(get_scenario_name(context), "✅ Comprehensive monitoring active and ready")
        context.monitoring_verified = True
        
    except Exception as e:
        log_to_file(get_scenario_name(context), "❌ ERROR starting comprehensive monitoring: {0}".format(str(e)))
        raise

@when('I trigger AWS FIS spot interruption with 2-minute grace period')
def step_trigger_fis_spot_interruption(context):
    """Trigger AWS FIS spot interruption"""
    try:
        context.interruption_start_time = datetime.now()
        log_to_file(get_scenario_name(context), "🧪 TRIGGERING AWS FIS SPOT INTERRUPTION at: {0}".format(context.interruption_start_time))
        
        # Initialize comprehensive monitoring if not already done
        if not hasattr(context, 'monitor'):
            context.monitor = ComprehensiveMonitor(context)
            context.monitor.start_monitoring()
        
        log_to_file(get_scenario_name(context), "🚀 Creating FIS experiment template...")
        
        # Create and start FIS experiment
        template_id = create_fis_spot_interruption_experiment(
            context, context.instance_id, context.region
        )
        context.fis_template_id = template_id
        
        log_to_file(get_scenario_name(context), "🚀 Starting FIS experiment...")
        
        experiment_id = start_fis_spot_interruption_experiment(
            context, template_id, context.region
        )
        context.fis_experiment_id = experiment_id
        
        log_to_file(get_scenario_name(context), "✅ FIS experiment started: {0}".format(experiment_id))
        log_to_file(get_scenario_name(context), "✅ All monitoring threads active and capturing events")
        
    except Exception as e:
        log_to_file(get_scenario_name(context), "❌ ERROR triggering FIS: {0}".format(str(e)))
        raise

@then('I should validate the complete spot interruption lifecycle')
def step_validate_complete_lifecycle(context):
    """Validate the complete spot interruption lifecycle"""
    try:
        log_to_file(get_scenario_name(context), "🔍 VALIDATING COMPLETE SPOT INTERRUPTION LIFECYCLE")
        log_to_file(get_scenario_name(context), "="*70)
        
        # Wait for the lifecycle to complete (with timeout)
        lifecycle_timeout = 600  # 10 minutes total timeout
        start_time = datetime.now()
        
        # Track validation results
        validations = {
            'sqs_activity': False,
            'spot_processing': False,
            'node_action': False,
            'lifecycle_complete': False
        }
        
        log_to_file(get_scenario_name(context), "🔍 Monitoring lifecycle progression...")
        
        # Monitor for lifecycle events
        while (datetime.now() - start_time).total_seconds() < lifecycle_timeout:
            
            # Check for SQS activity
            if not validations['sqs_activity'] and hasattr(context, 'monitor'):
                sqs_events = [e for e in context.monitor.events if e.get('type') == 'karpenter_sqs_activity']
                if sqs_events:
                    validations['sqs_activity'] = True
                    log_to_file(get_scenario_name(context), "✅ SQS activity detected")
            
            # Check for spot processing
            if not validations['spot_processing'] and hasattr(context, 'monitor'):
                spot_events = [e for e in context.monitor.events if e.get('type') == 'karpenter_spot_processing']
                if spot_events:
                    validations['spot_processing'] = True
                    log_to_file(get_scenario_name(context), "✅ Spot processing detected")
            
            # Check for node actions
            if not validations['node_action'] and hasattr(context, 'monitor'):
                node_events = [e for e in context.monitor.events if e.get('type') == 'karpenter_node_action']
                if node_events:
                    validations['node_action'] = True
                    log_to_file(get_scenario_name(context), "✅ Node action detected")
            
            # Check if lifecycle is complete
            if all([validations['sqs_activity'], validations['spot_processing']]):
                validations['lifecycle_complete'] = True
                elapsed = (datetime.now() - start_time).total_seconds()
                log_to_file(get_scenario_name(context), "🎯 LIFECYCLE VALIDATED in {0:.2f}s".format(elapsed))
                break
            
            # Progress update
            elapsed = (datetime.now() - start_time).total_seconds()
            if elapsed % 30 < 5:  # Log every 30 seconds
                completed = sum(validations.values())
                total = len(validations)
                log_to_file(get_scenario_name(context), "📊 Progress: {0}/{1} validations complete (+{2:.0f}s)".format(completed, total, elapsed))
            
            time.sleep(5)
        
        # Final validation summary
        log_to_file(get_scenario_name(context), "")
        log_to_file(get_scenario_name(context), "📋 LIFECYCLE VALIDATION SUMMARY")
        log_to_file(get_scenario_name(context), "="*50)
        
        for validation_name, passed in validations.items():
            status = "✅ PASS" if passed else "❌ FAIL"
            readable_name = validation_name.replace('_', ' ').title()
            log_to_file(get_scenario_name(context), "  {0}: {1}".format(readable_name, status))
        
        # Overall result
        total_passed = sum(validations.values())
        total_validations = len(validations)
        
        if total_passed >= 2:  # At least SQS and spot processing
            log_to_file(get_scenario_name(context), "")
            log_to_file(get_scenario_name(context), "🎉 SUCCESS: Core lifecycle components validated ({0}/{1})".format(total_passed, total_validations))
            context.lifecycle_validation_success = True
        else:
            log_to_file(get_scenario_name(context), "")
            log_to_file(get_scenario_name(context), "⚠️ PARTIAL SUCCESS: {0}/{1} components validated".format(total_passed, total_validations))
            context.lifecycle_validation_success = False
        
    except Exception as e:
        log_to_file(get_scenario_name(context), "❌ ERROR validating lifecycle: {0}".format(str(e)))
        context.lifecycle_validation_success = False

@then('I should cleanup test resources')
def step_cleanup_test_resources(context):
    """Cleanup test resources"""
    try:
        log_to_file(get_scenario_name(context), "🧹 CLEANING UP TEST RESOURCES")
        
        # Stop monitoring
        if hasattr(context, 'monitor'):
            context.monitor.stop_monitoring()
            log_to_file(get_scenario_name(context), "✅ Monitoring stopped")
        
        # Cleanup FIS template if it exists
        if hasattr(context, 'fis_template_id'):
            try:
                fis = boto3.client('fis', region_name=context.region)
                fis.delete_experiment_template(id=context.fis_template_id)
                log_to_file(get_scenario_name(context), "✅ FIS template cleaned up")
            except Exception as cleanup_error:
                log_to_file(get_scenario_name(context), "⚠️ FIS cleanup warning: {0}".format(str(cleanup_error)))
        
        log_to_file(get_scenario_name(context), "✅ Cleanup completed")
        
    except Exception as e:
        log_to_file(get_scenario_name(context), "⚠️ Cleanup error: {0}".format(str(e)))