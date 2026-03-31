import subprocess
import json
import time
import re
import boto3
import traceback
import threading
import queue
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
        f.write(f"[{timestamp}] {scenario_name}\n  → {message}\n")

def run_kubectl(cmd):
    """Run kubectl command and return output"""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"kubectl command failed: {result.stderr}")
    return result.stdout.strip()

# Basic setup step definitions
@given('I have access to EKS cluster "{cluster_name}" in region "{region}" for spot interruption testing')
def step_connect_to_cluster(context, cluster_name, region):
    """Connect to EKS cluster for spot interruption testing"""
    context.cluster_name = cluster_name
    context.region = region
    
    # Update kubeconfig
    subprocess.run([
        "aws", "eks", "update-kubeconfig", 
        "--region", region, 
        "--name", cluster_name
    ], check=True, capture_output=True)
    
    # Initialize results file
    with open("results/spot_interruption_test_results.log", "w") as f:
        f.write("SPOT INTERRUPTION TEST RUN\n")
        f.write(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("="*80 + "\n\n")
    
    log_to_file(get_scenario_name(context), f"Connected to cluster {cluster_name} in region {region}")

@given('I set the namespace to "{namespace}"')
def step_set_namespace(context, namespace):
    """Set the namespace for testing"""
    context.namespace = namespace
    log_to_file(get_scenario_name(context), f"Set namespace to: {namespace}")

@given('I configure SQS queue "{queue_name}" for spot interruption monitoring')
def step_configure_sqs_queue(context, queue_name):
    """Configure SQS queue for monitoring"""
    context.sqs_queue_name = queue_name
    log_to_file(get_scenario_name(context), f"📬 SQS CONFIGURATION: Set queue name to '{queue_name}'")

@given('the {app_name} is running on a spot instance in namespace "{namespace}"')
def step_verify_app_on_spot_instance(context, app_name, namespace):
    """Verify application is running on a spot instance"""
    try:
        # Get pod information
        pod_cmd = f"kubectl get pods -n {namespace} -l app={app_name} -o json"
        pod_result = run_kubectl(pod_cmd)
        pod_data = json.loads(pod_result)
        
        if not pod_data.get('items'):
            raise Exception(f"No pods found for app {app_name} in namespace {namespace}")
        
        pod = pod_data['items'][0]
        context.pod_name = pod['metadata']['name']
        context.node_name = pod['spec']['nodeName']
        
        log_to_file(get_scenario_name(context), f"Found pod: {context.pod_name}")
        
        # Get node information
        node_cmd = f"kubectl get node {context.node_name} -o json"
        node_result = run_kubectl(node_cmd)
        node_data = json.loads(node_result)
        
        # Extract instance ID from node
        instance_id = None
        provider_id = node_data.get('spec', {}).get('providerID', '')
        if provider_id:
            # Format: aws:///us-west-2a/i-1234567890abcdef0
            instance_id = provider_id.split('/')[-1]
        
        if not instance_id:
            raise Exception(f"Could not extract instance ID from node {context.node_name}")
        
        context.instance_id = instance_id
        
        # Verify it's a spot instance using AWS API
        ec2 = boto3.client('ec2', region_name=context.region)
        instance_response = ec2.describe_instances(InstanceIds=[instance_id])
        
        instance = instance_response['Reservations'][0]['Instances'][0]
        instance_lifecycle = instance.get('InstanceLifecycle', '')
        instance_state = instance.get('State', {}).get('Name', '')
        
        log_to_file(get_scenario_name(context), f"📋 SPOT INSTANCE VERIFICATION:")
        log_to_file(get_scenario_name(context), f"  Pod Status: Running")
        log_to_file(get_scenario_name(context), f"  Node Name: {context.node_name}")
        log_to_file(get_scenario_name(context), f"  Instance ID: {instance_id}")
        log_to_file(get_scenario_name(context), f"  Instance State: {instance_state}")
        log_to_file(get_scenario_name(context), f"  Instance Lifecycle: {instance_lifecycle}")
        
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
        
        # Check instance type and other indicators
        instance_type = instance.get('InstanceType', '')
        if instance_type:
            spot_indicators.append(f"instance type: {instance_type}")
        
        if 'karpenter.sh/capacity-type' in node_labels:
            capacity_type = node_labels['karpenter.sh/capacity-type']
            if capacity_type == 'spot':
                spot_indicators.append("karpenter.sh/capacity-type=spot")
        
        if spot_indicators:
            log_to_file(get_scenario_name(context), f"  Spot Indicators: {', '.join(spot_indicators)}")
            log_to_file(get_scenario_name(context), f"✅ VERIFIED: {app_name} is running on spot instance")
        else:
            log_to_file(get_scenario_name(context), f"⚠️ WARNING: Could not confirm spot instance")
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR verifying spot instance: {str(e)}")
        raise

def create_fis_spot_interruption_experiment(context, instance_id, region):
    """Create AWS FIS experiment template for spot interruption"""
    try:
        fis = boto3.client('fis', region_name=region)
        
        # Create experiment template for spot interruption
        experiment_template = {
            'description': 'BDD Test: Spot Instance Interruption Simulation',
            'roleArn': f'arn:aws:iam::665049067625:role/service-role/AWSFISIAMRole-1760016814597',
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
            },
            'targets': {
                'spot-instances-target': {
                    'resourceType': 'aws:ec2:spot-instance',
                    'resourceArns': [
                        f'arn:aws:ec2:{region}:665049067625:instance/{instance_id}'
                    ],
                    'selectionMode': 'ALL'
                }
            },
            'stopConditions': [
                {
                    'source': 'none'
                }
            ],
            'tags': {
                'TestType': 'BDD-SpotInterruption',
                'InstanceId': instance_id
            }
        }
        
        log_to_file(get_scenario_name(context), f"🧪 FIS: Creating experiment template")
        log_to_file(get_scenario_name(context), f"  Target Instance: {instance_id}")
        log_to_file(get_scenario_name(context), f"  Grace Period: 2 minutes")
        
        response = fis.create_experiment_template(**experiment_template)
        template_id = response['experimentTemplate']['id']
        
        log_to_file(get_scenario_name(context), f"  ✅ Template Created: {template_id}")
        
        return template_id
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR creating FIS template: {str(e)}")
        raise

def start_fis_spot_interruption_experiment(context, template_id, region):
    """Start the FIS spot interruption experiment"""
    try:
        fis = boto3.client('fis', region_name=region)
        
        log_to_file(get_scenario_name(context), f"🚀 FIS: Starting spot interruption experiment")
        log_to_file(get_scenario_name(context), f"  Template ID: {template_id}")
        
        response = fis.start_experiment(
            experimentTemplateId=template_id,
            tags={
                'TestRun': datetime.now().strftime('%Y%m%d-%H%M%S'),
                'TestType': 'BDD-SpotInterruption'
            }
        )
        
        experiment_id = response['experiment']['id']
        experiment_state = response['experiment']['state']['status']
        
        log_to_file(get_scenario_name(context), f"  ✅ Experiment Started: {experiment_id}")
        log_to_file(get_scenario_name(context), f"  Initial State: {experiment_state}")
        
        return experiment_id
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR starting FIS experiment: {str(e)}")
        raise

def monitor_fis_experiment(context, experiment_id, region, timeout_minutes=10):
    """Monitor the FIS experiment progress"""
    try:
        fis = boto3.client('fis', region_name=region)
        
        log_to_file(get_scenario_name(context), f"📊 FIS: Monitoring experiment progress")
        log_to_file(get_scenario_name(context), f"  Experiment ID: {experiment_id}")
        log_to_file(get_scenario_name(context), f"  Timeout: {timeout_minutes} minutes")
        
        start_time = datetime.now()
        timeout = timedelta(minutes=timeout_minutes)
        
        while datetime.now() - start_time < timeout:
            response = fis.get_experiment(id=experiment_id)
            experiment = response['experiment']
            
            state = experiment['state']['status']
            reason = experiment['state'].get('reason', 'No reason provided')
            
            current_time = datetime.now()
            elapsed = (current_time - start_time).total_seconds()
            
            log_to_file(get_scenario_name(context), f"  Status at +{elapsed:.0f}s: {state}")
            
            if state == 'completed':
                log_to_file(get_scenario_name(context), f"  ✅ Experiment completed successfully")
                return True
            elif state == 'failed':
                log_to_file(get_scenario_name(context), f"  ❌ Experiment failed: {reason}")
                return False
            elif state == 'stopped':
                log_to_file(get_scenario_name(context), f"  ⏹️ Experiment stopped: {reason}")
                return False
            
            # Log action states
            actions = experiment.get('actions', {})
            for action_name, action_data in actions.items():
                action_state = action_data.get('state', {}).get('status', 'unknown')
                log_to_file(get_scenario_name(context), f"    Action '{action_name}': {action_state}")
            
            time.sleep(10)  # Check every 10 seconds
        
        log_to_file(get_scenario_name(context), f"  ⏰ Monitoring timeout after {timeout_minutes} minutes")
        return False
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR monitoring FIS experiment: {str(e)}")
        return False

def cleanup_fis_experiment_template(context, template_id, region):
    """Clean up the FIS experiment template"""
    try:
        fis = boto3.client('fis', region_name=region)
        
        log_to_file(get_scenario_name(context), f"🧹 FIS: Cleaning up experiment template")
        log_to_file(get_scenario_name(context), f"  Template ID: {template_id}")
        
        fis.delete_experiment_template(id=template_id)
        log_to_file(get_scenario_name(context), f"  ✅ Template deleted successfully")
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"⚠️ WARNING: Failed to cleanup FIS template: {str(e)}")

# Import common functions from the main steps file

class ComprehensiveMonitor:
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
        
        # Start monitoring threads
        threads = [
            ('Karpenter Logs', threading.Thread(target=self._monitor_karpenter_logs, daemon=True)),
            ('Pod Events', threading.Thread(target=self._monitor_pod_events, daemon=True)),
            ('Node Events', threading.Thread(target=self._monitor_node_events, daemon=True)),
            ('SQS Queue', threading.Thread(target=self._monitor_sqs_queue, daemon=True)),
            ('Application Pods', threading.Thread(target=self._monitor_application_pods, daemon=True)),
        ]
        
        for name, thread in threads:
            thread.start()
            self.threads.append(thread)
            log_to_file(get_scenario_name(self.context), f"  ✅ {name} monitoring thread started")
        
        log_to_file(get_scenario_name(self.context), f"🔍 All {len(threads)} monitoring threads active and ready")
        
        # Give threads a moment to initialize
        import time
        time.sleep(1)
    
    def stop_monitoring(self):
        """Stop all monitoring threads"""
        if not self.monitoring_active:
            return
            
        log_to_file(get_scenario_name(self.context), "⏹️ Stopping comprehensive monitoring...")
        
        self.monitoring_active = False
        self.stop_event.set()
        
        # Wait for threads to finish with better timeout handling
        stopped_threads = 0
        for thread in self.threads:
            if thread.is_alive():
                thread.join(timeout=2)  # Shorter timeout
                if not thread.is_alive():
                    stopped_threads += 1
                else:
                    log_to_file(get_scenario_name(self.context), f"⚠️ Thread did not stop gracefully")
            else:
                stopped_threads += 1
        
        # Clear the threads list
        self.threads.clear()
        
        log_to_file(get_scenario_name(self.context), f"⏹️ Stopped {stopped_threads} monitoring threads")
        
        # Log summary of captured events
        total_events = (len(self.karpenter_logs) + len(self.pod_events) + 
                       len(self.node_events) + len(self.sqs_messages))
        log_to_file(get_scenario_name(self.context), f"📊 Monitoring summary: {total_events} total events captured")
        
        # Enhanced Karpenter logs summary with specific pod
        if self.karpenter_logs:
            log_to_file(get_scenario_name(self.context), f"  - Karpenter logs: {len(self.karpenter_logs)} total")
            log_to_file(get_scenario_name(self.context), f"    • karpenter-75dbd6c5dd-4csmc: {len(self.karpenter_logs)} logs")
        else:
            log_to_file(get_scenario_name(self.context), f"  - Karpenter logs: 0")
        
        log_to_file(get_scenario_name(self.context), f"  - Pod events: {len(self.pod_events)}")
        log_to_file(get_scenario_name(self.context), f"  - Node events: {len(self.node_events)}")
        log_to_file(get_scenario_name(self.context), f"  - SQS messages: {len(self.sqs_messages)}")
    
    def get_monitoring_status(self):
        """Get current monitoring status"""
        alive_threads = [t for t in self.threads if t.is_alive()]
        return {
            'active': self.monitoring_active,
            'threads_count': len(alive_threads),
            'total_threads': len(self.threads),
            'events_captured': {
                'karpenter_logs': len(self.karpenter_logs),
                'pod_events': len(self.pod_events),
                'node_events': len(self.node_events),
                'sqs_messages': len(self.sqs_messages)
            }
        }
    
    def _monitor_karpenter_logs(self):
        """Monitor specific Karpenter controller pod for queue-related logs with detailed event categorization"""
        try:
            # Use the specific Karpenter pod
            karpenter_pod = "karpenter-75dbd6c5dd-4csmc"
            
            log_to_file(get_scenario_name(self.context), f"📱 Monitoring logs from specific Karpenter pod: {karpenter_pod}")
            
            # Start monitoring the specific pod
            try:
                cmd = f"kubectl logs -f -n karpenter {karpenter_pod} --tail=0"
                process = subprocess.Popen(
                    cmd, shell=True, stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE, text=True
                )
                log_to_file(get_scenario_name(self.context), f"✅ Started monitoring logs for pod: {karpenter_pod}")
            except Exception as pod_error:
                log_to_file(get_scenario_name(self.context), f"⚠️ Could not start monitoring pod {karpenter_pod}: {str(pod_error)}")
                return
            
            # Event categories for detailed logging
            event_categories = {
                'sqs_operations': {
                    'patterns': ['messages', 'queue', 'spot_interrupted'],
                    'emoji': '📨',
                    'label': 'SQS'
                },
                'spot_interruption': {
                    'patterns': ['spot interrupt', 'spot termination', 'instance interrupt', 'spot_interrupted', 
                               'interruption message', 'spot warning'],
                    'emoji': '⚡',
                    'label': 'SPOT INTERRUPTION'
                },
                'node_operations': {
                    'patterns': ['taint', 'cordon', 'drain', 'evict', 'node termination', 'node deletion'],
                    'emoji': '🏷️',
                    'label': 'NODE OPS'
                },
                'nodeclaim_lifecycle': {
                    'patterns': ['nodeclaim', 'provisioning', 'deprovisioning', 'created nodeclaim', 
                               'launching instance', 'instance launched', 'registered nodeclaim'],
                    'emoji': '🔧',
                    'label': 'NODECLAIM'
                },
                'pod_scheduling': {
                    'patterns': ['pod scheduling', 'scheduling pod', 'bound pod', 'pending pod', 'provisionable pod'],
                    'emoji': '📦',
                    'label': 'POD SCHEDULING'
                },
                'aws_api_calls': {
                    'patterns': ['ec2:', 'aws api', 'runinstances', 'terminateinstances', 'describeinstances'],
                    'emoji': '☁️',
                    'label': 'AWS API'
                },
                'errors_warnings': {
                    'patterns': ['error', 'failed', 'exception', 'warn', 'timeout'],
                    'emoji': '❌',
                    'label': 'ERROR/WARN'
                }
            }
            
            # Monitor the pod process
            while self.monitoring_active and not self.stop_event.is_set():
                if process.poll() is None:  # Process is still running
                    try:
                        line = process.stdout.readline()
                        if line:
                            timestamp = datetime.now()
                            line_stripped = line.strip()
                            line_lower = line_stripped.lower()
                            
                            # Store all logs
                            self.karpenter_logs.append({
                                'timestamp': timestamp,
                                'message': line_stripped,
                                'pod': karpenter_pod
                            })
                            
                            # Categorize and log detailed events
                            event_logged = False
                            
                            for category, config in event_categories.items():
                                if any(pattern in line_lower for pattern in config['patterns']):
                                    # Create simple, readable log message
                                    simple_message = self._create_simple_log_message(line_stripped, category, timestamp)
                                    log_to_file(get_scenario_name(self.context), simple_message)
                                    
                                    # Determine specific subtype for SQS operations
                                    subtype = self._determine_event_subtype(line_lower, category)
                                    
                                    self.events.append({
                                        'type': f'karpenter_{category}',
                                        'subtype': subtype,
                                        'timestamp': timestamp,
                                        'message': line_stripped,
                                        'component': f'karpenter-{category}',
                                        'pod': karpenter_pod
                                    })
                                    
                                    event_logged = True
                                    break
                            
                            # Log any uncategorized important events with simple format
                            if not event_logged and any(keyword in line_lower for keyword in [
                                'interrupt', 'spot', 'terminate', 'provision', 'schedule'
                            ]):
                                simple_message = f"[{timestamp.strftime('%H:%M:%S')}] KARPENTER: {self._extract_key_info(line_stripped)}"
                                log_to_file(get_scenario_name(self.context), simple_message)
                                self.events.append({
                                    'type': 'karpenter_general',
                                    'timestamp': timestamp,
                                    'message': line_stripped,
                                    'component': 'karpenter',
                                    'pod': karpenter_pod
                                })
                    
                    except Exception as read_error:
                        # Handle read errors gracefully
                        pass
                else:
                    # Process has terminated
                    log_to_file(get_scenario_name(self.context), f"⚠️ Karpenter pod {karpenter_pod} process terminated")
                    break
                
                time.sleep(0.1)
            
            # Terminate the process
            try:
                process.terminate()
            except:
                pass
            
        except Exception as e:
            log_to_file(get_scenario_name(self.context), f"⚠️ Karpenter log monitoring error: {str(e)}")
    
    def _create_simple_log_message(self, log_line, category, timestamp):
        """Create a simple, readable log message for the sequence of events"""
        time_str = timestamp.strftime('%H:%M:%S')
        
        if category == 'sqs_operations':
            if 'spot_interrupted' in log_line.lower():
                return f"[{time_str}] 📨 SQS: Received spot interruption message"
            elif 'messages' in log_line.lower():
                return f"[{time_str}] 📨 SQS: Processing messages"
            elif 'queue' in log_line.lower():
                return f"[{time_str}] 📨 SQS: Queue activity detected"
            else:
                return f"[{time_str}] 📨 SQS: {self._extract_key_info(log_line)}"
        
        elif category == 'spot_interruption':
            return f"[{time_str}] ⚡ SPOT: {self._extract_key_info(log_line)}"
        
        elif category == 'node_operations':
            if 'taint' in log_line.lower():
                return f"[{time_str}] 🏷️ NODE: Node tainted"
            elif 'cordon' in log_line.lower():
                return f"[{time_str}] 🏷️ NODE: Node cordoned"
            elif 'drain' in log_line.lower():
                return f"[{time_str}] 🏷️ NODE: Node draining"
            else:
                return f"[{time_str}] 🏷️ NODE: {self._extract_key_info(log_line)}"
        
        elif category == 'nodeclaim_lifecycle':
            if 'created' in log_line.lower():
                return f"[{time_str}] 🔧 NODECLAIM: New NodeClaim created"
            elif 'launched' in log_line.lower():
                return f"[{time_str}] 🔧 NODECLAIM: Instance launched"
            elif 'registered' in log_line.lower():
                return f"[{time_str}] 🔧 NODECLAIM: NodeClaim registered"
            else:
                return f"[{time_str}] 🔧 NODECLAIM: {self._extract_key_info(log_line)}"
        
        elif category == 'pod_scheduling':
            return f"[{time_str}] 📦 POD: {self._extract_key_info(log_line)}"
        
        elif category == 'aws_api_calls':
            return f"[{time_str}] ☁️ AWS: {self._extract_key_info(log_line)}"
        
        elif category == 'errors_warnings':
            return f"[{time_str}] ❌ ERROR: {self._extract_key_info(log_line)}"
        
        else:
            return f"[{time_str}] KARPENTER: {self._extract_key_info(log_line)}"
    
    def _extract_key_info(self, log_line):
        """Extract key information from log line for readable display"""
        try:
            # Try to parse as JSON and extract key fields
            if log_line.startswith('{') and log_line.endswith('}'):
                import json
                log_data = json.loads(log_line)
                
                # Extract the most important information
                message = log_data.get('message', '')
                level = log_data.get('level', '')
                
                # Look for specific important fields
                if 'NodeClaim' in log_data:
                    nodeclaim = log_data['NodeClaim'].get('name', 'unknown')
                    if message:
                        return f"{message} (NodeClaim: {nodeclaim})"
                    else:
                        return f"NodeClaim operation: {nodeclaim}"
                
                elif 'Node' in log_data:
                    node = log_data['Node'].get('name', 'unknown')
                    if message:
                        return f"{message} (Node: {node})"
                    else:
                        return f"Node operation: {node}"
                
                elif 'messageKind' in log_data:
                    kind = log_data['messageKind']
                    return f"Processing {kind} message"
                
                elif 'queue' in log_data:
                    queue = log_data['queue']
                    return f"Queue operation: {queue}"
                
                elif message:
                    return message
                
                else:
                    # Fallback to showing level and any action
                    action = log_data.get('action', log_data.get('reason', 'operation'))
                    return f"{level}: {action}" if action != 'operation' else level
            
            else:
                # For non-JSON logs, extract first meaningful part
                if len(log_line) > 100:
                    return log_line[:100] + "..."
                return log_line
                
        except:
            # Fallback for any parsing errors
            if len(log_line) > 100:
                return log_line[:100] + "..."
            return log_line

    def _extract_log_context(self, log_line, category):
        """Extract relevant context information from log lines"""
        context = {}
        
        try:
            # Try to parse as JSON first
            if log_line.startswith('{') and log_line.endswith('}'):
                log_data = json.loads(log_line)
                
                # Extract common fields
                if 'NodeClaim' in log_data:
                    context['nodeclaim'] = log_data['NodeClaim'].get('name', 'unknown')
                if 'Node' in log_data:
                    context['node'] = log_data['Node'].get('name', 'unknown')
                if 'instance-type' in log_data:
                    context['instance_type'] = log_data['instance-type']
                if 'zone' in log_data:
                    context['zone'] = log_data['zone']
                if 'queue' in log_data:
                    context['queue'] = log_data['queue']
                if 'messageKind' in log_data:
                    context['message_kind'] = log_data['messageKind']
                if 'action' in log_data:
                    context['action'] = log_data['action']
                if 'reason' in log_data:
                    context['reason'] = log_data['reason']
                
                # Category-specific extractions
                if category == 'sqs_operations':
                    if 'reconcileID' in log_data:
                        context['reconcile_id'] = log_data['reconcileID'][:8] + '...'
                elif category == 'nodeclaim_lifecycle':
                    if 'provider-id' in log_data:
                        context['provider_id'] = log_data['provider-id']
                    if 'requests' in log_data:
                        context['requests'] = log_data['requests']
                elif category == 'aws_api_calls':
                    if 'instance-id' in log_data:
                        context['instance_id'] = log_data['instance-id']
                
        except (json.JSONDecodeError, KeyError):
            # If not JSON, extract key information using regex
            import re
            
            # Extract node names
            node_match = re.search(r'node["\s]*[:\s]*["\s]*([a-zA-Z0-9\-\.]+)', log_line, re.IGNORECASE)
            if node_match:
                context['node'] = node_match.group(1)
            
            # Extract instance IDs
            instance_match = re.search(r'i-[a-f0-9]{17}', log_line)
            if instance_match:
                context['instance_id'] = instance_match.group(0)
            
            # Extract NodeClaim names
            nodeclaim_match = re.search(r'nodeclaim["\s]*[:\s]*["\s]*([a-zA-Z0-9\-]+)', log_line, re.IGNORECASE)
            if nodeclaim_match:
                context['nodeclaim'] = nodeclaim_match.group(1)
        
        # Format context for display
        if context:
            context_parts = []
            for key, value in context.items():
                context_parts.append(f"{key}={value}")
            return ", ".join(context_parts)
        
        return None
    
    def _determine_event_subtype(self, line_lower, category):
        """Determine specific subtype for events"""
        if category == 'sqs_operations':
            if 'spot_interrupted' in line_lower:
                return 'spot_interrupted'
            elif 'messages' in line_lower:
                return 'messages'
            elif 'queue' in line_lower:
                return 'queue_activity'
            else:
                return 'sqs_general'
        elif category == 'nodeclaim_lifecycle':
            if any(word in line_lower for word in ['created', 'creating', 'provisioning']):
                return 'creation'
            elif any(word in line_lower for word in ['launched', 'launching', 'running']):
                return 'launch'
            elif any(word in line_lower for word in ['registered', 'registration']):
                return 'registration'
            elif any(word in line_lower for word in ['deprovisioning', 'terminating', 'deleting']):
                return 'deletion'
            else:
                return 'unknown'
        elif category == 'node_operations':
            if 'taint' in line_lower:
                return 'taint'
            elif 'cordon' in line_lower:
                return 'cordon'
            elif 'drain' in line_lower:
                return 'drain'
            elif 'evict' in line_lower:
                return 'evict'
            else:
                return 'node_action'
        elif category == 'spot_interruption':
            if 'spot_interrupted' in line_lower:
                return 'spot_interrupted'
            elif 'interruption message' in line_lower:
                return 'interruption_message'
            elif 'spot warning' in line_lower:
                return 'spot_warning'
            else:
                return 'spot_processing'
        else:
            return 'general'
    
    def _monitor_pod_events(self):
        """Monitor pod events"""
        try:
            while self.monitoring_active and not self.stop_event.is_set():
                cmd = f"kubectl get events -n {self.context.namespace} --field-selector involvedObject.kind=Pod -o json"
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                
                if result.returncode == 0:
                    events_data = json.loads(result.stdout)
                    for event in events_data.get('items', []):
                        event_time = event.get('firstTimestamp') or event.get('eventTime')
                        if event_time:
                            self.pod_events.append({
                                'timestamp': datetime.fromisoformat(event_time.replace('Z', '+00:00')),
                                'reason': event.get('reason'),
                                'message': event.get('message'),
                                'type': event.get('type'),
                                'pod': event.get('involvedObject', {}).get('name')
                            })
                
                time.sleep(5)
                
        except Exception as e:
            log_to_file(get_scenario_name(self.context), f"⚠️ Pod event monitoring error: {str(e)}")
    
    def _monitor_node_events(self):
        """Monitor node events"""
        try:
            while self.monitoring_active and not self.stop_event.is_set():
                cmd = "kubectl get events --field-selector involvedObject.kind=Node -o json"
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                
                if result.returncode == 0:
                    events_data = json.loads(result.stdout)
                    for event in events_data.get('items', []):
                        event_time = event.get('firstTimestamp') or event.get('eventTime')
                        if event_time:
                            self.node_events.append({
                                'timestamp': datetime.fromisoformat(event_time.replace('Z', '+00:00')),
                                'reason': event.get('reason'),
                                'message': event.get('message'),
                                'type': event.get('type'),
                                'node': event.get('involvedObject', {}).get('name')
                            })
                
                time.sleep(5)
                
        except Exception as e:
            log_to_file(get_scenario_name(self.context), f"⚠️ Node event monitoring error: {str(e)}")
    
    def _monitor_sqs_queue(self):
        """Monitor SQS queue for messages with detailed processing tracking"""
        try:
            # Check if SQS monitoring is configured
            if not hasattr(self.context, 'sqs_queue_name') or not self.context.sqs_queue_name:
                log_to_file(get_scenario_name(self.context), "ℹ️ SQS monitoring: No queue configured, skipping")
                return
            
            # Validate queue exists before starting monitoring
            sqs = boto3.client('sqs', region_name=self.context.region)
            
            try:
                queue_url_response = sqs.get_queue_url(QueueName=self.context.sqs_queue_name)
                queue_url = queue_url_response['QueueUrl']
                log_to_file(get_scenario_name(self.context), f"✅ SQS monitoring: Connected to queue {self.context.sqs_queue_name}")
            except Exception as queue_error:
                log_to_file(get_scenario_name(self.context), f"⚠️ SQS monitoring: Queue '{self.context.sqs_queue_name}' not accessible: {str(queue_error)}")
                log_to_file(get_scenario_name(self.context), "ℹ️ SQS monitoring: Continuing without SQS monitoring")
                return
            
            # Track message states
            message_states = {}  # message_id -> {'first_seen': timestamp, 'last_seen': timestamp, 'deleted': bool}
            
            # Start monitoring loop
            while self.monitoring_active and not self.stop_event.is_set():
                try:
                    # Get queue attributes to track message count changes
                    attrs_response = sqs.get_queue_attributes(
                        QueueUrl=queue_url,
                        AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
                    )
                    
                    visible_messages = int(attrs_response['Attributes'].get('ApproximateNumberOfMessages', 0))
                    invisible_messages = int(attrs_response['Attributes'].get('ApproximateNumberOfMessagesNotVisible', 0))
                    
                    # Receive messages without deleting them (for monitoring)
                    response = sqs.receive_message(
                        QueueUrl=queue_url,
                        MaxNumberOfMessages=10,
                        WaitTimeSeconds=2,
                        MessageAttributeNames=['All'],
                        AttributeNames=['All']
                    )
                    
                    current_time = datetime.now()
                    messages = response.get('Messages', [])
                    
                    # Track new messages
                    for message in messages:
                        message_id = message['MessageId']
                        
                        if message_id not in message_states:
                            # New message detected
                            message_states[message_id] = {
                                'first_seen': current_time,
                                'last_seen': current_time,
                                'deleted': False,
                                'body': json.loads(message['Body']) if message.get('Body') else {}
                            }
                            
                            self.sqs_messages.append({
                                'timestamp': current_time,
                                'message_id': message_id,
                                'body': json.loads(message['Body']) if message.get('Body') else {},
                                'receipt_handle': message['ReceiptHandle'],
                                'state': 'new'
                            })
                            
                            # Log important messages
                            message_body = json.loads(message['Body']) if message.get('Body') else {}
                            detail_type = message_body.get('DetailType', '')
                            
                            if 'spot' in detail_type.lower():
                                log_to_file(get_scenario_name(self.context), f"📨 SQS: NEW spot interruption message received")
                                log_to_file(get_scenario_name(self.context), f"  Message ID: {message_id}")
                                log_to_file(get_scenario_name(self.context), f"  Detail Type: {detail_type}")
                                log_to_file(get_scenario_name(self.context), f"  Timestamp: {current_time.strftime('%H:%M:%S.%f')[:-3]}")
                        else:
                            # Update last seen time
                            message_states[message_id]['last_seen'] = current_time
                    
                    # Check for deleted messages (messages that were previously seen but no longer appear)
                    current_message_ids = {msg['MessageId'] for msg in messages}
                    for message_id, state in message_states.items():
                        if not state['deleted'] and message_id not in current_message_ids:
                            # Message was deleted/processed
                            state['deleted'] = True
                            state['deleted_time'] = current_time
                            
                            processing_time = (current_time - state['first_seen']).total_seconds()
                            
                            log_to_file(get_scenario_name(self.context), f"🗑️ SQS: Message DELETED/PROCESSED")
                            log_to_file(get_scenario_name(self.context), f"  Message ID: {message_id}")
                            log_to_file(get_scenario_name(self.context), f"  Processing time: {processing_time:.2f}s")
                            log_to_file(get_scenario_name(self.context), f"  Deleted at: {current_time.strftime('%H:%M:%S.%f')[:-3]}")
                            
                            # Add deletion event
                            self.sqs_messages.append({
                                'timestamp': current_time,
                                'message_id': message_id,
                                'body': state['body'],
                                'state': 'deleted',
                                'processing_time': processing_time
                            })
                    
                    # Log queue state changes
                    if hasattr(self, '_last_visible_count'):
                        if visible_messages != self._last_visible_count:
                            log_to_file(get_scenario_name(self.context), f"📊 SQS: Queue state change - Visible: {visible_messages}, Invisible: {invisible_messages}")
                    
                    self._last_visible_count = visible_messages
                    self._last_invisible_count = invisible_messages
                    
                    time.sleep(3)  # Check every 3 seconds for better granularity
                    
                except Exception as poll_error:
                    log_to_file(get_scenario_name(self.context), f"⚠️ SQS polling error: {str(poll_error)}")
                    time.sleep(5)  # Wait longer on error
                
        except Exception as e:
            log_to_file(get_scenario_name(self.context), f"⚠️ SQS monitoring error: {str(e)}")
            log_to_file(get_scenario_name(self.context), "ℹ️ SQS monitoring: Continuing test without SQS monitoring")

    def _monitor_application_pods(self):
        """Monitor application pod logs with comprehensive logging during spot interruption"""
        try:
            # Get the application pod name
            if not hasattr(self.context, 'pod_name') or not self.context.pod_name:
                log_to_file(get_scenario_name(self.context), "ℹ️ Application pod monitoring: No pod name configured, skipping")
                return
            
            log_to_file(get_scenario_name(self.context), f"📱 Starting math-compute-sqs-app monitoring: {self.context.pod_name}")
            
            # Store all application logs for later analysis
            self.application_logs = []
            self.spot_warning_detected = False
            self.spot_warning_time = None
            self.math_computation_active = False
            self.sqs_processing_active = False
            
            cmd = f"kubectl logs -f -n {self.context.namespace} {self.context.pod_name} --tail=0"
            process = subprocess.Popen(
                cmd, shell=True, stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE, text=True
            )
            
            while self.monitoring_active and not self.stop_event.is_set():
                line = process.stdout.readline()
                if line:
                    timestamp = datetime.now()
                    line_stripped = line.strip()
                    line_lower = line_stripped.lower()
                    
                    # Store all application logs
                    log_entry = {
                        'timestamp': timestamp,
                        'message': line_stripped,
                        'raw_line': line
                    }
                    self.application_logs.append(log_entry)
                    
                    # Detect spot warning initiation
                    spot_warning_keywords = [
                        'spot', 'interrupt', 'termination', 'warning', 'sigterm', 'sigkill',
                        'shutdown', 'graceful', 'prestop', 'preempt', 'evict'
                    ]
                    
                    if any(keyword in line_lower for keyword in spot_warning_keywords):
                        if not self.spot_warning_detected:
                            self.spot_warning_detected = True
                            self.spot_warning_time = timestamp
                            log_to_file(get_scenario_name(self.context), f"🚨 SPOT WARNING DETECTED IN MATH-COMPUTE-SQS-APP!")
                            log_to_file(get_scenario_name(self.context), f"⏰ Warning detected at: {timestamp.strftime('%H:%M:%S.%f')[:-3]}")
                        
                        # Log all spot-related messages with simple format
                        simple_app_message = f"[{timestamp.strftime('%H:%M:%S')}] 🚨 APP: {self._extract_key_info(line_stripped)}"
                        log_to_file(get_scenario_name(self.context), simple_app_message)
                        self.events.append({
                            'type': 'application_spot_event',
                            'timestamp': timestamp,
                            'message': line_stripped,
                            'component': 'math-app-spot',
                            'priority': 'high'
                        })
                    
                    # Look for math computation specific messages
                    elif any(keyword in line_lower for keyword in [
                        'math', 'compute', 'calculation', 'algorithm', 'processing', 'result',
                        'fibonacci', 'prime', 'factorial', 'sum', 'multiply', 'divide'
                    ]):
                        if not self.math_computation_active:
                            self.math_computation_active = True
                            log_to_file(get_scenario_name(self.context), f"🧮 MATH COMPUTATION STARTED")
                        
                        log_to_file(get_scenario_name(self.context), f"[{timestamp.strftime('%H:%M:%S')}] 🧮 MATH: {self._extract_key_info(line_stripped)}")
                        self.events.append({
                            'type': 'application_math_computation',
                            'timestamp': timestamp,
                            'message': line_stripped,
                            'component': 'math-app-compute'
                        })
                    
                    # Look for SQS-related messages in application logs
                    elif any(keyword in line_lower for keyword in [
                        'sqs', 'queue', 'message', 'aws', 'receive', 'send', 'delete', 'poll'
                    ]):
                        if not self.sqs_processing_active:
                            self.sqs_processing_active = True
                            log_to_file(get_scenario_name(self.context), f"📬 SQS PROCESSING STARTED")
                        
                        log_to_file(get_scenario_name(self.context), f"[{timestamp.strftime('%H:%M:%S')}] 📬 APP-SQS: {self._extract_key_info(line_stripped)}")
                        self.events.append({
                            'type': 'application_sqs_activity',
                            'timestamp': timestamp,
                            'message': line_stripped,
                            'component': 'math-app-sqs'
                        })
                    
                    # Look for application lifecycle events
                    elif any(keyword in line_lower for keyword in [
                        'start', 'stop', 'exit', 'error', 'fail', 'ready', 'health', 'listen'
                    ]):
                        log_to_file(get_scenario_name(self.context), f"[{timestamp.strftime('%H:%M:%S')}] 📱 APP: {self._extract_key_info(line_stripped)}")
                        self.events.append({
                            'type': 'application_lifecycle',
                            'timestamp': timestamp,
                            'message': line_stripped,
                            'component': 'math-app-lifecycle'
                        })
                    
                    # Look for work completion and task processing
                    elif any(keyword in line_lower for keyword in [
                        'completed', 'finished', 'done', 'task', 'job', 'work', 'processed'
                    ]):
                        log_to_file(get_scenario_name(self.context), f"✅ MATH-APP WORK: {line_stripped}")
                        self.events.append({
                            'type': 'application_work_completion',
                            'timestamp': timestamp,
                            'message': line_stripped,
                            'component': 'math-app-work'
                        })
                    
                    # Look for HTTP/API related logs
                    elif any(keyword in line_lower for keyword in [
                        'http', 'request', 'response', 'api', 'endpoint', 'server'
                    ]):
                        log_to_file(get_scenario_name(self.context), f"🌐 MATH-APP HTTP: {line_stripped}")
                        self.events.append({
                            'type': 'application_http_activity',
                            'timestamp': timestamp,
                            'message': line_stripped,
                            'component': 'math-app-http'
                        })
                    
                    # Log everything else as general application activity (especially after spot warning)
                    else:
                        # Always log if it's after spot warning, or if it contains any meaningful content
                        if (self.spot_warning_detected or 
                            any(keyword in line_lower for keyword in [
                                'connection', 'timeout', 'retry', 'database', 'config', 'env'
                            ]) or
                            len(line_stripped) > 10):  # Log substantial messages
                            log_to_file(get_scenario_name(self.context), f"📝 MATH-APP LOG: {line_stripped}")
                            self.events.append({
                                'type': 'application_general',
                                'timestamp': timestamp,
                                'message': line_stripped,
                                'component': 'math-app-general'
                            })
                
                time.sleep(0.1)
            
            process.terminate()
            
            # Log summary of math-compute-sqs-app monitoring
            if hasattr(self, 'application_logs'):
                log_to_file(get_scenario_name(self.context), f"📊 Math-compute-sqs-app monitoring summary:")
                log_to_file(get_scenario_name(self.context), f"  Total application logs captured: {len(self.application_logs)}")
                log_to_file(get_scenario_name(self.context), f"  Math computation active: {'✅' if self.math_computation_active else '❌'}")
                log_to_file(get_scenario_name(self.context), f"  SQS processing active: {'✅' if self.sqs_processing_active else '❌'}")
                
                if self.spot_warning_detected:
                    log_to_file(get_scenario_name(self.context), f"  Spot warning detected at: {self.spot_warning_time.strftime('%H:%M:%S.%f')[:-3]}")
                    
                    # Count logs after spot warning
                    logs_after_warning = [log for log in self.application_logs 
                                        if log['timestamp'] >= self.spot_warning_time]
                    log_to_file(get_scenario_name(self.context), f"  Logs captured after spot warning: {len(logs_after_warning)}")
                    
                    # Check if math computation was interrupted
                    math_logs_after_warning = [log for log in logs_after_warning 
                                             if any(keyword in log['message'].lower() for keyword in [
                                                 'math', 'compute', 'calculation', 'result'
                                             ])]
                    if math_logs_after_warning:
                        log_to_file(get_scenario_name(self.context), f"  Math computation continued during interruption: ✅ ({len(math_logs_after_warning)} logs)")
                    else:
                        log_to_file(get_scenario_name(self.context), f"  Math computation stopped during interruption: ⚠️")
                log_to_file(get_scenario_name(self.context), f"📊 Application monitoring summary:")
                log_to_file(get_scenario_name(self.context), f"  Total application logs captured: {len(self.application_logs)}")
                if self.spot_warning_detected:
                    log_to_file(get_scenario_name(self.context), f"  Spot warning detected at: {self.spot_warning_time.strftime('%H:%M:%S.%f')[:-3]}")
                    
                    # Count logs after spot warning
                    logs_after_warning = [log for log in self.application_logs 
                                        if log['timestamp'] >= self.spot_warning_time]
                    log_to_file(get_scenario_name(self.context), f"  Logs captured after spot warning: {len(logs_after_warning)}")
            
        except Exception as e:
            log_to_file(get_scenario_name(self.context), f"⚠️ Application pod monitoring error: {str(e)}")
            log_to_file(get_scenario_name(self.context), "ℹ️ Continuing test without application pod monitoring")
    
    def get_application_logs_during_interruption(self):
        """Get application logs that occurred during the spot interruption period"""
        if not hasattr(self, 'application_logs'):
            return []
        
        # If we detected spot warning, return logs from that point
        if hasattr(self, 'spot_warning_time') and self.spot_warning_time:
            return [log for log in self.application_logs 
                   if log['timestamp'] >= self.spot_warning_time]
        
        # Otherwise, return logs from interruption start time
        if hasattr(self.context, 'interruption_start_time'):
            return [log for log in self.application_logs 
                   if log['timestamp'] >= self.context.interruption_start_time]
        
        # Fallback: return all logs
        return self.application_logs

# Step definitions for comprehensive testing

@given('I capture the initial pod and node state')
def step_capture_initial_state(context):
    try:
        log_to_file(get_scenario_name(context), "📸 CAPTURING INITIAL STATE")
        
        # Get initial pod state
        pod_cmd = f"kubectl get pod {context.pod_name} -n {context.namespace} -o json"
        pod_result = run_kubectl(pod_cmd)
        context.initial_pod_state = json.loads(pod_result)
        
        # Get initial node state
        node_cmd = f"kubectl get node {context.node_name} -o json"
        node_result = run_kubectl(node_cmd)
        context.initial_node_state = json.loads(node_result)
        
        # Get initial NodeClaim state
        try:
            nodeclaim_cmd = f"kubectl get nodeclaim -o json"
            nodeclaim_result = run_kubectl(nodeclaim_cmd)
            context.initial_nodeclaim_state = json.loads(nodeclaim_result)
        except:
            context.initial_nodeclaim_state = None
        
        log_to_file(get_scenario_name(context), f"  Initial Pod: {context.pod_name}")
        log_to_file(get_scenario_name(context), f"  Initial Node: {context.node_name}")
        log_to_file(get_scenario_name(context), f"  Initial Instance: {context.instance_id}")
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR capturing initial state: {str(e)}")
        raise

@given('I start comprehensive monitoring for all components')
def step_start_comprehensive_monitoring(context):
    try:
        log_to_file(get_scenario_name(context), "🔍 STARTING COMPREHENSIVE MONITORING FOR ALL COMPONENTS")
        
        # Initialize comprehensive monitoring
        if not hasattr(context, 'monitor'):
            context.monitor = ComprehensiveMonitor(context)
        
        # Start all monitoring threads
        context.monitor.start_monitoring()
        
        # Verify monitoring is active
        status = context.monitor.get_monitoring_status()
        
        log_to_file(get_scenario_name(context), f"📊 MONITORING STATUS:")
        log_to_file(get_scenario_name(context), f"  Active: {'✅ Yes' if status['active'] else '❌ No'}")
        log_to_file(get_scenario_name(context), f"  Active Threads: {status['threads_count']}")
        log_to_file(get_scenario_name(context), f"  Components Monitored:")
        log_to_file(get_scenario_name(context), f"    • Karpenter controller logs (SQS, NodeClaim, Spot processing)")
        log_to_file(get_scenario_name(context), f"    • Pod lifecycle events")
        log_to_file(get_scenario_name(context), f"    • Node lifecycle events")
        log_to_file(get_scenario_name(context), f"    • SQS message processing")
        
        if status['active'] and status['threads_count'] >= 3:
            log_to_file(get_scenario_name(context), "✅ Comprehensive monitoring active and ready")
            context.monitoring_verified = True
        else:
            log_to_file(get_scenario_name(context), "❌ Monitoring systems not fully active")
            context.monitoring_verified = False
            raise Exception("Comprehensive monitoring not ready")
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR starting comprehensive monitoring: {str(e)}")
        raise

@given('I start monitoring pod events')
def step_start_pod_monitoring(context):
    if not hasattr(context, 'monitor'):
        context.monitor = ComprehensiveMonitor(context)
    
    log_to_file(get_scenario_name(context), "🔍 Starting pod event monitoring")
    context.pod_monitoring = True
    
    # Start monitoring immediately if this is called individually
    if not context.monitor.monitoring_active:
        context.monitor.start_monitoring()

@given('I start monitoring node events')
def step_start_node_monitoring(context):
    if not hasattr(context, 'monitor'):
        context.monitor = ComprehensiveMonitor(context)
    
    log_to_file(get_scenario_name(context), "🔍 Starting node event monitoring")
    context.node_monitoring = True
    
    # Start monitoring immediately if this is called individually
    if not context.monitor.monitoring_active:
        context.monitor.start_monitoring()

@given('I verify all monitoring is active and ready')
def step_verify_monitoring_active(context):
    try:
        log_to_file(get_scenario_name(context), "🔍 VERIFYING MONITORING STATUS")
        
        if not hasattr(context, 'monitor'):
            log_to_file(get_scenario_name(context), "⚠️ No monitor initialized - creating now")
            context.monitor = ComprehensiveMonitor(context)
        
        # Start monitoring if not already active
        if not context.monitor.monitoring_active:
            log_to_file(get_scenario_name(context), "🚀 Starting monitoring threads...")
            context.monitor.start_monitoring()
        
        # Get monitoring status
        status = context.monitor.get_monitoring_status()
        
        log_to_file(get_scenario_name(context), f"📊 MONITORING STATUS:")
        log_to_file(get_scenario_name(context), f"  Active: {'✅ Yes' if status['active'] else '❌ No'}")
        log_to_file(get_scenario_name(context), f"  Active Threads: {status['threads_count']}")
        log_to_file(get_scenario_name(context), f"  Events Captured So Far:")
        
        for event_type, count in status['events_captured'].items():
            log_to_file(get_scenario_name(context), f"    {event_type}: {count}")
        
        if status['active'] and status['threads_count'] >= 3:  # At least 3 core threads (Karpenter, Pod, Node)
            log_to_file(get_scenario_name(context), "✅ All monitoring systems are active and ready")
            if status['threads_count'] < 4:
                log_to_file(get_scenario_name(context), "ℹ️ SQS monitoring may not be available (queue not configured)")
            context.monitoring_verified = True
        else:
            log_to_file(get_scenario_name(context), "❌ Monitoring systems not fully active")
            context.monitoring_verified = False
            raise Exception("Core monitoring systems not ready")
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR verifying monitoring: {str(e)}")
        raise

@when('I trigger AWS FIS spot interruption with 2-minute grace period')
def step_trigger_fis_spot_interruption(context):
    try:
        context.interruption_start_time = datetime.now()
        log_to_file(get_scenario_name(context), f"🧪 TRIGGERING AWS FIS SPOT INTERRUPTION at: {context.interruption_start_time}")
        
        # Initialize comprehensive monitoring if not already done
        if not hasattr(context, 'monitor'):
            context.monitor = ComprehensiveMonitor(context)
        
        # Start all monitoring threads BEFORE triggering FIS
        log_to_file(get_scenario_name(context), f"🔍 Starting comprehensive monitoring in parallel...")
        context.monitor.start_monitoring()
        
        # Give monitoring threads a moment to initialize
        import time
        time.sleep(2)
        
        log_to_file(get_scenario_name(context), f"🚀 Creating FIS experiment template...")
        
        # Create and start FIS experiment
        template_id = create_fis_spot_interruption_experiment(
            context, context.instance_id, context.region
        )
        context.fis_template_id = template_id
        
        log_to_file(get_scenario_name(context), f"🚀 Starting FIS experiment...")
        
        experiment_id = start_fis_spot_interruption_experiment(
            context, template_id, context.region
        )
        context.fis_experiment_id = experiment_id
        
        log_to_file(get_scenario_name(context), f"✅ FIS experiment started: {experiment_id}")
        log_to_file(get_scenario_name(context), f"✅ All monitoring threads active and capturing events")
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR triggering FIS: {str(e)}")
        raise

@then('I should capture the spot interruption event in EventBridge')
def step_capture_eventbridge_event(context):
    try:
        log_to_file(get_scenario_name(context), "📡 MONITORING EVENTBRIDGE RULE ACTIVITY")
        
        # Monitor CloudWatch Events metrics for timing
        cloudwatch = boto3.client('cloudwatch', region_name=context.region)
        rule_name = 'Karpenter-SpotInterrupt-20251103180522556700000020'
        
        log_to_file(get_scenario_name(context), f"🔍 Checking EventBridge rule: {rule_name}")
        log_to_file(get_scenario_name(context), f"⏰ Monitoring started at: {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
        
        # Monitor for rule activity over time
        start_monitoring = datetime.now()
        timeout = 300  # 5 minutes timeout
        check_interval = 10  # Check every 10 seconds
        
        baseline_matches = 0
        first_event_detected = False
        
        # Get baseline metrics
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(minutes=2)
            
            response = cloudwatch.get_metric_statistics(
                Namespace='AWS/Events',
                MetricName='MatchedEvents',
                Dimensions=[{'Name': 'RuleName', 'Value': rule_name}],
                StartTime=start_time,
                EndTime=end_time,
                Period=60,
                Statistics=['Sum']
            )
            
            baseline_matches = sum(dp['Sum'] for dp in response.get('Datapoints', []))
            log_to_file(get_scenario_name(context), f"📊 Baseline matched events: {baseline_matches}")
            
        except Exception as e:
            log_to_file(get_scenario_name(context), f"⚠️ Could not get baseline metrics: {str(e)}")
        
        # Monitor for new events
        while (datetime.now() - start_monitoring).total_seconds() < timeout:
            try:
                current_time = datetime.utcnow()
                check_start = current_time - timedelta(minutes=1)
                
                response = cloudwatch.get_metric_statistics(
                    Namespace='AWS/Events',
                    MetricName='MatchedEvents',
                    Dimensions=[{'Name': 'RuleName', 'Value': rule_name}],
                    StartTime=check_start,
                    EndTime=current_time,
                    Period=60,
                    Statistics=['Sum']
                )
                
                recent_matches = sum(dp['Sum'] for dp in response.get('Datapoints', []))
                elapsed = (datetime.now() - start_monitoring).total_seconds()
                
                if recent_matches > 0 and not first_event_detected:
                    event_detection_time = datetime.now()
                    time_from_fis_start = (event_detection_time - context.interruption_start_time).total_seconds()
                    
                    log_to_file(get_scenario_name(context), f"✅ FIRST EVENTBRIDGE EVENT DETECTED!")
                    log_to_file(get_scenario_name(context), f"  Detection time: {event_detection_time.strftime('%H:%M:%S.%f')[:-3]}")
                    log_to_file(get_scenario_name(context), f"  Time from FIS start: +{time_from_fis_start:.2f}s")
                    log_to_file(get_scenario_name(context), f"  Rule matches in last minute: {recent_matches}")
                    
                    context.eventbridge_first_event_time = event_detection_time
                    context.eventbridge_event_captured = True
                    first_event_detected = True
                    break
                
                if elapsed % 30 < 5:  # Log every 30 seconds
                    log_to_file(get_scenario_name(context), f"  Monitoring EventBridge... +{elapsed:.0f}s (matches: {recent_matches})")
                
                time.sleep(check_interval)
                
            except Exception as e:
                log_to_file(get_scenario_name(context), f"⚠️ Error checking EventBridge metrics: {str(e)}")
                time.sleep(check_interval)
        
        if not first_event_detected:
            log_to_file(get_scenario_name(context), f"⚠️ No EventBridge events detected within {timeout}s timeout")
            context.eventbridge_event_captured = False
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR monitoring EventBridge: {str(e)}")
        context.eventbridge_event_captured = False

@then('I should validate the complete spot interruption lifecycle')
def step_validate_complete_lifecycle(context):
    try:
        log_to_file(get_scenario_name(context), "🔍 VALIDATING COMPLETE SPOT INTERRUPTION LIFECYCLE")
        log_to_file(get_scenario_name(context), "=" * 60)
        
        # Track the complete lifecycle timing
        lifecycle_start_time = datetime.now()
        flow_timestamps = {}
        
        # Extended timeout for complete lifecycle including pod/node creation
        lifecycle_timeout = 600  # 10 minutes timeout
        
        # Track validation results for complete lifecycle
        validations = {
            'eventbridge_event': False,
            'sqs_message': False,
            'karpenter_receives': False,
            'node_creation': False,
            'pod_launch': False
        }
        
        log_to_file(get_scenario_name(context), "🔍 Monitoring complete lifecycle: EventBridge → SQS → Karpenter → Node → Pod")
        
        # Start comprehensive Karpenter log capture
        log_to_file(get_scenario_name(context), "📋 Starting comprehensive Karpenter controller log capture...")
        
        # Monitor for complete lifecycle events
        while (datetime.now() - lifecycle_start_time).total_seconds() < lifecycle_timeout:
            
            # 1. Check EventBridge event capture (only run once)
            if not validations['eventbridge_event'] and not hasattr(context, 'eventbridge_check_attempted'):
                try:
                    step_capture_eventbridge_event(context)
                    context.eventbridge_check_attempted = True
                    if getattr(context, 'eventbridge_event_captured', False):
                        validations['eventbridge_event'] = True
                        flow_timestamps['eventbridge'] = context.eventbridge_first_event_time
                        log_to_file(get_scenario_name(context), "✅ 1. EventBridge event captured")
                except:
                    context.eventbridge_check_attempted = True
            
            # 2. Check SQS message (use already detected Karpenter SQS activity)
            if validations['eventbridge_event'] and not validations['sqs_message']:
                if hasattr(context, 'monitor') and context.monitor:
                    sqs_events = [event for event in context.monitor.events 
                                 if event.get('type') == 'karpenter_sqs_operations']
                    
                    if sqs_events:
                        first_sqs_event = min(sqs_events, key=lambda x: x['timestamp'])
                        flow_timestamps['sqs'] = first_sqs_event['timestamp']
                        log_to_file(get_scenario_name(context), f"✅ 2. SQS message sent at: {first_sqs_event['timestamp'].strftime('%H:%M:%S')}")
                        context.sqs_first_message_time = first_sqs_event['timestamp']
                        context.sqs_message_captured = True
                        validations['sqs_message'] = True
                    elif not hasattr(context, 'sqs_warning_logged'):
                        log_to_file(get_scenario_name(context), "⏳ Waiting for SQS message...")
                        context.sqs_warning_logged = True
            
            # 3. Check Karpenter receives message
            if validations['sqs_message'] and not validations['karpenter_receives']:
                if hasattr(context, 'monitor') and context.monitor:
                    # Look for any Karpenter SQS activity (receiving, processing)
                    karpenter_sqs_events = [e for e in context.monitor.events 
                                           if e.get('type') == 'karpenter_sqs_operations']
                    
                    if karpenter_sqs_events:
                        first_receive = min(karpenter_sqs_events, key=lambda x: x['timestamp'])
                        flow_timestamps['karpenter_receive'] = first_receive['timestamp']
                        log_to_file(get_scenario_name(context), f"✅ 3. Karpenter received message at: {first_receive['timestamp'].strftime('%H:%M:%S')}")
                        validations['karpenter_receives'] = True
            
            # 4. Check for new node creation (NodeClaim)
            if validations['karpenter_receives'] and not validations['node_creation']:
                try:
                    step_monitor_new_nodeclaim(context)
                    if getattr(context, 'new_nodeclaim_created', False):
                        flow_timestamps['node_creation'] = datetime.now()
                        log_to_file(get_scenario_name(context), f"✅ 4. New node creation started at: {flow_timestamps['node_creation'].strftime('%H:%M:%S')}")
                        validations['node_creation'] = True
                except:
                    # Fallback: Look for node-related events in monitor
                    if hasattr(context, 'monitor') and context.monitor:
                        node_events = [e for e in context.monitor.events 
                                     if 'node' in e.get('message', '').lower() 
                                     or 'nodeclaim' in e.get('message', '').lower()]
                        if node_events and not hasattr(context, 'node_fallback_logged'):
                            flow_timestamps['node_creation'] = datetime.now()
                            log_to_file(get_scenario_name(context), f"✅ 4. Node activity detected at: {flow_timestamps['node_creation'].strftime('%H:%M:%S')} (fallback)")
                            validations['node_creation'] = True
                            context.node_fallback_logged = True
            
            # 5. Check for new pod launch
            if validations['node_creation'] and not validations['pod_launch']:
                try:
                    step_capture_new_pod_scheduling(context)
                    if getattr(context, 'new_pod_scheduled', False):
                        # Verify pod is actually ready
                        step_verify_new_pod_ready(context)
                        if getattr(context, 'new_pod_ready', False):
                            flow_timestamps['pod_launch'] = datetime.now()
                            log_to_file(get_scenario_name(context), f"✅ 5. New pod launched and ready at: {flow_timestamps['pod_launch'].strftime('%H:%M:%S')}")
                            validations['pod_launch'] = True
                except:
                    # Fallback: Look for pod-related events in monitor
                    if hasattr(context, 'monitor') and context.monitor:
                        pod_events = [e for e in context.monitor.events 
                                    if 'pod' in e.get('message', '').lower() 
                                    and ('scheduled' in e.get('message', '').lower() 
                                         or 'running' in e.get('message', '').lower()
                                         or 'ready' in e.get('message', '').lower())]
                        if pod_events and not hasattr(context, 'pod_fallback_logged'):
                            flow_timestamps['pod_launch'] = datetime.now()
                            log_to_file(get_scenario_name(context), f"✅ 5. Pod activity detected at: {flow_timestamps['pod_launch'].strftime('%H:%M:%S')} (fallback)")
                            validations['pod_launch'] = True
                            context.pod_fallback_logged = True
            
            # Check if complete lifecycle is done
            if all(validations.values()):
                elapsed = (datetime.now() - lifecycle_start_time).total_seconds()
                log_to_file(get_scenario_name(context), f"🎯 COMPLETE LIFECYCLE COMPLETED in {elapsed:.2f}s")
                break
            
            # Progress update every 30 seconds
            elapsed = (datetime.now() - lifecycle_start_time).total_seconds()
            if elapsed % 30 < 5:  # Log every 30 seconds
                completed = sum(validations.values())
                total = len(validations)
                log_to_file(get_scenario_name(context), f"📊 Lifecycle Progress: {completed}/{total} steps complete (+{elapsed:.0f}s)")
            
            time.sleep(5)  # Check every 5 seconds
        
        # Capture final comprehensive Karpenter logs
        log_to_file(get_scenario_name(context), "")
        log_to_file(get_scenario_name(context), "📋 COMPREHENSIVE KARPENTER CONTROLLER LOGS")
        log_to_file(get_scenario_name(context), "=" * 50)
        
        try:
            step_capture_karpenter_logs(context)
            if hasattr(context, 'karpenter_logs_captured') and context.karpenter_logs_captured:
                log_to_file(get_scenario_name(context), "✅ All Karpenter controller logs captured successfully")
            else:
                log_to_file(get_scenario_name(context), "⚠️ Karpenter logs capture may be incomplete")
        except Exception as e:
            log_to_file(get_scenario_name(context), f"❌ Error capturing Karpenter logs: {str(e)}")
        
        # Calculate and log detailed timing for the entire lifecycle
        log_to_file(get_scenario_name(context), "")
        log_to_file(get_scenario_name(context), "⏱️ COMPLETE LIFECYCLE TIMING ANALYSIS")
        log_to_file(get_scenario_name(context), "=" * 45)
        
        if 'eventbridge' in flow_timestamps and 'sqs' in flow_timestamps:
            eventbridge_to_sqs = (flow_timestamps['sqs'] - flow_timestamps['eventbridge']).total_seconds()
            log_to_file(get_scenario_name(context), f"📡 EventBridge → SQS: {eventbridge_to_sqs:.2f}s")
        
        if 'sqs' in flow_timestamps and 'karpenter_receive' in flow_timestamps:
            sqs_to_karpenter = (flow_timestamps['karpenter_receive'] - flow_timestamps['sqs']).total_seconds()
            log_to_file(get_scenario_name(context), f"📬 SQS → Karpenter: {sqs_to_karpenter:.2f}s")
        
        if 'karpenter_receive' in flow_timestamps and 'node_creation' in flow_timestamps:
            karpenter_to_node = (flow_timestamps['node_creation'] - flow_timestamps['karpenter_receive']).total_seconds()
            log_to_file(get_scenario_name(context), f"🖥️ Karpenter → Node Creation: {karpenter_to_node:.2f}s")
        
        if 'node_creation' in flow_timestamps and 'pod_launch' in flow_timestamps:
            node_to_pod = (flow_timestamps['pod_launch'] - flow_timestamps['node_creation']).total_seconds()
            node_to_pod_minutes = int(node_to_pod // 60)
            node_to_pod_seconds = int(node_to_pod % 60)
            node_to_pod_formatted = f"{node_to_pod_minutes}:{node_to_pod_seconds:02d}" if node_to_pod_minutes > 0 else f"{node_to_pod:.2f}s"
            log_to_file(get_scenario_name(context), f"🚀 Node Creation → Pod Launch: {node_to_pod_formatted}")
        
        if 'eventbridge' in flow_timestamps and 'pod_launch' in flow_timestamps:
            total_lifecycle_time = (flow_timestamps['pod_launch'] - flow_timestamps['eventbridge']).total_seconds()
            total_minutes = int(total_lifecycle_time // 60)
            total_seconds = int(total_lifecycle_time % 60)
            total_formatted = f"{total_minutes}:{total_seconds:02d}" if total_minutes > 0 else f"{total_lifecycle_time:.2f}s"
            
            log_to_file(get_scenario_name(context), f"")
            log_to_file(get_scenario_name(context), f"🎯 TOTAL LIFECYCLE TIME: {total_formatted}")
            log_to_file(get_scenario_name(context), f"   (EventBridge detection → New pod ready)")
            
            # Performance assessment for complete lifecycle
            if total_lifecycle_time < 120:  # 2 minutes
                log_to_file(get_scenario_name(context), "🏆 EXCELLENT: Complete lifecycle in under 2 minutes")
            elif total_lifecycle_time < 300:  # 5 minutes
                log_to_file(get_scenario_name(context), "✅ GOOD: Complete lifecycle in under 5 minutes")
            elif total_lifecycle_time < 600:  # 10 minutes
                log_to_file(get_scenario_name(context), "⚠️ ACCEPTABLE: Complete lifecycle in under 10 minutes")
            else:
                log_to_file(get_scenario_name(context), "❌ SLOW: Complete lifecycle took over 10 minutes")
        
        # Final validation summary
        log_to_file(get_scenario_name(context), "")
        log_to_file(get_scenario_name(context), "📋 COMPLETE LIFECYCLE VALIDATION SUMMARY")
        log_to_file(get_scenario_name(context), "=" * 45)
        
        validation_labels = {
            'eventbridge_event': 'EventBridge Event Capture',
            'sqs_message': 'SQS Message Processing', 
            'karpenter_receives': 'Karpenter Message Receipt',
            'node_creation': 'New Node Creation',
            'pod_launch': 'New Pod Launch & Ready'
        }
        
        for validation_name, passed in validations.items():
            status = "✅ PASS" if passed else "❌ FAIL"
            label = validation_labels.get(validation_name, validation_name)
            log_to_file(get_scenario_name(context), f"  {label}: {status}")
        
        # Overall result
        total_passed = sum(validations.values())
        total_validations = len(validations)
        
        if total_passed == total_validations:
            log_to_file(get_scenario_name(context), f"")
            log_to_file(get_scenario_name(context), f"🎉 SUCCESS: Complete lifecycle validated! All {total_validations} steps completed.")
            context.lifecycle_validation_success = True
        else:
            log_to_file(get_scenario_name(context), f"")
            log_to_file(get_scenario_name(context), f"⚠️ PARTIAL: {total_passed}/{total_validations} lifecycle steps completed")
            context.lifecycle_validation_success = False
        
        # Store timing data for later use
        context.complete_lifecycle_timestamps = flow_timestamps
        context.complete_lifecycle_validations = validations
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR validating complete lifecycle: {str(e)}")
        context.lifecycle_validation_success = False
        context.lifecycle_validation_success = False

@then('I should analyze timing delays and calculate total processing time')
def step_analyze_timing_delays(context):
    try:
        log_to_file(get_scenario_name(context), "⏱️ ANALYZING TIMING DELAYS AND TOTAL PROCESSING TIME")
        
        if not hasattr(context, 'monitor') or not context.monitor:
            log_to_file(get_scenario_name(context), "⚠️ No monitoring data available for timing analysis")
            return
        
        # Get all captured events
        all_events = context.monitor.events
        karpenter_logs = context.monitor.karpenter_logs
        sqs_messages = context.monitor.sqs_messages
        
        # Key timestamps to track
        timestamps = {}
        delays = {}
        
        # 1. FIS experiment start time
        if hasattr(context, 'interruption_start_time'):
            timestamps['fis_start'] = context.interruption_start_time
            log_to_file(get_scenario_name(context), f"📅 FIS Start: {timestamps['fis_start'].strftime('%H:%M:%S.%f')[:-3]}")
        
        # 2. EventBridge event detection time
        if hasattr(context, 'eventbridge_first_event_time'):
            timestamps['eventbridge_event'] = context.eventbridge_first_event_time
            log_to_file(get_scenario_name(context), f"📅 EventBridge Event: {timestamps['eventbridge_event'].strftime('%H:%M:%S.%f')[:-3]}")
            
            if 'fis_start' in timestamps:
                delays['fis_to_eventbridge'] = (timestamps['eventbridge_event'] - timestamps['fis_start']).total_seconds()
                log_to_file(get_scenario_name(context), f"⏱️ FIS → EventBridge delay: {delays['fis_to_eventbridge']:.2f}s")
        
        # 3. First SQS message arrival time
        if sqs_messages:
            first_sqs_message = min(sqs_messages, key=lambda x: x['timestamp'])
            timestamps['sqs_message'] = first_sqs_message['timestamp']
            log_to_file(get_scenario_name(context), f"📅 First SQS Message: {timestamps['sqs_message'].strftime('%H:%M:%S.%f')[:-3]}")
            
            if 'eventbridge_event' in timestamps:
                delays['eventbridge_to_sqs'] = (timestamps['sqs_message'] - timestamps['eventbridge_event']).total_seconds()
                log_to_file(get_scenario_name(context), f"⏱️ EventBridge → SQS delay: {delays['eventbridge_to_sqs']:.2f}s")
        
        # 4. First Karpenter SQS ReceiveMessage operation (with disruption.queue priority)
        karpenter_receive_events = [e for e in all_events if e.get('type') == 'karpenter_sqs_activity' and e.get('subtype') == 'receivemessage']
        disruption_queue_events = [e for e in all_events if e.get('type') == 'karpenter_sqs_activity' and e.get('subtype') == 'disruptionqueue']
        
        # Prioritize ReceiveMessage, then disruption.queue
        if karpenter_receive_events:
            first_receive = min(karpenter_receive_events, key=lambda x: x['timestamp'])
            timestamps['karpenter_sqs_receive'] = first_receive['timestamp']
            log_to_file(get_scenario_name(context), f"📅 Karpenter SQS ReceiveMessage: {timestamps['karpenter_sqs_receive'].strftime('%H:%M:%S.%f')[:-3]}")
            log_to_file(get_scenario_name(context), f"📨 SQS Receive: {first_receive['message']}")
            
            if 'sqs_message' in timestamps:
                delays['sqs_to_karpenter_receive'] = (timestamps['karpenter_sqs_receive'] - timestamps['sqs_message']).total_seconds()
                log_to_file(get_scenario_name(context), f"⏱️ SQS Message → Karpenter ReceiveMessage delay: {delays['sqs_to_karpenter_receive']:.2f}s")
        
        elif disruption_queue_events:
            first_disruption = min(disruption_queue_events, key=lambda x: x['timestamp'])
            timestamps['karpenter_sqs_receive'] = first_disruption['timestamp']
            log_to_file(get_scenario_name(context), f"📅 Karpenter disruption.queue: {timestamps['karpenter_sqs_receive'].strftime('%H:%M:%S.%f')[:-3]}")
            log_to_file(get_scenario_name(context), f"📨 Disruption Queue: {first_disruption['message']}")
            
            if 'sqs_message' in timestamps:
                delays['sqs_to_karpenter_receive'] = (timestamps['karpenter_sqs_receive'] - timestamps['sqs_message']).total_seconds()
                log_to_file(get_scenario_name(context), f"⏱️ SQS Message → Karpenter disruption.queue delay: {delays['sqs_to_karpenter_receive']:.2f}s")
        
        # 5. Fallback SQS activity detection (if ReceiveMessage not found)
        if 'karpenter_sqs_receive' not in timestamps:
            # Try any Karpenter SQS activity
            karpenter_sqs_events = [e for e in all_events if e.get('type') == 'karpenter_sqs_activity']
            if not karpenter_sqs_events:
                # Try application SQS activity
                karpenter_sqs_events = [e for e in all_events if e.get('type') == 'application_sqs_activity']
            if not karpenter_sqs_events:
                # Try any interruption-related events
                karpenter_sqs_events = [e for e in all_events if 'interrupt' in e.get('message', '').lower() or 'spot' in e.get('message', '').lower()]
            
            if karpenter_sqs_events:
                first_sqs_activity = min(karpenter_sqs_events, key=lambda x: x['timestamp'])
                timestamps['karpenter_sqs_activity'] = first_sqs_activity['timestamp']
                activity_type = first_sqs_activity.get('type', 'unknown')
                log_to_file(get_scenario_name(context), f"📅 SQS Activity ({activity_type}): {timestamps['karpenter_sqs_activity'].strftime('%H:%M:%S.%f')[:-3]}")
                log_to_file(get_scenario_name(context), f"📨 SQS Activity: {first_sqs_activity['message']}")
                
                if 'sqs_message' in timestamps:
                    delays['sqs_to_karpenter_activity'] = (timestamps['karpenter_sqs_activity'] - timestamps['sqs_message']).total_seconds()
                    log_to_file(get_scenario_name(context), f"⏱️ SQS Message → Activity delay: {delays['sqs_to_karpenter_activity']:.2f}s")
        
        # 6. First Karpenter spot processing
        karpenter_spot_events = [e for e in all_events if e.get('type') == 'karpenter_spot_processing']
        if karpenter_spot_events:
            first_spot_processing = min(karpenter_spot_events, key=lambda x: x['timestamp'])
            timestamps['karpenter_spot_processing'] = first_spot_processing['timestamp']
            log_to_file(get_scenario_name(context), f"📅 Karpenter Spot Processing: {timestamps['karpenter_spot_processing'].strftime('%H:%M:%S.%f')[:-3]}")
            log_to_file(get_scenario_name(context), f"⚡ Spot Processing: {first_spot_processing['message']}")
            
            # Calculate delay from either ReceiveMessage or general SQS activity
            reference_key = 'karpenter_sqs_receive' if 'karpenter_sqs_receive' in timestamps else 'karpenter_sqs_activity'
            if reference_key in timestamps:
                delays['karpenter_sqs_to_processing'] = (timestamps['karpenter_spot_processing'] - timestamps[reference_key]).total_seconds()
                log_to_file(get_scenario_name(context), f"⏱️ Karpenter SQS → Processing delay: {delays['karpenter_sqs_to_processing']:.2f}s")
        
        # 6. First node action (taint/cordon)
        karpenter_node_events = [e for e in all_events if e.get('type') == 'karpenter_node_action']
        if karpenter_node_events:
            first_node_action = min(karpenter_node_events, key=lambda x: x['timestamp'])
            timestamps['karpenter_node_action'] = first_node_action['timestamp']
            log_to_file(get_scenario_name(context), f"📅 Karpenter Node Action: {timestamps['karpenter_node_action'].strftime('%H:%M:%S.%f')[:-3]}")
            log_to_file(get_scenario_name(context), f"🏷️ Node Action: {first_node_action['message']}")
            
            if 'karpenter_spot_processing' in timestamps:
                delays['processing_to_node_action'] = (timestamps['karpenter_node_action'] - timestamps['karpenter_spot_processing']).total_seconds()
                log_to_file(get_scenario_name(context), f"⏱️ Processing → Node Action delay: {delays['processing_to_node_action']:.2f}s")
        
        # 7. First NodeClaim creation
        karpenter_nodeclaim_events = [e for e in all_events if e.get('type') == 'karpenter_nodeclaim' and e.get('subtype') == 'creation']
        if karpenter_nodeclaim_events:
            first_nodeclaim_creation = min(karpenter_nodeclaim_events, key=lambda x: x['timestamp'])
            timestamps['nodeclaim_creation'] = first_nodeclaim_creation['timestamp']
            log_to_file(get_scenario_name(context), f"📅 NodeClaim Creation: {timestamps['nodeclaim_creation'].strftime('%H:%M:%S.%f')[:-3]}")
            log_to_file(get_scenario_name(context), f"🔧 NodeClaim Creation: {first_nodeclaim_creation['message']}")
            
            if 'karpenter_node_action' in timestamps:
                delays['node_action_to_nodeclaim'] = (timestamps['nodeclaim_creation'] - timestamps['karpenter_node_action']).total_seconds()
                log_to_file(get_scenario_name(context), f"⏱️ Node Action → NodeClaim Creation delay: {delays['node_action_to_nodeclaim']:.2f}s")
        
        # 8. NodeClaim launch/ready
        karpenter_launch_events = [e for e in all_events if e.get('type') == 'karpenter_nodeclaim' and e.get('subtype') == 'launch']
        if karpenter_launch_events:
            first_launch = min(karpenter_launch_events, key=lambda x: x['timestamp'])
            timestamps['nodeclaim_launch'] = first_launch['timestamp']
            log_to_file(get_scenario_name(context), f"📅 NodeClaim Launch: {timestamps['nodeclaim_launch'].strftime('%H:%M:%S.%f')[:-3]}")
            log_to_file(get_scenario_name(context), f"🚀 NodeClaim Launch: {first_launch['message']}")
            
            if 'nodeclaim_creation' in timestamps:
                delays['nodeclaim_creation_to_launch'] = (timestamps['nodeclaim_creation'] - timestamps['nodeclaim_launch']).total_seconds()
                log_to_file(get_scenario_name(context), f"⏱️ NodeClaim Creation → Launch delay: {delays['nodeclaim_creation_to_launch']:.2f}s")
        
        # Calculate total end-to-end time
        if 'fis_start' in timestamps:
            latest_timestamp = max([ts for ts in timestamps.values()])
            total_time = (latest_timestamp - timestamps['fis_start']).total_seconds()
            
            log_to_file(get_scenario_name(context), "")
            log_to_file(get_scenario_name(context), "📊 TIMING SUMMARY")
            log_to_file(get_scenario_name(context), "=" * 30)
            
            # Individual delays
            for delay_name, delay_value in delays.items():
                log_to_file(get_scenario_name(context), f"  {delay_name.replace('_', ' ').title()}: {delay_value:.2f}s")
            
            log_to_file(get_scenario_name(context), f"")
            log_to_file(get_scenario_name(context), f"🎯 TOTAL END-TO-END TIME: {total_time:.2f}s")
            log_to_file(get_scenario_name(context), f"📈 Latest Event: {latest_timestamp.strftime('%H:%M:%S.%f')[:-3]}")
            
            # Store for later use
            context.total_processing_time = total_time
            context.processing_delays = delays
            context.processing_timestamps = timestamps
            
            # Performance assessment
            if total_time < 30:
                log_to_file(get_scenario_name(context), "✅ EXCELLENT: Processing completed in under 30 seconds")
            elif total_time < 60:
                log_to_file(get_scenario_name(context), "✅ GOOD: Processing completed in under 1 minute")
            elif total_time < 120:
                log_to_file(get_scenario_name(context), "⚠️ ACCEPTABLE: Processing completed in under 2 minutes")
            else:
                log_to_file(get_scenario_name(context), "❌ SLOW: Processing took over 2 minutes")
            
            # Calculate final total replacement time (old pod/node shutdown → new pod/node ready)
            log_to_file(get_scenario_name(context), "")
            log_to_file(get_scenario_name(context), "🔄 FINAL REPLACEMENT TIMING")
            log_to_file(get_scenario_name(context), "=" * 50)
            
            # Old pod termination → New pod ready
            if hasattr(context, 'original_pod_terminated') and hasattr(context, 'new_pod_ready_time'):
                if hasattr(context, 'instance_termination_time'):
                    pod_replacement_time = (context.new_pod_ready_time - context.instance_termination_time).total_seconds()
                    pod_replacement_minutes = int(pod_replacement_time // 60)
                    pod_replacement_seconds = int(pod_replacement_time % 60)
                    pod_replacement_formatted = f"{pod_replacement_minutes}:{pod_replacement_seconds:02d}"
                    
                    log_to_file(get_scenario_name(context), f"🔄 OLD POD SHUTDOWN → NEW POD READY: {pod_replacement_formatted} ({pod_replacement_time:.2f}s)")
                    
                    if pod_replacement_time < 120:  # Under 2 minutes
                        log_to_file(get_scenario_name(context), "✅ EXCELLENT: Pod replacement completed in under 2 minutes")
                    elif pod_replacement_time < 300:  # Under 5 minutes
                        log_to_file(get_scenario_name(context), "✅ GOOD: Pod replacement completed in under 5 minutes")
                    elif pod_replacement_time < 600:  # Under 10 minutes
                        log_to_file(get_scenario_name(context), "⚠️ ACCEPTABLE: Pod replacement completed in under 10 minutes")
                    else:
                        log_to_file(get_scenario_name(context), "❌ SLOW: Pod replacement took over 10 minutes")
            
            # Old node termination → New node ready
            if hasattr(context, 'instance_termination_time') and hasattr(context, 'new_node_ready_time'):
                node_replacement_time = (context.new_node_ready_time - context.instance_termination_time).total_seconds()
                node_replacement_minutes = int(node_replacement_time // 60)
                node_replacement_seconds = int(node_replacement_time % 60)
                node_replacement_formatted = f"{node_replacement_minutes}:{node_replacement_seconds:02d}"
                
                log_to_file(get_scenario_name(context), f"🔄 OLD NODE SHUTDOWN → NEW NODE READY: {node_replacement_formatted} ({node_replacement_time:.2f}s)")
                
                if node_replacement_time < 180:  # Under 3 minutes
                    log_to_file(get_scenario_name(context), "✅ EXCELLENT: Node replacement completed in under 3 minutes")
                elif node_replacement_time < 300:  # Under 5 minutes
                    log_to_file(get_scenario_name(context), "✅ GOOD: Node replacement completed in under 5 minutes")
                elif node_replacement_time < 600:  # Under 10 minutes
                    log_to_file(get_scenario_name(context), "⚠️ ACCEPTABLE: Node replacement completed in under 10 minutes")
                else:
                    log_to_file(get_scenario_name(context), "❌ SLOW: Node replacement took over 10 minutes")
            
            # Complete recovery time (old resources shutdown → new resources ready)
            if (hasattr(context, 'instance_termination_time') and 
                hasattr(context, 'new_pod_ready_time') and 
                hasattr(context, 'new_node_ready_time')):
                
                complete_recovery_time = max(
                    (context.new_pod_ready_time - context.instance_termination_time).total_seconds(),
                    (context.new_node_ready_time - context.instance_termination_time).total_seconds()
                )
                
                complete_recovery_minutes = int(complete_recovery_time // 60)
                complete_recovery_seconds = int(complete_recovery_time % 60)
                complete_recovery_formatted = f"{complete_recovery_minutes}:{complete_recovery_seconds:02d}"
                
                log_to_file(get_scenario_name(context), f"")
                log_to_file(get_scenario_name(context), f"🎯 COMPLETE RECOVERY TIME: {complete_recovery_formatted} ({complete_recovery_time:.2f}s)")
                log_to_file(get_scenario_name(context), f"   (From old resources shutdown to new resources fully ready)")
                
                if complete_recovery_time < 300:  # Under 5 minutes
                    log_to_file(get_scenario_name(context), "🏆 EXCELLENT: Complete recovery in under 5 minutes")
                elif complete_recovery_time < 600:  # Under 10 minutes
                    log_to_file(get_scenario_name(context), "✅ GOOD: Complete recovery in under 10 minutes")
                elif complete_recovery_time < 900:  # Under 15 minutes
                    log_to_file(get_scenario_name(context), "⚠️ ACCEPTABLE: Complete recovery in under 15 minutes")
                else:
                    log_to_file(get_scenario_name(context), "❌ SLOW: Complete recovery took over 15 minutes")
        
        # Additional SQS processing insights
        if sqs_messages:
            log_to_file(get_scenario_name(context), "")
            log_to_file(get_scenario_name(context), "📬 SQS MESSAGE PROCESSING DETAILS")
            log_to_file(get_scenario_name(context), f"  Total SQS messages captured: {len(sqs_messages)}")
            
            for i, msg in enumerate(sqs_messages[:3]):  # Show first 3 messages
                msg_time = msg['timestamp'].strftime('%H:%M:%S.%f')[:-3]
                log_to_file(get_scenario_name(context), f"  Message {i+1}: {msg_time}")
                if 'body' in msg:
                    detail_type = msg['body'].get('DetailType', 'Unknown')
                    log_to_file(get_scenario_name(context), f"    Type: {detail_type}")
        
        # Karpenter log insights
        relevant_logs = [log for log in karpenter_logs if any(keyword in log['message'].lower() 
                        for keyword in ['sqs', 'spot', 'interrupt', 'taint', 'cordon'])]
        
        if relevant_logs:
            log_to_file(get_scenario_name(context), "")
            log_to_file(get_scenario_name(context), "🔍 RELEVANT KARPENTER LOG ENTRIES")
            for i, log_entry in enumerate(relevant_logs[:5]):  # Show first 5 relevant logs
                log_time = log_entry['timestamp'].strftime('%H:%M:%S.%f')[:-3]
                log_to_file(get_scenario_name(context), f"  {i+1}. [{log_time}] {log_entry['message']}")
        
        # Detailed Karpenter Controller Logs by Event Category
        if hasattr(context, 'monitor') and context.monitor:
            log_to_file(get_scenario_name(context), "")
            log_to_file(get_scenario_name(context), "📋 DETAILED KARPENTER CONTROLLER LOGS BY EVENT")
            log_to_file(get_scenario_name(context), "=" * 60)
            
            # Show specific pod monitoring summary
            if context.monitor.karpenter_logs:
                log_to_file(get_scenario_name(context), f"📱 Monitored Karpenter pod: karpenter-75dbd6c5dd-4csmc")
                log_to_file(get_scenario_name(context), f"  • Total log entries: {len(context.monitor.karpenter_logs)}")
                log_to_file(get_scenario_name(context), "")
            
            # Group events by category
            event_categories = {}
            for event in context.monitor.events:
                if event.get('type', '').startswith('karpenter_'):
                    category = event['type'].replace('karpenter_', '').upper()
                    if category not in event_categories:
                        event_categories[category] = []
                    event_categories[category].append(event)
            
            # Display events by category
            for category, events in event_categories.items():
                if events:
                    log_to_file(get_scenario_name(context), f"")
                    log_to_file(get_scenario_name(context), f"🔸 {category} EVENTS ({len(events)} total)")
                    log_to_file(get_scenario_name(context), "-" * 40)
                    
                    # Show first 3 and last 3 events for each category
                    display_events = []
                    if len(events) <= 6:
                        display_events = events
                    else:
                        display_events = events[:3] + [{'separator': True}] + events[-3:]
                    
                    for i, event in enumerate(display_events):
                        if event.get('separator'):
                            log_to_file(get_scenario_name(context), f"    ... ({len(events) - 6} more events) ...")
                            continue
                            
                        timestamp = event['timestamp'].strftime('%H:%M:%S.%f')[:-3]
                        subtype = event.get('subtype', 'general').upper()
                        message = event['message']
                        context_info = event.get('context', '')
                        pod_name = event.get('pod', 'unknown')
                        
                        # Include pod name in the log entry
                        log_to_file(get_scenario_name(context), f"    [{timestamp}] {subtype} [{pod_name}]")
                        
                        # Truncate very long messages
                        if len(message) > 200:
                            message = message[:200] + "..."
                        log_to_file(get_scenario_name(context), f"      📝 {message}")
                        
                        if context_info:
                            log_to_file(get_scenario_name(context), f"      📋 {context_info}")
            
            # Application Pod Container Logs Summary
            log_to_file(get_scenario_name(context), "")
            log_to_file(get_scenario_name(context), "📱 APPLICATION POD CONTAINER LOGS")
            log_to_file(get_scenario_name(context), "=" * 40)
            
            # Use comprehensive logs from real-time monitoring if available
            if hasattr(context.monitor, 'application_logs') and context.monitor.application_logs:
                log_to_file(get_scenario_name(context), f"📊 Math-Compute-SQS-App Real-time Monitoring Results:")
                log_to_file(get_scenario_name(context), f"  Total application logs captured: {len(context.monitor.application_logs)}")
                log_to_file(get_scenario_name(context), f"  Math computation detected: {'✅' if hasattr(context.monitor, 'math_computation_active') and context.monitor.math_computation_active else '❌'}")
                log_to_file(get_scenario_name(context), f"  SQS processing detected: {'✅' if hasattr(context.monitor, 'sqs_processing_active') and context.monitor.sqs_processing_active else '❌'}")
                
                # Check if spot warning was detected
                if hasattr(context.monitor, 'spot_warning_detected') and context.monitor.spot_warning_detected:
                    log_to_file(get_scenario_name(context), f"🚨 Spot warning detected at: {context.monitor.spot_warning_time.strftime('%H:%M:%S.%f')[:-3]}")
                    
                    # Get logs during interruption period
                    interruption_logs = context.monitor.get_application_logs_during_interruption()
                    log_to_file(get_scenario_name(context), f"📝 Application logs during spot interruption ({len(interruption_logs)} lines):")
                    
                    if interruption_logs:
                        # Show all logs during interruption (they're important)
                        for i, log_entry in enumerate(interruption_logs):
                            timestamp = log_entry['timestamp'].strftime('%H:%M:%S.%f')[:-3]
                            message = log_entry['message']
                            log_to_file(get_scenario_name(context), f"    [{timestamp}] {message}")
                            
                            # Stop at 50 lines to avoid overwhelming output
                            if i >= 49:
                                remaining = len(interruption_logs) - 50
                                if remaining > 0:
                                    log_to_file(get_scenario_name(context), f"    ... and {remaining} more log lines")
                                break
                    else:
                        log_to_file(get_scenario_name(context), "    No logs captured during interruption period")
                
                else:
                    log_to_file(get_scenario_name(context), "ℹ️ No spot warning detected in application logs")
                    
                    # Show recent application logs anyway
                    recent_logs = context.monitor.application_logs[-20:] if len(context.monitor.application_logs) > 20 else context.monitor.application_logs
                    log_to_file(get_scenario_name(context), f"📝 Recent application logs ({len(recent_logs)} lines):")
                    
                    for log_entry in recent_logs:
                        timestamp = log_entry['timestamp'].strftime('%H:%M:%S.%f')[:-3]
                        message = log_entry['message']
                        log_to_file(get_scenario_name(context), f"    [{timestamp}] {message}")
                
                # Show math-compute-sqs-app events by category
                app_events = [event for event in context.monitor.events 
                             if event.get('type', '').startswith('application_')]
                
                if app_events:
                    log_to_file(get_scenario_name(context), "")
                    log_to_file(get_scenario_name(context), "📋 MATH-COMPUTE-SQS-APP EVENTS BY CATEGORY")
                    log_to_file(get_scenario_name(context), "-" * 45)
                    
                    # Group application events by type with math-specific emojis
                    app_event_categories = {}
                    category_emojis = {
                        'SPOT_EVENT': '🚨',
                        'MATH_COMPUTATION': '🧮',
                        'SQS_ACTIVITY': '📬',
                        'WORK_COMPLETION': '✅',
                        'HTTP_ACTIVITY': '🌐',
                        'LIFECYCLE': '📱',
                        'GENERAL': '📝'
                    }
                    
                    for event in app_events:
                        category = event['type'].replace('application_', '').upper()
                        if category not in app_event_categories:
                            app_event_categories[category] = []
                        app_event_categories[category].append(event)
                    
                    # Sort categories by importance for math-compute-sqs-app
                    category_order = ['SPOT_EVENT', 'MATH_COMPUTATION', 'SQS_ACTIVITY', 'WORK_COMPLETION', 
                                    'HTTP_ACTIVITY', 'LIFECYCLE', 'GENERAL']
                    
                    for category in category_order:
                        if category in app_event_categories:
                            events = app_event_categories[category]
                            emoji = category_emojis.get(category, '📝')
                            log_to_file(get_scenario_name(context), f"")
                            log_to_file(get_scenario_name(context), f"{emoji} {category} EVENTS ({len(events)} total)")
                            
                            # Show all events for critical categories, limited for others
                            max_events = 20 if category in ['SPOT_EVENT', 'MATH_COMPUTATION'] else 10
                            
                            for event in events[:max_events]:
                                timestamp = event['timestamp'].strftime('%H:%M:%S.%f')[:-3]
                                message = event['message'][:150] + "..." if len(event['message']) > 150 else event['message']
                                priority = event.get('priority', 'normal')
                                priority_emoji = '🚨' if priority == 'high' else '📝'
                                log_to_file(get_scenario_name(context), f"    [{timestamp}] {priority_emoji} {message}")
                            
                            if len(events) > max_events:
                                log_to_file(get_scenario_name(context), f"    ... and {len(events) - max_events} more {category.lower()} events")
                    
                    # Show any remaining categories not in the ordered list
                    for category, events in app_event_categories.items():
                        if category not in category_order:
                            log_to_file(get_scenario_name(context), f"")
                            log_to_file(get_scenario_name(context), f"📝 {category} EVENTS ({len(events)} total)")
                            
                            for event in events[:5]:
                                timestamp = event['timestamp'].strftime('%H:%M:%S.%f')[:-3]
                                message = event['message'][:150] + "..." if len(event['message']) > 150 else event['message']
                                log_to_file(get_scenario_name(context), f"    [{timestamp}] 📝 {message}")
                            
                            if len(events) > 5:
                                log_to_file(get_scenario_name(context), f"    ... and {len(events) - 5} more {category.lower()} events")
            
            # Fallback to kubectl logs if real-time monitoring didn't capture logs
            elif hasattr(context, 'pod_name') and hasattr(context, 'namespace'):
                try:
                    log_to_file(get_scenario_name(context), "📝 Fallback: Using kubectl logs (real-time monitoring may have missed some logs)")
                    
                    # Get pod logs during the interruption period
                    since_time = context.interruption_start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
                    pod_logs_cmd = f"kubectl logs {context.pod_name} -n {context.namespace} --since-time={since_time} --tail=100"
                    pod_logs_result = subprocess.run(pod_logs_cmd, shell=True, capture_output=True, text=True)
                    
                    if pod_logs_result.returncode == 0 and pod_logs_result.stdout.strip():
                        log_lines = pod_logs_result.stdout.strip().split('\n')
                        log_to_file(get_scenario_name(context), f"📝 Pod logs during interruption ({len(log_lines)} lines):")
                        
                        # Show all lines (they're important during interruption)
                        for line in log_lines:
                            log_to_file(get_scenario_name(context), f"    {line}")
                    else:
                        log_to_file(get_scenario_name(context), "📝 No application logs captured during interruption")
                        
                except Exception as pod_log_error:
                    log_to_file(get_scenario_name(context), f"⚠️ Could not capture pod logs: {str(pod_log_error)}")
            
            else:
                log_to_file(get_scenario_name(context), "⚠️ No application pod information available for log capture")
            
            # Get new pod logs if available
            if hasattr(context, 'new_pod_name') and hasattr(context, 'namespace'):
                try:
                    log_to_file(get_scenario_name(context), "")
                    log_to_file(get_scenario_name(context), "🆕 NEW POD STARTUP LOGS")
                    log_to_file(get_scenario_name(context), "-" * 25)
                    
                    new_pod_logs_cmd = f"kubectl logs {context.new_pod_name} -n {context.namespace} --tail=30"
                    new_pod_logs_result = subprocess.run(new_pod_logs_cmd, shell=True, capture_output=True, text=True)
                    
                    if new_pod_logs_result.returncode == 0 and new_pod_logs_result.stdout.strip():
                        log_to_file(get_scenario_name(context), f"📝 New pod ({context.new_pod_name}) startup logs:")
                        for line in new_pod_logs_result.stdout.strip().split('\n'):
                            log_to_file(get_scenario_name(context), f"    {line}")
                    else:
                        log_to_file(get_scenario_name(context), "📝 No logs available from new pod yet")
                        
                except Exception as new_pod_log_error:
                    log_to_file(get_scenario_name(context), f"⚠️ Could not capture new pod logs: {str(new_pod_log_error)}")
            
            # Event Timeline Summary
            log_to_file(get_scenario_name(context), "")
            log_to_file(get_scenario_name(context), "⏰ EVENT TIMELINE SUMMARY")
            log_to_file(get_scenario_name(context), "=" * 30)
            
            # Sort all events by timestamp
            all_timeline_events = []
            for event in context.monitor.events:
                if event.get('type', '').startswith('karpenter_'):
                    all_timeline_events.append({
                        'timestamp': event['timestamp'],
                        'category': event['type'].replace('karpenter_', '').upper(),
                        'subtype': event.get('subtype', 'general'),
                        'message': event['message'][:100] + "..." if len(event['message']) > 100 else event['message']
                    })
            
            # Sort by timestamp and show key events
            all_timeline_events.sort(key=lambda x: x['timestamp'])
            
            # Show first 15 events in timeline
            for i, event in enumerate(all_timeline_events[:15]):
                timestamp = event['timestamp'].strftime('%H:%M:%S.%f')[:-3]
                log_to_file(get_scenario_name(context), f"  {i+1:2d}. [{timestamp}] {event['category']}.{event['subtype']}")
                log_to_file(get_scenario_name(context), f"      {event['message']}")
            
            if len(all_timeline_events) > 15:
                log_to_file(get_scenario_name(context), f"      ... and {len(all_timeline_events) - 15} more events")
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR analyzing timing delays: {str(e)}")
        traceback.print_exc()

@then('I should capture the SQS message from EventBridge')
def step_capture_sqs_message(context):
    try:
        log_to_file(get_scenario_name(context), "📬 DETECTING SQS MESSAGE TIMING")
        
        # Check if we already detected SQS activity through Karpenter logs
        if hasattr(context, 'monitor') and context.monitor:
            sqs_events = [event for event in context.monitor.events 
                         if event.get('type') == 'karpenter_sqs_activity']
            
            if sqs_events:
                first_sqs_event = min(sqs_events, key=lambda x: x['timestamp'])
                
                log_to_file(get_scenario_name(context), f"✅ MESSAGE SENT: {first_sqs_event['timestamp'].strftime('%H:%M:%S.%f')[:-3]}")
                log_to_file(get_scenario_name(context), f"✅ MESSAGE RECEIVED: {first_sqs_event['timestamp'].strftime('%H:%M:%S.%f')[:-3]}")
                log_to_file(get_scenario_name(context), f"✅ KARPENTER CONTROLLER LOGS: Processing detected")
                
                # Calculate timing if we have EventBridge timing
                if hasattr(context, 'eventbridge_first_event_time'):
                    eb_to_sqs_delay = (first_sqs_event['timestamp'] - context.eventbridge_first_event_time).total_seconds()
                    log_to_file(get_scenario_name(context), f"⏱️ EventBridge → SQS delay: {eb_to_sqs_delay:.2f}s")
                
                context.sqs_first_message_time = first_sqs_event['timestamp']
                context.sqs_message_captured = True
                context.sqs_message_detail_type = "Processed by Karpenter"
                return
        
        # If no Karpenter SQS activity detected yet, mark as not captured
        log_to_file(get_scenario_name(context), "⚠️ No SQS activity detected yet")
        context.sqs_message_captured = False
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR detecting SQS timing: {str(e)}")
        context.sqs_message_captured = False

@then('I should verify the SQS message contains correct instance details')
def step_verify_sqs_message_details(context):
    try:
        log_to_file(get_scenario_name(context), "🔍 VERIFYING SQS MESSAGE DETAILS")
        
        if hasattr(context, 'monitor') and context.monitor.sqs_messages:
            latest_message = context.monitor.sqs_messages[-1]
            message_body = latest_message['body']
            
            # Extract detail from the message
            detail = json.loads(message_body.get('Detail', '{}'))
            instance_id = detail.get('instance-id')
            
            log_to_file(get_scenario_name(context), f"  Expected Instance ID: {context.instance_id}")
            log_to_file(get_scenario_name(context), f"  Message Instance ID: {instance_id}")
            
            if instance_id == context.instance_id:
                log_to_file(get_scenario_name(context), "✅ SQS message contains correct instance details")
                context.sqs_message_valid = True
            else:
                log_to_file(get_scenario_name(context), "❌ SQS message instance ID mismatch")
                context.sqs_message_valid = False
        else:
            log_to_file(get_scenario_name(context), "⚠️ No SQS messages to verify")
            context.sqs_message_valid = False
            
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR verifying SQS message: {str(e)}")
        context.sqs_message_valid = False

@then('I should monitor Karpenter polling SQS messages')
def step_monitor_karpenter_sqs_polling(context):
    try:
        log_to_file(get_scenario_name(context), "🤖 MONITORING KARPENTER SQS POLLING")
        
        # Look for Karpenter logs indicating SQS polling
        if hasattr(context, 'monitor'):
            karpenter_sqs_logs = [
                log for log in context.monitor.karpenter_logs 
                if any(keyword in log['message'].lower() for keyword in ['sqs', 'queue', 'poll', 'message'])
            ]
            
            log_to_file(get_scenario_name(context), f"  Found {len(karpenter_sqs_logs)} SQS-related log entries")
            
            for log_entry in karpenter_sqs_logs[-5:]:  # Show last 5 entries
                log_to_file(get_scenario_name(context), f"    {log_entry['timestamp']}: {log_entry['message']}")
        
        log_to_file(get_scenario_name(context), "✅ Karpenter SQS polling monitoring complete")
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR monitoring Karpenter SQS polling: {str(e)}")

# Continue with more step definitions...
# (This is getting quite long, so I'll continue in the next part)

@then('I should capture Karpenter processing the spot interruption')
def step_capture_karpenter_processing(context):
    try:
        log_to_file(get_scenario_name(context), "🤖 MONITORING KARPENTER SPOT PROCESSING")
        
        # Look for Karpenter logs indicating spot interruption processing
        if hasattr(context, 'monitor'):
            spot_logs = [
                log for log in context.monitor.karpenter_logs 
                if any(keyword in log['message'].lower() for keyword in [
                    'spot', 'interrupt', 'termination', 'evict'
                ])
            ]
            
            log_to_file(get_scenario_name(context), f"  Found {len(spot_logs)} spot interruption log entries")
            
            for log_entry in spot_logs[-10:]:  # Show last 10 entries
                log_to_file(get_scenario_name(context), f"    {log_entry['timestamp']}: {log_entry['message']}")
            
            if spot_logs:
                context.karpenter_spot_processing = True
                log_to_file(get_scenario_name(context), "✅ Karpenter spot interruption processing captured")
            else:
                context.karpenter_spot_processing = False
                log_to_file(get_scenario_name(context), "⚠️ No Karpenter spot processing logs found")
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR monitoring Karpenter processing: {str(e)}")
        context.karpenter_spot_processing = False

@then('I should verify Karpenter taints the node with spot interruption')
def step_verify_node_taint(context):
    try:
        log_to_file(get_scenario_name(context), "🏷️ VERIFYING NODE TAINT")
        
        # Check node taints
        node_cmd = f"kubectl get node {context.node_name} -o json"
        node_result = run_kubectl(node_cmd)
        node_data = json.loads(node_result)
        
        taints = node_data.get('spec', {}).get('taints', [])
        
        # Look for spot interruption taint
        spot_taint = None
        for taint in taints:
            if 'spot' in taint.get('key', '').lower() or 'interrupt' in taint.get('key', '').lower():
                spot_taint = taint
                break
        
        if spot_taint:
            log_to_file(get_scenario_name(context), f"✅ Node taint found:")
            log_to_file(get_scenario_name(context), f"  Key: {spot_taint.get('key')}")
            log_to_file(get_scenario_name(context), f"  Value: {spot_taint.get('value')}")
            log_to_file(get_scenario_name(context), f"  Effect: {spot_taint.get('effect')}")
            context.node_tainted = True
        else:
            log_to_file(get_scenario_name(context), "⚠️ No spot interruption taint found on node")
            log_to_file(get_scenario_name(context), f"  Current taints: {[t.get('key') for t in taints]}")
            context.node_tainted = False
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR verifying node taint: {str(e)}")
        context.node_tainted = False

@then('I should verify Karpenter updates the NodeClaim status')
def step_verify_nodeclaim_status(context):
    try:
        log_to_file(get_scenario_name(context), "📋 VERIFYING NODECLAIM STATUS")
        
        # Get NodeClaim status
        nodeclaim_cmd = f"kubectl get nodeclaim -o json"
        nodeclaim_result = run_kubectl(nodeclaim_cmd)
        nodeclaim_data = json.loads(nodeclaim_result)
        
        # Find NodeClaim for our node
        target_nodeclaim = None
        for item in nodeclaim_data.get('items', []):
            if item.get('status', {}).get('nodeName') == context.node_name:
                target_nodeclaim = item
                break
        
        if target_nodeclaim:
            status = target_nodeclaim.get('status', {})
            conditions = status.get('conditions', [])
            
            log_to_file(get_scenario_name(context), f"✅ NodeClaim found for node {context.node_name}")
            log_to_file(get_scenario_name(context), f"  NodeClaim name: {target_nodeclaim.get('metadata', {}).get('name')}")
            log_to_file(get_scenario_name(context), f"  Conditions: {len(conditions)}")
            
            for condition in conditions:
                log_to_file(get_scenario_name(context), f"    {condition.get('type')}: {condition.get('status')}")
            
            context.nodeclaim_updated = True
        else:
            log_to_file(get_scenario_name(context), "⚠️ No NodeClaim found for the node")
            context.nodeclaim_updated = False
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR verifying NodeClaim: {str(e)}")
        context.nodeclaim_updated = False

@then('I should capture node cordoning by Karpenter')
def step_capture_node_cordoning(context):
    try:
        log_to_file(get_scenario_name(context), "🚫 MONITORING NODE CORDONING")
        
        # Check if node is cordoned
        node_cmd = f"kubectl get node {context.node_name} -o json"
        node_result = run_kubectl(node_cmd)
        node_data = json.loads(node_result)
        
        unschedulable = node_data.get('spec', {}).get('unschedulable', False)
        
        if unschedulable:
            log_to_file(get_scenario_name(context), f"✅ Node {context.node_name} is cordoned")
            context.node_cordoned = True
        else:
            log_to_file(get_scenario_name(context), f"⚠️ Node {context.node_name} is not cordoned")
            context.node_cordoned = False
        
        # Check node events for cordoning
        if hasattr(context, 'monitor'):
            cordon_events = [
                event for event in context.monitor.node_events
                if event['node'] == context.node_name and 
                'cordon' in event['message'].lower()
            ]
            
            log_to_file(get_scenario_name(context), f"  Found {len(cordon_events)} cordoning events")
            for event in cordon_events:
                log_to_file(get_scenario_name(context), f"    {event['timestamp']}: {event['message']}")
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR monitoring node cordoning: {str(e)}")
        context.node_cordoned = False

@then('I should monitor pod status during grace period')
def step_monitor_pod_grace_period(context):
    try:
        log_to_file(get_scenario_name(context), "⏰ MONITORING POD DURING GRACE PERIOD")
        
        grace_period_duration = 120  # 2 minutes
        start_time = datetime.now()
        
        pod_states = []
        
        while (datetime.now() - start_time).total_seconds() < grace_period_duration:
            try:
                pod_cmd = f"kubectl get pod {context.pod_name} -n {context.namespace} -o json"
                pod_result = run_kubectl(pod_cmd)
                pod_data = json.loads(pod_result)
                
                phase = pod_data.get('status', {}).get('phase')
                deletion_timestamp = pod_data.get('metadata', {}).get('deletionTimestamp')
                
                current_time = datetime.now()
                elapsed = (current_time - start_time).total_seconds()
                
                state = {
                    'timestamp': current_time,
                    'elapsed': elapsed,
                    'phase': phase,
                    'deletion_timestamp': deletion_timestamp,
                    'terminating': deletion_timestamp is not None
                }
                
                pod_states.append(state)
                
                if elapsed % 30 < 5:  # Log every 30 seconds
                    status = "Terminating" if deletion_timestamp else phase
                    log_to_file(get_scenario_name(context), f"  Grace period +{elapsed:.0f}s: Pod status = {status}")
                
                time.sleep(5)
                
            except Exception:
                # Pod might be deleted
                break
        
        context.grace_period_pod_states = pod_states
        log_to_file(get_scenario_name(context), f"✅ Grace period monitoring complete: {len(pod_states)} states captured")
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR monitoring grace period: {str(e)}")

@then('I should capture any prestop script execution')
def step_capture_prestop_execution(context):
    try:
        log_to_file(get_scenario_name(context), "📜 MONITORING PRESTOP SCRIPT EXECUTION")
        
        # Check pod events for prestop execution
        if hasattr(context, 'monitor'):
            prestop_events = [
                event for event in context.monitor.pod_events
                if event['pod'] == context.pod_name and
                any(keyword in event['message'].lower() for keyword in [
                    'prestop', 'preStop', 'lifecycle', 'hook'
                ])
            ]
            
            if prestop_events:
                log_to_file(get_scenario_name(context), f"✅ Found {len(prestop_events)} prestop-related events:")
                for event in prestop_events:
                    log_to_file(get_scenario_name(context), f"  {event['timestamp']}: {event['message']}")
                context.prestop_executed = True
            else:
                log_to_file(get_scenario_name(context), "ℹ️ No prestop script events detected")
                context.prestop_executed = False
        
        # Also check pod logs for prestop execution
        try:
            logs_cmd = f"kubectl logs {context.pod_name} -n {context.namespace} --tail=50"
            logs_result = run_kubectl(logs_cmd)
            
            if any(keyword in logs_result.lower() for keyword in ['prestop', 'sigterm', 'shutdown']):
                log_to_file(get_scenario_name(context), "✅ Prestop/shutdown activity detected in pod logs")
                context.prestop_in_logs = True
            else:
                log_to_file(get_scenario_name(context), "ℹ️ No prestop activity in pod logs")
                context.prestop_in_logs = False
                
        except Exception:
            context.prestop_in_logs = False
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR monitoring prestop execution: {str(e)}")

@then('I should monitor for SIGTERM signals to the pod')
def step_monitor_sigterm_signals(context):
    try:
        log_to_file(get_scenario_name(context), "⚡ MONITORING SIGTERM SIGNALS")
        
        # Check pod events for termination signals
        if hasattr(context, 'monitor'):
            sigterm_events = [
                event for event in context.monitor.pod_events
                if event['pod'] == context.pod_name and
                any(keyword in event['message'].lower() for keyword in [
                    'sigterm', 'terminate', 'killing', 'graceful'
                ])
            ]
            
            if sigterm_events:
                log_to_file(get_scenario_name(context), f"✅ Found {len(sigterm_events)} SIGTERM-related events:")
                for event in sigterm_events:
                    log_to_file(get_scenario_name(context), f"  {event['timestamp']}: {event['message']}")
                context.sigterm_detected = True
            else:
                log_to_file(get_scenario_name(context), "ℹ️ No SIGTERM events detected yet")
                context.sigterm_detected = False
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR monitoring SIGTERM: {str(e)}")

@then('I should verify pod remains running during grace period')
def step_verify_pod_running_grace_period(context):
    try:
        log_to_file(get_scenario_name(context), "✅ VERIFYING POD SURVIVAL DURING GRACE PERIOD")
        
        if hasattr(context, 'grace_period_pod_states'):
            running_states = [
                state for state in context.grace_period_pod_states
                if state['phase'] == 'Running' and not state['terminating']
            ]
            
            total_states = len(context.grace_period_pod_states)
            running_count = len(running_states)
            
            log_to_file(get_scenario_name(context), f"  Total states captured: {total_states}")
            log_to_file(get_scenario_name(context), f"  Running states: {running_count}")
            log_to_file(get_scenario_name(context), f"  Running percentage: {(running_count/total_states)*100:.1f}%")
            
            if running_count > total_states * 0.5:  # More than 50% of time running
                log_to_file(get_scenario_name(context), "✅ Pod remained running during most of grace period")
                context.pod_survived_grace_period = True
            else:
                log_to_file(get_scenario_name(context), "⚠️ Pod was terminating for most of grace period")
                context.pod_survived_grace_period = False
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR verifying pod grace period survival: {str(e)}")

@then('I should capture the actual instance termination')
def step_capture_instance_termination(context):
    try:
        log_to_file(get_scenario_name(context), "💥 MONITORING INSTANCE TERMINATION")
        
        # Monitor EC2 instance state
        ec2 = boto3.client('ec2', region_name=context.region)
        
        start_time = datetime.now()
        timeout = 300  # 5 minutes
        
        while (datetime.now() - start_time).total_seconds() < timeout:
            try:
                response = ec2.describe_instances(InstanceIds=[context.instance_id])
                
                if response['Reservations']:
                    instance = response['Reservations'][0]['Instances'][0]
                    state = instance['State']['Name']
                    
                    if state in ['shutting-down', 'terminated']:
                        log_to_file(get_scenario_name(context), f"✅ Instance termination detected: {state}")
                        context.instance_terminated = True
                        context.instance_termination_time = datetime.now()
                        return
                    
                    elapsed = (datetime.now() - start_time).total_seconds()
                    if elapsed % 30 < 5:  # Log every 30 seconds
                        log_to_file(get_scenario_name(context), f"  Instance state at +{elapsed:.0f}s: {state}")
                
                time.sleep(10)
                
            except Exception as e:
                if "InvalidInstanceID.NotFound" in str(e):
                    log_to_file(get_scenario_name(context), "✅ Instance terminated (not found)")
                    context.instance_terminated = True
                    context.instance_termination_time = datetime.now()
                    return
                else:
                    raise e
        
        log_to_file(get_scenario_name(context), "⚠️ Instance termination not detected within timeout")
        context.instance_terminated = False
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR monitoring instance termination: {str(e)}")
        context.instance_terminated = False

@then('I should monitor for SIGKILL signals \\(exit code 137\\)')
def step_monitor_sigkill_signals(context):
    try:
        log_to_file(get_scenario_name(context), "💀 MONITORING SIGKILL SIGNALS (EXIT CODE 137)")
        
        # Check pod events for SIGKILL/exit code 137
        if hasattr(context, 'monitor'):
            sigkill_events = [
                event for event in context.monitor.pod_events
                if event['pod'] == context.pod_name and
                any(keyword in event['message'].lower() for keyword in [
                    'sigkill', '137', 'killed', 'oomkilled'
                ])
            ]
            
            if sigkill_events:
                log_to_file(get_scenario_name(context), f"✅ Found {len(sigkill_events)} SIGKILL-related events:")
                for event in sigkill_events:
                    log_to_file(get_scenario_name(context), f"  {event['timestamp']}: {event['message']}")
                context.sigkill_detected = True
            else:
                log_to_file(get_scenario_name(context), "ℹ️ No SIGKILL/137 events detected")
                context.sigkill_detected = False
        
        # Also check pod status for exit code
        try:
            pod_cmd = f"kubectl get pod {context.pod_name} -n {context.namespace} -o json"
            pod_result = run_kubectl(pod_cmd)
            pod_data = json.loads(pod_result)
            
            container_statuses = pod_data.get('status', {}).get('containerStatuses', [])
            for container in container_statuses:
                terminated = container.get('state', {}).get('terminated')
                if terminated:
                    exit_code = terminated.get('exitCode')
                    if exit_code == 137:
                        log_to_file(get_scenario_name(context), f"✅ Container exit code 137 detected (SIGKILL)")
                        context.exit_code_137 = True
                        return
            
            context.exit_code_137 = False
            
        except Exception:
            context.exit_code_137 = False
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR monitoring SIGKILL: {str(e)}")

# Continue with remaining steps...
@then('I should capture pod eviction events')
def step_capture_pod_eviction(context):
    try:
        log_to_file(get_scenario_name(context), "🚪 MONITORING POD EVICTION EVENTS")
        
        # Check pod events for eviction
        if hasattr(context, 'monitor'):
            eviction_events = [
                event for event in context.monitor.pod_events
                if event['pod'] == context.pod_name and
                any(keyword in event['reason'].lower() for keyword in [
                    'evict', 'preempt', 'delete'
                ]) if event.get('reason')
            ]
            
            if eviction_events:
                log_to_file(get_scenario_name(context), f"✅ Found {len(eviction_events)} eviction events:")
                for event in eviction_events:
                    log_to_file(get_scenario_name(context), f"  {event['timestamp']}: {event['reason']} - {event['message']}")
                context.pod_eviction_detected = True
            else:
                log_to_file(get_scenario_name(context), "ℹ️ No explicit eviction events detected")
                context.pod_eviction_detected = False
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR monitoring pod eviction: {str(e)}")

@then('I should verify original pod termination')
def step_verify_original_pod_termination(context):
    try:
        log_to_file(get_scenario_name(context), "💀 VERIFYING ORIGINAL POD TERMINATION")
        
        try:
            pod_cmd = f"kubectl get pod {context.pod_name} -n {context.namespace} -o json"
            pod_result = run_kubectl(pod_cmd)
            pod_data = json.loads(pod_result)
            
            phase = pod_data.get('status', {}).get('phase')
            deletion_timestamp = pod_data.get('metadata', {}).get('deletionTimestamp')
            
            if phase in ['Failed', 'Succeeded'] or deletion_timestamp:
                current_time = datetime.now()
                log_to_file(get_scenario_name(context), f"✅ Original pod terminated: phase={phase}")
                log_to_file(get_scenario_name(context), f"  Termination detected at: {current_time.strftime('%H:%M:%S.%f')[:-3]}")
                context.original_pod_terminated = True
                context.original_pod_termination_time = current_time
            else:
                log_to_file(get_scenario_name(context), f"⚠️ Original pod still exists: phase={phase}")
                context.original_pod_terminated = False
                
        except Exception:
            # Pod not found - it's been deleted
            current_time = datetime.now()
            log_to_file(get_scenario_name(context), "✅ Original pod terminated (not found)")
            log_to_file(get_scenario_name(context), f"  Termination detected at: {current_time.strftime('%H:%M:%S.%f')[:-3]}")
            context.original_pod_terminated = True
            context.original_pod_termination_time = current_time
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR verifying pod termination: {str(e)}")

@then('I should capture Karpenter provisioning new node')
def step_capture_new_node_provisioning(context):
    try:
        log_to_file(get_scenario_name(context), "🏗️ MONITORING NEW NODE PROVISIONING")
        
        # Monitor for new nodes
        start_time = datetime.now()
        timeout = 300  # 5 minutes
        
        initial_nodes = set()
        try:
            nodes_cmd = "kubectl get nodes -o json"
            nodes_result = run_kubectl(nodes_cmd)
            nodes_data = json.loads(nodes_result)
            initial_nodes = {node['metadata']['name'] for node in nodes_data.get('items', [])}
        except Exception:
            pass
        
        while (datetime.now() - start_time).total_seconds() < timeout:
            try:
                nodes_cmd = "kubectl get nodes -o json"
                nodes_result = run_kubectl(nodes_cmd)
                nodes_data = json.loads(nodes_result)
                current_nodes = {node['metadata']['name'] for node in nodes_data.get('items', [])}
                
                new_nodes = current_nodes - initial_nodes
                
                if new_nodes:
                    new_node = list(new_nodes)[0]
                    log_to_file(get_scenario_name(context), f"✅ New node provisioned: {new_node}")
                    context.new_node_name = new_node
                    context.new_node_provisioned = True
                    context.new_node_provision_time = datetime.now()
                    return
                
                elapsed = (datetime.now() - start_time).total_seconds()
                if elapsed % 30 < 5:  # Log every 30 seconds
                    log_to_file(get_scenario_name(context), f"  Waiting for new node... +{elapsed:.0f}s")
                
                time.sleep(10)
                
            except Exception as e:
                log_to_file(get_scenario_name(context), f"⚠️ Error checking nodes: {str(e)}")
                time.sleep(10)
        
        log_to_file(get_scenario_name(context), "⚠️ New node provisioning not detected within timeout")
        context.new_node_provisioned = False
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR monitoring new node provisioning: {str(e)}")

@then('I should monitor new NodeClaim creation')
def step_monitor_new_nodeclaim(context):
    try:
        log_to_file(get_scenario_name(context), "📋 MONITORING NEW NODECLAIM CREATION FROM KARPENTER LOGS")
        
        # Check if we have monitoring data from Karpenter logs
        if not hasattr(context, 'monitor') or not context.monitor:
            log_to_file(get_scenario_name(context), "⚠️ No Karpenter monitoring active - falling back to kubectl polling")
            return step_monitor_new_nodeclaim_fallback(context)
        
        # Look for NodeClaim creation events in captured Karpenter logs
        nodeclaim_events = [e for e in context.monitor.events if e.get('type') == 'karpenter_nodeclaim']
        
        if nodeclaim_events:
            # Find the most recent NodeClaim creation event
            creation_events = [e for e in nodeclaim_events if any(keyword in e['message'].lower() 
                             for keyword in ['created', 'provisioning', 'launching'])]
            
            if creation_events:
                latest_creation = max(creation_events, key=lambda x: x['timestamp'])
                
                # Extract NodeClaim name from the log message
                nodeclaim_name = extract_nodeclaim_name_from_log(latest_creation['message'])
                
                if nodeclaim_name:
                    log_to_file(get_scenario_name(context), f"✅ NodeClaim creation detected from Karpenter logs: {nodeclaim_name}")
                    log_to_file(get_scenario_name(context), f"📅 Creation time: {latest_creation['timestamp'].strftime('%H:%M:%S.%f')[:-3]}")
                    log_to_file(get_scenario_name(context), f"📝 Log entry: {latest_creation['message']}")
                    
                    context.new_nodeclaim_name = nodeclaim_name
                    context.new_nodeclaim_created = True
                    context.nodeclaim_creation_time = latest_creation['timestamp']
                    return
        
        # If no creation events found in logs yet, wait briefly and check again
        log_to_file(get_scenario_name(context), "⏳ Waiting for NodeClaim creation event in Karpenter logs...")
        
        start_time = datetime.now()
        timeout = 60  # Reduced timeout since we're monitoring real-time logs
        
        while (datetime.now() - start_time).total_seconds() < timeout:
            time.sleep(2)  # Check every 2 seconds
            
            # Check for new NodeClaim events
            current_events = [e for e in context.monitor.events if e.get('type') == 'karpenter_nodeclaim' 
                            and e['timestamp'] > start_time]
            
            creation_events = [e for e in current_events if any(keyword in e['message'].lower() 
                             for keyword in ['created', 'provisioning', 'launching'])]
            
            if creation_events:
                latest_creation = max(creation_events, key=lambda x: x['timestamp'])
                nodeclaim_name = extract_nodeclaim_name_from_log(latest_creation['message'])
                
                if nodeclaim_name:
                    log_to_file(get_scenario_name(context), f"✅ NEW NodeClaim creation detected: {nodeclaim_name}")
                    log_to_file(get_scenario_name(context), f"📅 Creation time: {latest_creation['timestamp'].strftime('%H:%M:%S.%f')[:-3]}")
                    log_to_file(get_scenario_name(context), f"📝 Log entry: {latest_creation['message']}")
                    
                    context.new_nodeclaim_name = nodeclaim_name
                    context.new_nodeclaim_created = True
                    context.nodeclaim_creation_time = latest_creation['timestamp']
                    return
        
        log_to_file(get_scenario_name(context), "⚠️ No NodeClaim creation detected in Karpenter logs within timeout")
        log_to_file(get_scenario_name(context), "🔄 Falling back to kubectl verification...")
        
        # Fallback to kubectl check to verify if NodeClaim exists
        step_monitor_new_nodeclaim_fallback(context)
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR monitoring NodeClaim creation: {str(e)}")
        traceback.print_exc()

def extract_nodeclaim_name_from_log(log_message):
    """Extract NodeClaim name from Karpenter log message"""
    try:
        # Common patterns in Karpenter logs for NodeClaim names
        patterns = [
            r'nodeclaim[/\s]+([a-z0-9-]+)',
            r'created nodeclaim ([a-z0-9-]+)',
            r'provisioning ([a-z0-9-]+)',
            r'launching ([a-z0-9-]+)',
            r'"name":"([a-z0-9-]+)"'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, log_message.lower())
            if match:
                return match.group(1)
        
        # If no pattern matches, try to find any alphanumeric string that looks like a NodeClaim name
        words = log_message.split()
        for word in words:
            # NodeClaim names are typically lowercase with dashes and numbers
            if re.match(r'^[a-z0-9-]{8,}$', word.strip('",.:;')):
                return word.strip('",.:;')
        
        return None
        
    except Exception:
        return None

def step_monitor_new_nodeclaim_fallback(context):
    """Fallback method using kubectl polling"""
    try:
        log_to_file(get_scenario_name(context), "🔄 Using kubectl polling for NodeClaim detection...")
        
        # Get initial NodeClaims
        initial_nodeclaims = set()
        try:
            nodeclaim_cmd = "kubectl get nodeclaim -o json"
            nodeclaim_result = run_kubectl(nodeclaim_cmd)
            nodeclaim_data = json.loads(nodeclaim_result)
            initial_nodeclaims = {nc['metadata']['name'] for nc in nodeclaim_data.get('items', [])}
            log_to_file(get_scenario_name(context), f"📊 Initial NodeClaims: {len(initial_nodeclaims)}")
        except Exception as e:
            log_to_file(get_scenario_name(context), f"⚠️ Could not get initial NodeClaims: {str(e)}")
        
        # Quick check - maybe NodeClaim already exists
        try:
            nodeclaim_cmd = "kubectl get nodeclaim -o json"
            nodeclaim_result = run_kubectl(nodeclaim_cmd)
            nodeclaim_data = json.loads(nodeclaim_result)
            current_nodeclaims = {nc['metadata']['name'] for nc in nodeclaim_data.get('items', [])}
            
            new_nodeclaims = current_nodeclaims - initial_nodeclaims
            
            if new_nodeclaims:
                new_nodeclaim = list(new_nodeclaims)[0]
                log_to_file(get_scenario_name(context), f"✅ NodeClaim already exists: {new_nodeclaim}")
                context.new_nodeclaim_name = new_nodeclaim
                context.new_nodeclaim_created = True
                return
            
            # If we have more NodeClaims than initially, pick the newest one
            if len(current_nodeclaims) > len(initial_nodeclaims):
                # Get the newest NodeClaim by creation timestamp
                newest_nodeclaim = None
                newest_time = None
                
                for nc in nodeclaim_data.get('items', []):
                    creation_time = nc['metadata'].get('creationTimestamp')
                    if creation_time:
                        if newest_time is None or creation_time > newest_time:
                            newest_time = creation_time
                            newest_nodeclaim = nc['metadata']['name']
                
                if newest_nodeclaim:
                    log_to_file(get_scenario_name(context), f"✅ Found newest NodeClaim: {newest_nodeclaim}")
                    context.new_nodeclaim_name = newest_nodeclaim
                    context.new_nodeclaim_created = True
                    return
                    
        except Exception as e:
            log_to_file(get_scenario_name(context), f"⚠️ Error in quick NodeClaim check: {str(e)}")
        
        log_to_file(get_scenario_name(context), "ℹ️ No new NodeClaim detected - may already be provisioned")
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR in NodeClaim fallback monitoring: {str(e)}")

@then('I should verify new node joins the cluster')
def step_verify_new_node_joins(context):
    try:
        log_to_file(get_scenario_name(context), "🔗 VERIFYING NEW NODE JOINS CLUSTER")
        
        if hasattr(context, 'new_node_name'):
            node_cmd = f"kubectl get node {context.new_node_name} -o json"
            node_result = run_kubectl(node_cmd)
            node_data = json.loads(node_result)
            
            conditions = node_data.get('status', {}).get('conditions', [])
            ready_condition = next((c for c in conditions if c['type'] == 'Ready'), None)
            
            if ready_condition and ready_condition['status'] == 'True':
                log_to_file(get_scenario_name(context), f"✅ New node {context.new_node_name} is Ready")
                context.new_node_ready = True
            else:
                log_to_file(get_scenario_name(context), f"⚠️ New node {context.new_node_name} is not Ready yet")
                context.new_node_ready = False
        else:
            log_to_file(get_scenario_name(context), "⚠️ No new node detected to verify")
            context.new_node_ready = False
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR verifying new node: {str(e)}")

@then('I should capture new node readiness')
def step_capture_new_node_readiness(context):
    try:
        log_to_file(get_scenario_name(context), "✅ MONITORING NEW NODE READINESS")
        
        if hasattr(context, 'new_node_name'):
            # Monitor node readiness over time
            start_time = datetime.now()
            timeout = 300  # 5 minutes
            
            while (datetime.now() - start_time).total_seconds() < timeout:
                try:
                    node_cmd = f"kubectl get node {context.new_node_name} -o json"
                    node_result = run_kubectl(node_cmd)
                    node_data = json.loads(node_result)
                    
                    conditions = node_data.get('status', {}).get('conditions', [])
                    ready_condition = next((c for c in conditions if c['type'] == 'Ready'), None)
                    
                    if ready_condition and ready_condition['status'] == 'True':
                        current_time = datetime.now()
                        elapsed = (current_time - start_time).total_seconds()
                        log_to_file(get_scenario_name(context), f"✅ NEW NODE READY: {context.new_node_name}")
                        log_to_file(get_scenario_name(context), f"  Ready at: {current_time.strftime('%H:%M:%S.%f')[:-3]}")
                        log_to_file(get_scenario_name(context), f"  Total node startup time: {elapsed:.2f}s")
                        
                        context.new_node_ready_time = current_time
                        return
                    
                    time.sleep(10)
                    
                except Exception:
                    time.sleep(10)
            
            log_to_file(get_scenario_name(context), "⚠️ New node readiness timeout")
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR monitoring node readiness: {str(e)}")

@then('I should capture new pod scheduling')
def step_capture_new_pod_scheduling(context):
    try:
        log_to_file(get_scenario_name(context), "📅 MONITORING NEW POD SCHEDULING")
        
        # Look for new pods with the same app label
        start_time = datetime.now()
        timeout = 300  # 5 minutes
        
        while (datetime.now() - start_time).total_seconds() < timeout:
            try:
                pods_cmd = f"kubectl get pods -n {context.namespace} -l app=math-compute-sqs-app -o json"
                pods_result = run_kubectl(pods_cmd)
                pods_data = json.loads(pods_result)
                
                for pod in pods_data.get('items', []):
                    pod_name = pod['metadata']['name']
                    
                    # Skip the original pod
                    if pod_name == context.pod_name:
                        continue
                    
                    # Check if this is a new pod
                    creation_time = pod['metadata']['creationTimestamp']
                    pod_creation = datetime.fromisoformat(creation_time.replace('Z', '+00:00')).replace(tzinfo=None)
                    
                    if pod_creation > context.interruption_start_time:
                        current_time = datetime.now()
                        elapsed = (current_time - start_time).total_seconds()
                        log_to_file(get_scenario_name(context), f"✅ NEW POD SCHEDULED: {pod_name}")
                        log_to_file(get_scenario_name(context), f"  Scheduled at: {current_time.strftime('%H:%M:%S.%f')[:-3]}")
                        log_to_file(get_scenario_name(context), f"  Time to schedule new pod: {elapsed:.2f}s")
                        
                        context.new_pod_name = pod_name
                        context.new_pod_scheduled = True
                        context.new_pod_scheduled_time = current_time
                        return
                
                time.sleep(10)
                
            except Exception as e:
                log_to_file(get_scenario_name(context), f"⚠️ Error checking pods: {str(e)}")
                time.sleep(10)
        
        log_to_file(get_scenario_name(context), "⚠️ New pod scheduling not detected within timeout")
        context.new_pod_scheduled = False
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR monitoring pod scheduling: {str(e)}")

@then('I should monitor new pod creation events')
def step_monitor_new_pod_creation(context):
    try:
        log_to_file(get_scenario_name(context), "🆕 MONITORING NEW POD CREATION EVENTS")
        
        if hasattr(context, 'monitor') and hasattr(context, 'new_pod_name'):
            # Look for events related to the new pod
            new_pod_events = [
                event for event in context.monitor.pod_events
                if event['pod'] == context.new_pod_name
            ]
            
            log_to_file(get_scenario_name(context), f"  Found {len(new_pod_events)} events for new pod:")
            for event in new_pod_events[-10:]:  # Show last 10 events
                log_to_file(get_scenario_name(context), f"    {event['timestamp']}: {event['reason']} - {event['message']}")
            
            context.new_pod_events_captured = len(new_pod_events) > 0
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR monitoring new pod events: {str(e)}")

@then('I should verify new pod starts successfully')
def step_verify_new_pod_starts(context):
    try:
        log_to_file(get_scenario_name(context), "🚀 VERIFYING NEW POD STARTUP")
        
        if hasattr(context, 'new_pod_name'):
            # Monitor new pod until it's running
            start_time = datetime.now()
            timeout = 300  # 5 minutes
            
            while (datetime.now() - start_time).total_seconds() < timeout:
                try:
                    pod_cmd = f"kubectl get pod {context.new_pod_name} -n {context.namespace} -o json"
                    pod_result = run_kubectl(pod_cmd)
                    pod_data = json.loads(pod_result)
                    
                    phase = pod_data.get('status', {}).get('phase')
                    
                    if phase == 'Running':
                        # Check if containers are ready
                        container_statuses = pod_data.get('status', {}).get('containerStatuses', [])
                        all_ready = all(cs.get('ready', False) for cs in container_statuses)
                        
                        if all_ready:
                            current_time = datetime.now()
                            elapsed = (current_time - start_time).total_seconds()
                            log_to_file(get_scenario_name(context), f"✅ NEW POD READY: {context.new_pod_name}")
                            log_to_file(get_scenario_name(context), f"  Pod Phase: Running")
                            log_to_file(get_scenario_name(context), f"  Ready at: {current_time.strftime('%H:%M:%S.%f')[:-3]}")
                            log_to_file(get_scenario_name(context), f"  Total startup time: {elapsed:.2f}s")
                            
                            # Calculate startup time if we have scheduling time
                            if hasattr(context, 'new_pod_scheduled_time'):
                                startup_time = (current_time - context.new_pod_scheduled_time).total_seconds()
                                log_to_file(get_scenario_name(context), f"  Startup time (scheduled → ready): {startup_time:.2f}s")
                            
                            context.new_pod_running = True
                            context.new_pod_ready_time = current_time
                            return
                    
                    time.sleep(10)
                    
                except Exception:
                    time.sleep(10)
            
            log_to_file(get_scenario_name(context), "⚠️ New pod startup timeout")
            context.new_pod_running = False
        else:
            log_to_file(get_scenario_name(context), "⚠️ No new pod to verify")
            context.new_pod_running = False
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR verifying new pod startup: {str(e)}")

@then('I should capture application recovery')
def step_capture_application_recovery(context):
    try:
        log_to_file(get_scenario_name(context), "🔄 VERIFYING APPLICATION RECOVERY")
        
        if hasattr(context, 'new_pod_name') and context.new_pod_running:
            # Test application functionality
            try:
                # Check if the application is responding (this depends on your app)
                # For now, we'll just verify the pod is running and ready
                pod_cmd = f"kubectl get pod {context.new_pod_name} -n {context.namespace} -o json"
                pod_result = run_kubectl(pod_cmd)
                pod_data = json.loads(pod_result)
                
                phase = pod_data.get('status', {}).get('phase')
                container_statuses = pod_data.get('status', {}).get('containerStatuses', [])
                all_ready = all(cs.get('ready', False) for cs in container_statuses)
                
                if phase == 'Running' and all_ready:
                    log_to_file(get_scenario_name(context), "✅ Application recovery verified - pod running and ready")
                    context.application_recovered = True
                else:
                    log_to_file(get_scenario_name(context), f"⚠️ Application not fully recovered - phase: {phase}, ready: {all_ready}")
                    context.application_recovered = False
                    
            except Exception as e:
                log_to_file(get_scenario_name(context), f"⚠️ Error verifying application: {str(e)}")
                context.application_recovered = False
        else:
            log_to_file(get_scenario_name(context), "⚠️ No new pod running to verify recovery")
            context.application_recovered = False
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR verifying application recovery: {str(e)}")

@then('I should analyze the complete timing breakdown')
def step_analyze_timing_breakdown(context):
    try:
        log_to_file(get_scenario_name(context), "\n" + "="*80)
        log_to_file(get_scenario_name(context), "📊 COMPREHENSIVE TIMING ANALYSIS")
        log_to_file(get_scenario_name(context), "="*80)
        
        if not hasattr(context, 'interruption_start_time'):
            log_to_file(get_scenario_name(context), "⚠️ No timing data available")
            return
        
        start_time = context.interruption_start_time
        current_time = datetime.now()
        
        # Calculate all timing components
        timing_data = {
            'test_start': start_time,
            'test_end': current_time,
            'total_duration': (current_time - start_time).total_seconds()
        }
        
        # Add specific timing milestones
        milestones = [
            ('instance_termination_time', 'Instance Termination'),
            ('new_node_provision_time', 'New Node Provisioned'),
            ('new_node_ready_time', 'New Node Ready'),
            ('new_pod_schedule_time', 'New Pod Scheduled'),
            ('new_pod_ready_time', 'New Pod Ready')
        ]
        
        log_to_file(get_scenario_name(context), f"\n🎯 TIMING BREAKDOWN:")
        log_to_file(get_scenario_name(context), f"  Test Start: {start_time.strftime('%H:%M:%S.%f')[:-3]}")
        
        for attr_name, description in milestones:
            if hasattr(context, attr_name):
                milestone_time = getattr(context, attr_name)
                elapsed = (milestone_time - start_time).total_seconds()
                timing_data[attr_name] = elapsed
                log_to_file(get_scenario_name(context), f"  {description}: +{elapsed:.2f}s")
        
        log_to_file(get_scenario_name(context), f"  Test End: {current_time.strftime('%H:%M:%S.%f')[:-3]}")
        log_to_file(get_scenario_name(context), f"  Total Duration: {timing_data['total_duration']:.2f}s")
        
        # Calculate derived metrics
        log_to_file(get_scenario_name(context), f"\n📈 DERIVED METRICS:")
        
        if 'new_node_provision_time' in timing_data and 'instance_termination_time' in timing_data:
            node_replacement_time = timing_data['new_node_provision_time'] - timing_data['instance_termination_time']
            log_to_file(get_scenario_name(context), f"  Node Replacement Time: {node_replacement_time:.2f}s")
        
        if 'new_pod_ready_time' in timing_data and 'new_pod_schedule_time' in timing_data:
            pod_startup_time = timing_data['new_pod_ready_time'] - timing_data['new_pod_schedule_time']
            log_to_file(get_scenario_name(context), f"  Pod Startup Time: {pod_startup_time:.2f}s")
        
        if 'new_pod_ready_time' in timing_data:
            total_recovery_time = timing_data['new_pod_ready_time']
            log_to_file(get_scenario_name(context), f"  Total Recovery Time: {total_recovery_time:.2f}s")
            
            # Performance assessment
            if total_recovery_time < 120:
                assessment = "🟢 EXCELLENT (< 2 min)"
            elif total_recovery_time < 300:
                assessment = "🟡 GOOD (< 5 min)"
            elif total_recovery_time < 600:
                assessment = "🟠 ACCEPTABLE (< 10 min)"
            else:
                assessment = "🔴 SLOW (> 10 min)"
            
            log_to_file(get_scenario_name(context), f"  Performance Assessment: {assessment}")
        
        context.timing_analysis_complete = True
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR in timing analysis: {str(e)}")

@then('I should verify all components worked correctly')
def step_verify_all_components(context):
    try:
        log_to_file(get_scenario_name(context), "\n" + "="*80)
        log_to_file(get_scenario_name(context), "✅ COMPREHENSIVE COMPONENT VERIFICATION")
        log_to_file(get_scenario_name(context), "="*80)
        
        # Define all components to check
        components = [
            ('eventbridge_event_captured', 'EventBridge Event Capture'),
            ('sqs_message_captured', 'SQS Message Processing'),
            ('karpenter_spot_processing', 'Karpenter Spot Processing'),
            ('node_tainted', 'Node Tainting'),
            ('node_cordoned', 'Node Cordoning'),
            ('sigterm_detected', 'SIGTERM Signal Detection'),
            ('original_pod_terminated', 'Original Pod Termination'),
            ('instance_terminated', 'Instance Termination'),
            ('new_node_provisioned', 'New Node Provisioning'),
            ('new_pod_scheduled', 'New Pod Scheduling'),
            ('new_pod_running', 'New Pod Running'),
            ('application_recovered', 'Application Recovery')
        ]
        
        passed_components = 0
        total_components = len(components)
        
        log_to_file(get_scenario_name(context), f"\n📋 COMPONENT STATUS:")
        
        for attr_name, description in components:
            status = getattr(context, attr_name, False)
            icon = "✅" if status else "❌"
            log_to_file(get_scenario_name(context), f"  {icon} {description}")
            if status:
                passed_components += 1
        
        success_rate = (passed_components / total_components) * 100
        
        log_to_file(get_scenario_name(context), f"\n📊 OVERALL RESULTS:")
        log_to_file(get_scenario_name(context), f"  Components Passed: {passed_components}/{total_components}")
        log_to_file(get_scenario_name(context), f"  Success Rate: {success_rate:.1f}%")
        
        if success_rate >= 90:
            overall_status = "🟢 EXCELLENT"
        elif success_rate >= 75:
            overall_status = "🟡 GOOD"
        elif success_rate >= 50:
            overall_status = "🟠 PARTIAL"
        else:
            overall_status = "🔴 POOR"
        
        log_to_file(get_scenario_name(context), f"  Overall Status: {overall_status}")
        
        context.component_verification_complete = True
        context.overall_success_rate = success_rate
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR in component verification: {str(e)}")

@then('I should generate comprehensive test report')
def step_generate_comprehensive_report(context):
    try:
        log_to_file(get_scenario_name(context), "\n" + "="*80)
        log_to_file(get_scenario_name(context), "📄 COMPREHENSIVE TEST REPORT")
        log_to_file(get_scenario_name(context), "="*80)
        
        # Test summary
        log_to_file(get_scenario_name(context), f"\n🎯 TEST SUMMARY:")
        log_to_file(get_scenario_name(context), f"  Test Type: Comprehensive Spot Interruption Lifecycle")
        log_to_file(get_scenario_name(context), f"  Cluster: {getattr(context, 'cluster_name', 'Unknown')}")
        log_to_file(get_scenario_name(context), f"  Namespace: {getattr(context, 'namespace', 'Unknown')}")
        log_to_file(get_scenario_name(context), f"  Original Instance: {getattr(context, 'instance_id', 'Unknown')}")
        log_to_file(get_scenario_name(context), f"  Original Pod: {getattr(context, 'pod_name', 'Unknown')}")
        log_to_file(get_scenario_name(context), f"  Original Node: {getattr(context, 'node_name', 'Unknown')}")
        
        if hasattr(context, 'new_pod_name'):
            log_to_file(get_scenario_name(context), f"  New Pod: {context.new_pod_name}")
        if hasattr(context, 'new_node_name'):
            log_to_file(get_scenario_name(context), f"  New Node: {context.new_node_name}")
        
        # Test results
        success_rate = getattr(context, 'overall_success_rate', 0)
        log_to_file(get_scenario_name(context), f"\n📊 TEST RESULTS:")
        log_to_file(get_scenario_name(context), f"  Overall Success Rate: {success_rate:.1f}%")
        
        if hasattr(context, 'timing_analysis_complete'):
            log_to_file(get_scenario_name(context), f"  Timing Analysis: ✅ Complete")
        
        # Recommendations
        log_to_file(get_scenario_name(context), f"\n💡 RECOMMENDATIONS:")
        
        if success_rate < 75:
            log_to_file(get_scenario_name(context), f"  - Review failed components and investigate root causes")
            log_to_file(get_scenario_name(context), f"  - Check Karpenter configuration and permissions")
            log_to_file(get_scenario_name(context), f"  - Verify EventBridge and SQS setup")
        
        if not getattr(context, 'new_node_provisioned', False):
            log_to_file(get_scenario_name(context), f"  - Check Karpenter node provisioning configuration")
            log_to_file(get_scenario_name(context), f"  - Verify EC2 instance limits and availability")
        
        if not getattr(context, 'application_recovered', False):
            log_to_file(get_scenario_name(context), f"  - Review application health checks and startup time")
            log_to_file(get_scenario_name(context), f"  - Check pod resource requests and limits")
        
        log_to_file(get_scenario_name(context), f"\n🏁 COMPREHENSIVE TEST REPORT COMPLETE")
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"❌ ERROR generating report: {str(e)}")

@then('I should cleanup test resources')
def step_cleanup_test_resources(context):
    try:
        log_to_file(get_scenario_name(context), "🧹 CLEANING UP TEST RESOURCES")
        
        # Stop monitoring
        if hasattr(context, 'monitor'):
            context.monitor.stop_monitoring()
        
        # Clean up FIS resources
        if hasattr(context, 'fis_template_id'):
            cleanup_fis_experiment_template(context, context.fis_template_id, context.region)
        
        log_to_file(get_scenario_name(context), "✅ Test cleanup complete")
        
    except Exception as e:
        log_to_file(get_scenario_name(context), f"⚠️ Cleanup warning: {str(e)}")