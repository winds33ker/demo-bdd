"""Configuration parameters for BDD tests"""
import os

class TestConfig:
    # EKS Cluster Configuration
    CLUSTER_NAME = os.getenv('CLUSTER_NAME', 'eks-dev-cluster-3')
    AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
    NAMESPACE = os.getenv('NAMESPACE', 'argocd')
    
#    # Application Configuration
#    APP_NAME = os.getenv('APP_NAME', 'math-compute-sqs-app')
#    DEPLOYMENT_NAME = os.getenv('DEPLOYMENT_NAME', 'springboot-test-app')
#    SERVICE_NAME = os.getenv('SERVICE_NAME', 'springboot-test-service')
#    
#    # HPA Configuration
#    HPA_NAME = os.getenv('HPA_NAME', 'springboot-hpa')
#    MIN_REPLICAS = int(os.getenv('MIN_REPLICAS', '2'))
#    MAX_REPLICAS = int(os.getenv('MAX_REPLICAS', '10'))
#    CPU_TARGET = int(os.getenv('CPU_TARGET', '50'))
#    MEMORY_TARGET = int(os.getenv('MEMORY_TARGET', '70'))
#    
#    # Load Testing Configuration
#    LOAD_DURATION = int(os.getenv('LOAD_DURATION', '60'))
#    MEMORY_SIZE = int(os.getenv('MEMORY_SIZE', '500'))
#    CONCURRENT_REQUESTS = int(os.getenv('CONCURRENT_REQUESTS', '5'))
#    
#    # Timing Configuration
#    SCALE_UP_TIMEOUT = int(os.getenv('SCALE_UP_TIMEOUT', '120'))
#    SCALE_DOWN_TIMEOUT = int(os.getenv('SCALE_DOWN_TIMEOUT', '300'))
#    SPOT_INTERRUPTION_TIMEOUT = int(os.getenv('SPOT_INTERRUPTION_TIMEOUT', '180'))
#    
#    # AWS Services Configuration
#    SERVICES_TO_CHECK = os.getenv('SERVICES_TO_CHECK', 'sqs,events,kms').split(',')
#    
#    # Security Configuration
#    ALLOWED_NAMESPACES = os.getenv('ALLOWED_NAMESPACES', 'karpenter,kube-system').split(',')
#    KMS_KEY_ID = os.getenv('KMS_KEY_ID', '2611e42d-e61a-4a42-88d4-c6578e55d1d3')