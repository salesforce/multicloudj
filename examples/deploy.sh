#!/bin/bash
set -e

# Configuration
PROJECT_ID="substrate-sdk-gcp-poc1"
REGION="us-west2"
CLUSTER_NAME="sdk-scrathpad"
IMAGE_NAME="gcr.io/${PROJECT_ID}/multicloudj-examples:latest"

echo "=== Building Docker Image ==="
# Option 1: Using Docker (requires Docker daemon running)
# docker build -t ${IMAGE_NAME} .
# docker push ${IMAGE_NAME}

# Option 2: Using Cloud Build (recommended for GCP)
gcloud builds submit . --tag=${IMAGE_NAME} --project=${PROJECT_ID}

echo "=== Deploying to GKE ==="
# Ensure kubectl is configured for the right cluster
gcloud container clusters get-credentials ${CLUSTER_NAME} --region=${REGION} --project=${PROJECT_ID}

# Apply Kubernetes manifests
kubectl apply -f k8s-deployment.yaml

echo "=== Checking Deployment Status ==="
kubectl rollout status deployment/multicloudj-examples -n multicloudj

echo "=== Viewing Logs ==="
echo "To view logs, run:"
echo "  kubectl logs -f deployment/multicloudj-examples -n multicloudj"
