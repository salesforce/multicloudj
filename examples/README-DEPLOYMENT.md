# Deploying MultiCloudJ Examples to GKE

This guide explains how to deploy the MultiCloudJ blob example to your GKE cluster `sdk-scrathpad`.

## Prerequisites

- Java 11+
- Maven
- Docker or gcloud CLI with Cloud Build enabled
- kubectl configured for your GKE cluster
- Access to GCP project: substrate-sdk-gcp-poc1

## Files Created

- `Dockerfile` - Container image definition
- `k8s-deployment.yaml` - Kubernetes Deployment manifest (for long-running services)
- `k8s-job.yaml` - Kubernetes Job manifest (for one-time execution, recommended)
- `deploy.sh` - Automated deployment script

## Deployment Steps

### Step 1: Build the Application

The application has already been built. The JAR file is located at:
```
examples/target/multicloudj-examples-0.2.25-SNAPSHOT.jar
```

### Step 2: Build and Push Docker Image

#### Option A: Using Docker (requires Docker daemon)
```bash
cd examples
docker build -t gcr.io/substrate-sdk-gcp-poc1/multicloudj-examples:latest .
docker push gcr.io/substrate-sdk-gcp-poc1/multicloudj-examples:latest
```

#### Option B: Using Cloud Build (recommended)
```bash
cd examples
gcloud builds submit . --tag=gcr.io/substrate-sdk-gcp-poc1/multicloudj-examples:latest
```

### Step 3: Deploy to GKE

#### For One-Time Execution (Job):
```bash
kubectl apply -f k8s-job.yaml
```

#### For Continuous Running (Deployment):
```bash
kubectl apply -f k8s-deployment.yaml
```

### Step 4: Monitor the Application

#### Check Job Status:
```bash
kubectl get jobs -n multicloudj
kubectl describe job multicloudj-blob-example -n multicloudj
```

#### View Logs:
```bash
# For Job
kubectl logs -f job/multicloudj-blob-example -n multicloudj

# For Deployment
kubectl logs -f deployment/multicloudj-examples -n multicloudj
```

#### Check Pods:
```bash
kubectl get pods -n multicloudj
```

## Configuration

The application reads configuration from environment variables defined in the ConfigMap:
- `BUCKET_NAME`: The GCS bucket name (default: palsfdc)
- `BUCKET_REGION`: The GCS bucket region (default: us-west2)

To modify these values, edit the ConfigMap in the YAML files before deploying.

## Workload Identity Setup

The deployment uses GKE Workload Identity to authenticate to GCP services:
- Kubernetes ServiceAccount: `multicloudj-sa`
- GCP Service Account: `chameleon@substrate-sdk-gcp-poc1.iam.gserviceaccount.com`

Make sure the GCP service account has the necessary permissions for:
- Cloud Storage (for blob operations)
- STS (for token generation)

To bind the service accounts:
```bash
gcloud iam service-accounts add-iam-policy-binding \
  chameleon@substrate-sdk-gcp-poc1.iam.gserviceaccount.com \
  --role roles/iam.workloadIdentityUser \
  --member "serviceAccount:substrate-sdk-gcp-poc1.svc.id.goog[multicloudj/multicloudj-sa]"
```

## Automated Deployment

Use the provided script:
```bash
cd examples
./deploy.sh
```

## Cleanup

To remove the deployment:
```bash
# Remove Job
kubectl delete -f k8s-job.yaml

# Or remove Deployment
kubectl delete -f k8s-deployment.yaml

# Remove entire namespace
kubectl delete namespace multicloudj
```

## Troubleshooting

### Authentication Issues
If you see authentication errors in the logs, verify:
1. Workload Identity is enabled on the cluster
2. The GCP service account has necessary permissions
3. The ServiceAccount annotation is correct

### Image Pull Errors
If pods can't pull the image:
1. Verify the image was pushed: `gcloud container images list --repository=gcr.io/substrate-sdk-gcp-poc1`
2. Check cluster has access to GCR: The cluster should have read access to the project's GCR

### Application Errors
Check the application logs:
```bash
kubectl logs -f <pod-name> -n multicloudj
```
