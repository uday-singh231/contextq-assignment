# Distributed Ray AI Platform on Kubernetes (EKS)

A production-ready platform for orchestrating distributed Ray workloads, entity resolution, ML model training, and Apache Iceberg catalog synchronization on Amazon EKS using KubeRay.

---

## 1. High-Level Architecture Diagram

graph TD
    subgraph CI_CD ["GitHub Actions CI/CD"]
        PR["Pull Request"] -->|Triggers| TF_Plan["Terraform Plan"]
        Merge["Merge to main"] -->|"terraform/** changed"| TF_Apply["Terraform Apply"]
        Manual["Manual Trigger (workflow_dispatch)"] -->|"Deploy Image & Job"| ECR["Build & Push to ECR"]
        ECR -->|Apply Manifests| K8s_API["EKS Kubernetes API"]
    end

    subgraph AWS_Cloud ["AWS Cloud"]
        subgraph VPC ["Amazon VPC 10.0.0.0/16"]
            subgraph EKS ["Amazon EKS Cluster"]
                KubeRay["KubeRay Operator"] -->|Manages Lifecycle| RayJob_CRD["RayJob: ray-entity-resolution"]

                subgraph NodeGroup_System ["System Pool - On-Demand"]
                    RayHead["Ray Head Pod"]
                end

                subgraph NodeGroup_Spot ["Worker Pool - Spot Instances"]
                    RayWorker1["Ray Worker: Data Ingestion"]
                    RayWorker2["Ray Worker: ML Training"]
                end

                subgraph NodeGroup_Specialized ["Specialized Pool - On-Demand"]
                    RayActor["EntityResolver LLM Actor"]
                end
            end

            subgraph Storage_Data ["Storage & Catalog Layer"]
                S3["Amazon S3: Iceberg Warehouse"]
                Glue["AWS Glue Data Catalog"]
            end
        end
    end

    RayJob_CRD --> RayHead
    RayHead --> RayWorker1
    RayHead --> RayWorker2
    RayHead --> RayActor
    RayWorker1 -->|Read & Write| S3
    RayWorker2 -->|Schema & Metadata Updates| Glue
---

## 2. Setup & Deployment Instructions

### Prerequisites
- [AWS CLI v2](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html) configured with administrative access
- [Terraform >= 1.5.0](https://developer.hashicorp.com/terraform/downloads)
- [kubectl](https://kubernetes.io/docs/tasks/tools/) and [Helm 3](https://helm.sh/docs/intro/install/)
- Python 3.10+ (for local testing)

### Step 1: Provision Infrastructure via Terraform
```bash
cd terraform
terraform init
terraform plan
terraform apply -auto-approve
```
Take note of the outputs: `cluster_name`, `s3_bucket_name`, and `ray_irsa_role_arn`.

### Step 2: Configure Kubernetes Context
```bash
aws eks update-kubeconfig --region us-east-1 --name dev-ray-eks
```

### Step 3: Install KubeRay Operator
```bash
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
helm upgrade --install kuberay-operator kuberay/kuberay-operator \
  --namespace kuberay-system \
  --create-namespace
```

### Step 4: Configure Kubernetes Manifests
Replace placeholders in `k8s/service-account.yaml` and `k8s/rayjob.yaml` with your AWS Account ID and S3 Bucket Name:
```bash
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
sed -i "s|<REPLACE_WITH_RAY_IRSA_ROLE_ARN>|arn:aws:iam::${AWS_ACCOUNT_ID}:role/dev-ray-pod-irsa-role|g" k8s/service-account.yaml
sed -i "s|<ACCOUNT_ID>|${AWS_ACCOUNT_ID}|g" k8s/rayjob.yaml
sed -i "s|<REPLACE_WITH_S3_BUCKET_NAME>|dev-ray-iceberg-${AWS_ACCOUNT_ID}|g" k8s/rayjob.yaml
```

### Step 5: Submit the RayJob
```bash
kubectl apply -f k8s/secret.yaml
kubectl apply -f k8s/service-account.yaml
kubectl apply -f k8s/rayjob.yaml
```

### Step 6: Verify and Inspect Logs
```bash
# Check RayJob status
kubectl get rayjob ray-entity-resolution

# Stream logs from the Ray head pod
kubectl logs -l ray.io/node-type=head -f
```

---

## 3. Spot Instance Strategy for Cost Reduction

Data processing and machine learning batch workloads are inherently transient and elastic, making them ideal candidates for Spot instances. However, Ray clusters require architectural isolation to ensure spot terminations do not abort the entire job:

1. **Instance Family Diversification**:
   - The worker node group (`ray-spot-workers`) is configured with multiple instance types: `m6i.xlarge`, `m5.xlarge`, `c6i.xlarge`, and `c5.xlarge`.
   - AWS allocates spot capacity based on the pool with highest availability, substantially lowering interruption probability compared to single-instance groups.
2. **Decoupled System & Stateful Workloads**:
   - **Ray Head Node**: Hosted on an **On-Demand** node pool (`system-pool`). If the Ray Head pod is evicted, the Global Control Store (GCS) and cluster metadata are lost. Keeping the head on On-Demand guarantees cluster stability.
   - **EntityResolver Actor**: Hosted on a dedicated **On-Demand** node pool (`ray-actor-workers`) via node selector `node-role: specialized-actor`. Stateful actors maintaining warm models or LLM tokens cannot tolerate arbitrary evictions.
   - **Batch Compute**: Only stateless `@ray.remote` ingestion and distributed training workers run on Spot nodes.
3. **Ray Task Fault Tolerance**:
   - Ray provides lineage-based task reconstruction. If a Spot worker node receives a 2-minute termination notice or is evicted, Ray marks the node dead in GCS and automatically reschedules interrupted tasks onto available worker pods.

---

## 4. Ray Data & Train Production Tuning

- **Shared Memory Configuration (`/dev/shm`)**:
  - Ray uses Apache Arrow Plasma Object Store for zero-copy memory reads between processes.
  - The default Linux container shared memory is capped at 64MB. Both head and worker pods explicitly mount an `emptyDir` backed by RAM (`medium: Memory`) at `/dev/shm` (1Gi on head, 2Gi on workers) to prevent `ObjectStoreFull` errors.
- **High-Throughput Ingestion**:
  - Ray tasks (`@ray.remote(num_cpus=1)`) ingest datasets in parallel partitions across worker nodes, preventing single-node memory saturation.
- **Resource Placement for Specialized Actor**:
  - The `EntityResolver` requires custom resource constraint `@ray.remote(resources={"resolver_node": 1})`.
  - The `actor-workers` Kubernetes spec advertises `rayStartParams: {resources: '{"resolver_node": 1}'}` and binds to `node-role: specialized-actor`, ensuring this actor is physically isolated from generic batch worker nodes.

---

## 5. Serverless Alternative to the Kubernetes Cluster

If managing an EKS cluster, node groups, and the KubeRay Operator introduces too much operational overhead, a fully serverless alternative on AWS comprises:

```text
S3 (Raw Data) ──> AWS Glue 4.0 (Spark ETL & Iceberg) ──> AWS Step Functions ──> Amazon SageMaker Training (Spot) ──> Apache Iceberg (S3)
```

### Components
1. **AWS Glue 4.0 (Serverless Spark)**:
   - Replaces Ray Data for parallel reading of CSV datasets.
   - Native integration with Apache Iceberg catalogs via AWS Glue Data Catalog.
   - Built-in machine learning transform (`FindMatches`) provides out-of-the-box entity resolution without managing custom actors or GPU instances.
2. **Amazon SageMaker Training (Managed Spot Training)**:
   - Replaces Ray Train for executing the linear regression / XGBoost revenue prediction model.
   - SageMaker spins up isolated instances on-demand, trains the model, saves checkpoints to S3, and shuts down compute automatically (scaling to zero).
3. **AWS Step Functions**:
   - Acts as the serverless workflow orchestrator, replacing the `ray_pipeline.py` master script and `RayJob` controller.

### Trade-Off Analysis

| Criteria | Ray on Amazon EKS | AWS Serverless (Glue + SageMaker + Step Functions) |
| :--- | :--- | :--- |
| **Operational Overhead** | Higher (managing EKS, node pools, upgrades, Helm) | Minimal (managed by AWS, no servers to patch) |
| **Execution Latency** | Low (containers already warm, low startup overhead) | Higher (cold start delays provisioning Glue & SageMaker) |
| **Language & Ecosystem** | Pure Python, single unified codebase and debug loop | Fragmented (PySpark for Glue, Python for SageMaker, JSON/ASL for Step Functions) |
| **Cost at Sustained Load** | Lower at high, continuous volume with Spot instances | Higher per unit of compute; best for sporadic, infrequent batch runs |

---

## 6. Observability & Monitoring

- **Ray Dashboard**: Exposes real-time task graphs, CPU/memory usage, and actor status on port `8265`.
  ```bash
  kubectl port-forward svc/ray-entity-resolution-head-svc 8265:8265
  ```
- **Centralized Logging**: All pipeline stdout and stderr logs are emitted in container streams, collected via AWS Fluent Bit / CloudWatch Container Insights.
- **Prometheus Metrics**: KubeRay exports cluster and operator health metrics on port `44217`.
