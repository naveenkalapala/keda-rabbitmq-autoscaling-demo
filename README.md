# KEDA RabbitMQ Autoscaling Demo

A complete end-to-end demonstration of **Kubernetes Event-Driven Autoscaling (KEDA)** using RabbitMQ queue length as the scaling metric. This project validates that consumer pods automatically scale out under load and scale back to zero when idle.

---

## Architecture

```
┌─────────────────────┐
│  Fake Order         │
│  Publisher (Job)    │
│                     │
│  Publishes N fake   │
│  order messages     │
└────────┬────────────┘
         │
         │ AMQP 5672
         ▼
┌─────────────────────┐
│     RabbitMQ        │
│  (queue: fake-orders)│
│                     │
│  Durable queue      │
│  holds messages     │
└────────┬────────────┘
         │
         │ Consumed by
         ▼
┌─────────────────────┐
│  Order Consumer     │
│  Deployment         │
│                     │
│  Processes orders   │
│  with simulated     │
│  750ms latency      │
└─────────────────────┘
         ▲
         │ Scales
         │
┌─────────────────────┐
│       KEDA          │
│                     │
│  ScaledObject       │
│  monitors queue     │
│  length via AMQP    │
└────────┬────────────┘
         │
         │ Exposes external metric
         ▼
┌─────────────────────┐
│  External Metrics   │
│  API Server         │
│  (keda-operator-    │
│   metrics-apiserver)│
└────────┬────────────┘
         │
         │ Feeds metric to
         ▼
┌─────────────────────┐
│        HPA          │
│                     │
│  desiredReplicas =  │
│  ceil(queueLen / 5) │
│  min=1, max=10      │
└────────┬────────────┘
         │
         │ Scales deployment
         ▼
┌─────────────────────┐
│  Consumer Pods      │
│  (0 → 10 replicas) │
└─────────────────────┘
```

### Flow

1. **Publisher Job** generates fake order JSON messages and publishes them to the `fake-orders` RabbitMQ queue.
2. **RabbitMQ** holds messages in a durable queue until consumers acknowledge them.
3. **Consumer Deployment** pods pull messages, simulate processing (750ms delay), and ACK.
4. **KEDA ScaledObject** polls RabbitMQ every 15 seconds and reports the queue length as an external metric.
5. **HPA** (auto-created by KEDA) computes desired replicas: `ceil(queue_length / 5)`.
6. The consumer deployment **scales out** (up to 10 replicas) when backlog grows, and **scales to zero** when the queue is empty for 120 seconds.

---

## Prerequisites

- Kubernetes cluster (Kind, EKS, GKE, etc.)
- `kubectl` configured
- `helm` (for KEDA installation)
- Docker (to build/push images)

---

## Quick Start

### 1. Install KEDA

```bash
helm repo add kedacore https://kedacore.github.io/charts
helm repo update
kubectl create namespace keda
helm install keda kedacore/keda --namespace keda
```

Verify KEDA is running:

```bash
kubectl get pods -n keda
```

### 2. Deploy RabbitMQ

```bash
kubectl apply -f k8s/rabbitmq-deployment.yaml
```

Wait for the pod to be ready:

```bash
kubectl wait --for=condition=ready pod -l app=rabbitmq -n keda --timeout=60s
```

### 3. Deploy Secret + Consumer + KEDA Scaling

```bash
kubectl apply -f k8s/rabbitmq-secret.yaml
kubectl apply -f k8s/consumer-deployment.yaml
kubectl apply -f k8s/trigger-auth.yaml
kubectl apply -f k8s/scaled-object.yaml
```

This creates:
- `Secret/rabbitmq-connection` — AMQP connection string for KEDA
- `Deployment/fake-order` — consumer workload (scaled by KEDA)
- `TriggerAuthentication/fake-order-trigger-auth` — links KEDA to the secret
- `ScaledObject/fake-order` — defines the RabbitMQ scaling trigger

### 4. Generate Load

```bash
kubectl apply -f k8s/publisher-job.yaml
```

Or run with a custom count:

```bash
kubectl delete job fake-orders-publisher -n keda --ignore-not-found
kubectl apply -f k8s/publisher-job.yaml
```

### 5. Observe Scaling

```bash
# Watch pods scale up
kubectl get pods -n keda -w

# Check HPA status
kubectl get hpa -n keda

# Check ScaledObject status
kubectl get scaledobject -n keda

# View consumer logs
kubectl logs -l app=fake-order -n keda --tail=20 -f
```

### 6. Access RabbitMQ Management UI

```bash
kubectl port-forward -n keda svc/rabbitmq 15672:15672
```

Open http://localhost:15672 (login: `guest` / `guest`)

---

## Project Structure

```
.
├── README.md                        # This file
├── k8s/                             # Kubernetes manifests
│   ├── rabbitmq-deployment.yaml     # RabbitMQ Deployment + Service
│   ├── rabbitmq-secret.yaml         # AMQP connection string Secret (used by KEDA)
│   ├── consumer-deployment.yaml     # Consumer Deployment
│   ├── trigger-auth.yaml            # KEDA TriggerAuthentication
│   ├── scaled-object.yaml           # KEDA ScaledObject (scaling rules)
│   └── publisher-job.yaml           # Kubernetes Job to generate load
├── publisher/                       # Publisher app + image
│   ├── fake-orders-generator.py     # Publishes fake orders to RabbitMQ
│   └── Dockerfile                   # Docker image for the publisher
├── consumer/                        # Consumer app + image
│   ├── orders-consumer.py           # Processes orders from RabbitMQ
│   └── Dockerfile                   # Docker image for the consumer
└── Images/                          # Screenshots for docs
```

---

## Configuration

### ScaledObject Parameters

| Parameter | Value | Description |
|-----------|-------|-------------|
| `pollingInterval` | 15s | How often KEDA checks the queue length |
| `cooldownPeriod` | 120s | Wait time before scaling down after last trigger |
| `idleReplicaCount` | 0 | Scale to zero when queue is empty |
| `minReplicaCount` | 1 | Minimum replicas when active |
| `maxReplicaCount` | 10 | Maximum replicas |
| `value` | 5 | Target messages per replica (queue_length / 5 = desired replicas) |
| `activationValue` | 1 | Minimum queue length to activate from zero |

### Consumer Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `RABBITMQ_HOST` | rabbitmq | RabbitMQ hostname |
| `RABBITMQ_PORT` | 5672 | RabbitMQ port |
| `RABBITMQ_USER` | guest | RabbitMQ username |
| `RABBITMQ_PASSWORD` | guest | RabbitMQ password |
| `QUEUE_NAME` | fake-orders | Queue to consume from |
| `PREFETCH_COUNT` | 20 | Messages to prefetch per consumer |
| `PROCESSING_TIME_MS` | 750 | Simulated processing time per message |

### Publisher Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--count` | 500 | Number of orders to publish |
| `--burst-delay-ms` | 0 | Delay between messages (0 = max throughput) |
| `--queue` | fake-orders | Target queue name |

---

## Scaling Behavior

### Scale Up Example

```
Queue Length: 50 messages
Target per replica: 5
Desired replicas: ceil(50 / 5) = 10 (capped at maxReplicaCount=10)
```

### Scale Down

- After the queue is drained, KEDA waits `cooldownPeriod` (120s) before scaling down.
- When fully idle, replicas go to `idleReplicaCount` (0).

### Scale to Zero → Activation

- When `idleReplicaCount=0`, the deployment has 0 pods.
- Once `activationValue` (1 message) appears in the queue, KEDA activates and scales to `minReplicaCount` (1).
- Further scaling follows the normal HPA formula.

---

## Docker Images

### Build

```bash
docker build -t <your-registry>/fake-orders-publisher:latest publisher/
docker build -t <your-registry>/fake-orders-consumer:latest consumer/
```

### Push

```bash
docker push <your-registry>/fake-orders-publisher:latest
docker push <your-registry>/fake-orders-consumer:latest
```

Pre-built images available on Docker Hub:
- `nkalapala24/fake-orders-publisher:latest`
- `nkalapala24/fake-orders-consumer:latest`

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| ScaledObject not found / CRD error | Install KEDA first: `helm install keda kedacore/keda -n keda` |
| Consumer not scaling | Check `kubectl describe scaledobject fake-order -n keda` for errors |
| KEDA can't reach RabbitMQ | Verify secret: `kubectl get secret rabbitmq-connection -n keda -o yaml` |
| Publisher can't connect | Ensure RabbitMQ pod is Ready: `kubectl get pods -n keda -l app=rabbitmq` |
| Queue not visible in UI | Port-forward 15672 and check http://localhost:15672 |

---

## Clean Up

```bash
kubectl delete -f k8s/publisher-job.yaml
kubectl delete -f k8s/scaled-object.yaml
kubectl delete -f k8s/trigger-auth.yaml
kubectl delete -f k8s/consumer-deployment.yaml
kubectl delete -f k8s/rabbitmq-secret.yaml
kubectl delete -f k8s/rabbitmq-deployment.yaml
helm uninstall keda -n keda
kubectl delete namespace keda
```

---

