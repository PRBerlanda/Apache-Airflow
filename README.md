# Airflow on Kubernetes (minikube + Helm, KubernetesExecutor)

Part of the [Apache-Airflow](https://github.com/PRBerlanda/Apache-Airflow) portfolio. Each project lives in its own branch.

This project deploys Airflow on a local Kubernetes cluster (**minikube**) with the official **Helm chart** and the **KubernetesExecutor**. Each task runs in its own pod.

DAGs, logs and extracted data are kept in **PersistentVolumes** that are mounted from this folder into minikube. The DAG is `get_stocks.py`, copied from the [`airflow-celery`](https://github.com/PRBerlanda/Apache-Airflow/tree/airflow-celery) branch.

```
airflow-config.yml     Namespace + 3 PersistentVolumes + 3 PersistentVolumeClaims (dags, logs, data)
override-values.yml    values for the Apache Airflow Helm chart
Dockerfile             apache/airflow:3.2.2 + requirements.txt (yfinance)
requirements.txt       extra Python packages baked into the image
dags/                  mounted at /data/dags  -> Airflow DAG folder
logs/                  mounted at /data/logs  -> task logs
data/                  mounted at /data/data  -> /opt/airflow/data (AIRFLOW_DATA_DIR)
```

## How to run

You need Docker, [minikube](https://minikube.sigs.k8s.io/docs/start/), [Helm](https://helm.sh/docs/intro/install/) and `kubectl` (or `minikube kubectl --`).

```bash
git clone -b airflow-kubernetes https://github.com/PRBerlanda/Apache-Airflow.git airflow-kubernetes
cd airflow-kubernetes

# Terminal 1: start Docker and the cluster
sudo service docker start
minikube start

# Terminal 2: namespace and volumes
minikube kubectl -- apply -f airflow-config.yml

# Terminals 3, 4 and 5: keep these running (one mount per terminal)
minikube mount ./dags/:/data/dags
minikube mount ./logs/:/data/logs
minikube mount ./data/:/data/data

# Terminal 2: build the image inside minikube's Docker
eval $(minikube -p minikube docker-env)
docker build -t airflow-yfinance:latest .

# Install Airflow with the Helm chart
helm repo add apache-airflow https://airflow.apache.org
helm repo update
helm upgrade --install airflow apache-airflow/airflow --namespace airflow -f override-values.yml

# UI at http://localhost:8080
kubectl port-forward svc/airflow-api-server 8080:8080 --namespace airflow
```

Before running the DAG, create the pool it uses. You can do it in the UI (**Admin → Pools**) or with:

```bash
kubectl exec -n airflow deploy/airflow-scheduler -- airflow pools set small_pool 2 "Yahoo Finance"
```

Open the dashboard with `minikube dashboard` to watch a pod start for each task.

## Concepts

- **Docker** builds and runs containers: isolated processes started from an image that holds all the dependencies of an application. [Docker overview](https://docs.docker.com/get-started/docker-overview/)
- **Kubernetes** orchestrates containers across a cluster.
  - The control plane keeps the cluster in the desired state.
  - The worker nodes run the applications.
  - [Kubernetes tutorials](https://kubernetes.io/docs/tutorials/)
- **Minikube** runs a single-node Kubernetes cluster on your machine, for testing. [minikube](https://minikube.sigs.k8s.io/docs/)
- **Pod** is the smallest unit in Kubernetes: one or more containers that share storage and network. With the KubernetesExecutor, every Airflow task becomes a pod.
- **Helm** is the package manager for Kubernetes; its packages are called *charts*. The Apache Airflow chart installs every component from a single values file. [Helm docs](https://helm.sh/docs/)
- **PersistentVolume (PV)** is cluster storage that outlives containers. Data written inside a container is lost when the container stops.
- **PersistentVolumeClaim (PVC)** is how a pod requests part of a PV.

### CeleryExecutor or KubernetesExecutor?

- **CeleryExecutor** keeps workers running all the time, so tasks start almost immediately. It suits time-sensitive tasks.
- **KubernetesExecutor** starts a new pod for each task.
  - That adds start-up time, sometimes minutes, but the pods scale down to zero between runs. This saves resources for DAGs that run rarely.
  - You can also set CPU, memory, volumes and image per task.

## Changes from the original course code

- **`override-values.yml`: dotted keys fixed.** `workers.replicas: 0` and `redis.enabled: false` were literal keys with a dot, which Helm ignores. They are now nested YAML keys.
- **Image name now matches the build command.** The values pointed to the repository `AirflowKubernets`, which is not a valid Docker name (uppercase letters) and did not match `docker build -t airflow .`.
  - The image is now `airflow-yfinance:latest`, with `pullPolicy: IfNotPresent` and `airflowVersion: "3.2.2"`.
- **Data volume is now mounted.** The `airflow-data-claim` PVC was created but never mounted.
  - It is now mounted at `/opt/airflow/data`, exposed as `AIRFLOW_DATA_DIR`.
  - The PVCs bind to their PVs explicitly with `volumeName`.
- **`expose_config` moved** from `[webserver]` to `[api]`, its section in Airflow 3.
- **Pinned dependencies.** The Dockerfile installs `requirements.txt` with yfinance pinned, and `dags/` ships with the DAG used in the course.

**Status:**

- **Checked:** the YAML files parse, and the DAG imports cleanly with Airflow 3.3.
- **Not checked:** the Helm deployment itself was not run in a cluster during this review.
