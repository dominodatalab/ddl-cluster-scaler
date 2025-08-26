# Ray Cluster Custom Scaler (Beta)
This is a basic Ray Cluster Scaler



## Installation

If `domino-field` namespace is not present create using below command

```shell
kubectl create namespace domino-field
kubectl label namespace domino-field  domino-compute=true
kubectl label namespace domino-field  domino-platform=true
```

```shell
export field_namespace=domino-field
helm install -f ./values.yaml ddl-cluster-scaler helm/ddl-cluster-scaler -n ${field_namespace}
```
## Upgrade

```shell
export field_namespace=domino-field

helm upgrade -f ./values.yaml rayclusterscaler helm/rayclusterscaler -n ${field_namespace}
```

## Delete 

```shell
export field_namespace=domino-field
helm delete  rayclusterscaler -n ${field_namespace}
```

## Endpoints
Minimal HTTP API to **list**, **inspect**, and **scale** Kubernetes cluster CRDs (Ray, Dask, Spark).

All responses are JSON and include `Cache-Control: no-store`.

---

This service `http://ddl-cluster-scaler-svc.domino-field/` provides the following endpoints

### Authentication

Send **one** of:

- `Authorization: Bearer <token>`
- `X-Domino-Api-Key: <key>`

Authorization policy:

- **Admins**: full access
- **Non-admins**: can only access clusters they **own**, determined by label on the pod:
`metadata.labels["dominodatalab.com/starting-user-id"]`


---

## Supported kinds

Use these exact **plural** names in paths:

- `rayclusters`
- `daskclusters`
- `sparkclusters`

---

### 1) List clusters

**GET** `/ddl_cluster_scaler_api/list/{kind}`

Lists clusters of the given kind that the caller is authorized to view.

#### Path parameters
- `kind` — one of `rayclusters|daskclusters|sparkclusters`

#### Success response — `200`
```json
{
"kind": "rayclusters",
"count": 2,
"clusters": [ { "...k8s object..." }, { "...k8s object..." } ]
}
```
#### Errors
- 
- `400`:
```json
{ "error": "invalid_kind", "message": "kind must be one of [ ... ]" }
```

- `500`:
```json
{ "error": "list_failed", "message": "Failed to list clusters", "details": "..." }
```

#### Example
```shell
curl -sS \
  -H "Authorization: Bearer $TOKEN" \
  "$BASE/ddl_cluster_scaler_api/list/rayclusters"
```

###  2) Get a cluster

*GET* /cluster/{kind}/{name}

Fetch a single cluster by name (if authorized).

#### Path parameters

- kind — rayclusters|daskclusters|sparkclusters

- name — Kubernetes object name

#### Success response — 200

```json
{ "...k8s object..." }
```

#### Errors

- `400`:
```json
{ "error": "invalid_kind", "message": "kind must be one of [ ... ]" }
```


- `403`:
```json
{ "error": "unauthorized", "message": "Not authorized to access cluster <name>" }
```

- `500`:
```json
{ "error": "get_failed", "message": "Failed to fetch cluster <name>", "details": "..." }
```

#### Example
```shell
curl -sS \
  -H "Authorization: Bearer $TOKEN" \
  "$BASE/cluster/rayclusters/my-raycluster"
```

### 3) Scale a cluster

**POST** `/cluster/scale/{kind}`

Update desired replicas. Requires CRD to expose spec.autoscaling.

> If present, spec.worker.replicas is also updated.
> Ensures spec.autoscaling.maxReplicas >= minReplicas with at least +1 headroom.

#### Path parameters

- kind — rayclusters|daskclusters|sparkclusters

**Request body (JSON)**
```json
{
  "cluster_name": "my-raycluster",
  "replicas": 3
}
```

### Success response — 200

```json
{
  "kind": "rayclusters",
  "name": "my-raycluster",
  "replicas": 3,
  "object": { "...patched k8s object..." }
}

```

#### Errors
- `400`:
```json
{ "error": "invalid_kind", "message": "kind must be one of [ ... ]" }
```
```json
{ "error": "bad_request", "message": "cluster_name is required" }
```
```json
{ "error": "bad_request", "message": "replicas must be a non-negative integer" }
```

- `403`:
```json
{ "error": "unauthorized", "message": "Not authorized to update cluster <name>" }
```

- `409`:
```json
{ "error": "not_scalable", "message": "Cannot scale this cluster: autoscaling not enabled" }
```

- `500`:
```json
{ "error": "scale_failed", "message": "Failed to scale cluster <name>", "details": "..." }
```

#### Example
```shell
curl -sS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"cluster_name":"my-raycluster","replicas":4}' \
  "$BASE/cluster/scale/rayclusters"
```

### Common response headers
- Content-Type: application/json
- Cache-Control: no-store

### Notes

- kind must match the CRD plural exactly.

- Ownership is evaluated via metadata.labels["dominodatalab.com/starting-user-id"].

- Error payloads include a stable error code, a human-readable message, and optional details for debugging.