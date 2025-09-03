# Cluster Scaler Client (Python)

A tiny requests-based helper for talking to the **DDL Cluster Scaler** service from Python.  
It wraps common flows like **get cluster**, **scale workers**, and **restart head + verify**.

---

## Requirements

- Python 3.8+
- `requests` (`pip install requests`)
- Network access to the Cluster Scaler service

---

## Environment Variables

These are read by the client at runtime:

- `CLUSTER_SCALER_BASE_URL` (optional)  
  Defaults to `http://ddl-cluster-scaler-svc.domino-field.svc.cluster.local`.

- `DOMINO_API_PROXY` (preferred)  
  Base URL that serves a token at `/access-token`. Used to build `Authorization: Bearer <token>`.

- `DOMINO_TOKEN` (fallback)  
  Bearer token to use if `DOMINO_API_PROXY` is not set.

- `DOMINO_API_KEY` (fallback)  
  Domino API key; sent as `X-Domino-Api-Key` if neither proxy nor token is present.

- `DOMINO_RUN_ID` (**required**)  
  Used to generate a cluster name `<kind-without-"clusters">-<DOMINO_RUN_ID>` (e.g., `ray-abc123`).

- `COMPUTE_NAMESPACE` (optional)  
  Defaults to `domino-compute`. Used when deriving the head pod name behind the scenes.

---

## Endpoints (expected server-side)

The client assumes these routes exist on your scaler:

- `GET    /healthz`
- `GET    /ddl_cluster_scaler/cluster/<kind>/<name>`
- `PATCH  /ddl_cluster_scaler/scale/<kind>/<name>`  
  JSON body: `{"replicas": <int>, "worker_hw_tier_id": "<id>"}` (tier optional; name also accepted via `worker_hw_tier`)
- `PATCH  /ddl_cluster_scaler/restart_head/<kind>/<name>?started_at=<ISO-8601>`
- `GET    /ddl_cluster_scaler/restart_head_status/<kind>/<name>?started_at=<ISO-8601>`

> The **status** route above assumes your server can resolve `<kind>/<name>` to the correct head pod internally.  
> If your server uses a different path (e.g., by namespace/pod), adjust the client or add a server alias.

---

## Quick Start

```python
from src.ddl_cluster_scaling_client import (
    is_cluster_auto_scaler_healthy,
    get_cluster_status,
    scale_cluster,
    wait_until_scaling_complete,
    restart_head_node,
    restart_head_node_status,
    wait_until_node_restarted,
)
from datetime import datetime, timezone

# 1) health
code, body = is_cluster_auto_scaler_healthy()
print("health:", code, body)

# 2) read current cluster
print(get_cluster_status("rayclusters"))

# 3) scale to 2 workers (optionally pass a HW tier id/name)
print(scale_cluster("rayclusters", worker_hw_tier_name="Medium", replicas=2))

# 4) wait until scaled
wait_until_scaling_complete("rayclusters")

# 5) restart the head (returns JSON with the server’s view; often includes started_at)
kick = restart_head_node("rayclusters")

# If the server doesn’t echo started_at, use the same one you sent (the client generates one)
started_at = kick.get("started_at")  # ISO-8601 string, e.g. "2025-09-02T15:40:00Z"

# 6) check status once
print(restart_head_node_status("rayclusters", restarted_since=started_at))

# 7) or poll until restarted and ready
wait_until_node_restarted("rayclusters", restarted_since=started_at)
```

---

## Function Reference

### `is_cluster_auto_scaler_healthy() -> tuple[int, Any]`
Probes `/healthz`. Returns `(status_code, body)` where `body` is JSON if available, else text (or `None` for `204`).

---

### `get_auth_headers() -> dict[str, str]`
Builds auth headers. Resolution order:

1) `DOMINO_API_PROXY` → `GET <proxy>/access-token` → `Authorization: Bearer <token>`  
2) `DOMINO_TOKEN` → `Authorization: Bearer <token>`  
3) `DOMINO_API_KEY` → `X-Domino-Api-Key: <key>`

Raises if none are set.

---

### `get_cluster_status(cluster_kind: str = "rayclusters") -> dict`
Fetches the cluster CR for the current run (`<kind-without-"clusters">-<DOMINO_RUN_ID>`).  
Calls `GET /ddl_cluster_scaler/cluster/<kind>/<name>` and returns parsed JSON.

---

### `scale_cluster(cluster_kind: str = "rayclusters", worker_hw_tier_name="Small", replicas: int = 1) -> Any`
Scales worker replicas to `replicas` (server caps at max).  
Sends both `worker_hw_tier` and `worker_hw_tier_id` for compatibility.  
Returns the server response (JSON preferred, text fallback).

---

### `is_scaling_complete(cluster_kind: str = "rayclusters") -> bool`
Returns `True` when `actual_workers == minReplicas`, assuming Ray-style status shape:
- `.status.nodes = [head, worker1, worker2, ...]`

---

### `wait_until_scaling_complete(cluster_kind: str = "rayclusters") -> bool`
Polls every 2s until `is_scaling_complete()` is `True`.  
(No explicit timeout—add one in your caller if needed.)

---

### `restart_head_node(cluster_kind: str = "rayclusters") -> dict | str`
Initiates a head restart (deletes head pod) with a timestamp.  
Sends `PATCH /ddl_cluster_scaler/restart_head/<kind>/<name>?started_at=<ISO-8601>`  
Returns the server JSON (or text) and typically echoes `started_at`.

---

### `restart_head_node_status(cluster_kind: str = "rayclusters", restarted_since: str = "2099-12-31T00:00:00Z") -> dict | str`
Checks whether the **head** was **restarted on/after** `restarted_since` **and is Ready now**.  
Calls `GET /ddl_cluster_scaler/restart_head_status/<kind>/<name>?started_at=<ISO-8601>`  
Returns JSON with keys like `ok`, `restarted`, `ready`, `status`.

---

### `wait_until_node_restarted(cluster_kind: str = "rayclusters", restarted_since: str = "2099-12-31T00:00:00Z") -> None`
Polls the status every 3 seconds until `status == "restarted_and_ready"`.  
(No explicit timeout—wrap in your own deadline if you need one.)

---

## Timestamps: ISO-8601 (UTC)

When initiating a restart, the client generates an ISO-8601 UTC timestamp (e.g. `2025-09-02T15:40:00Z`) and passes it as `started_at`. The status endpoint uses the same `started_at` to decide if the head’s `creationTimestamp` is **on/after** that moment.

If you need your own stamp:

```python
from datetime import datetime, timezone
def iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
```

---

## Pretty Printing JSON

```python
import json
import src.ddl_cluster_scaling_client as client
def pretty(obj) -> str:
    return json.dumps(obj, indent=2, sort_keys=True, ensure_ascii=False)

print(pretty(client.get_cluster_status("rayclusters")))
```

---

## Troubleshooting

- **401/403** — Auth headers missing/invalid. Verify `DOMINO_API_PROXY`, `DOMINO_TOKEN`, or `DOMINO_API_KEY`.
- **404** — Cluster name mismatch. Confirm `DOMINO_RUN_ID` and the `<kind>/<name>` the server expects.
- **Timeouts** — Bump the `(connect, read)` timeouts in the client calls if your network is slow.
- **Non-JSON responses** — The client falls back to `resp.text`. If your server always returns JSON, set proper `Content-Type`.
- **Head status never becomes `restarted_and_ready`** — Ensure server’s `restart_head_status` route maps `<kind>/<name>` to the actual head pod and that your `started_at` is the one used when initiating the restart.

---

## Notes & Conventions

- Cluster names are derived as `<kind without "clusters">-<DOMINO_RUN_ID>` (e.g., `ray-<RUN>`).  
  Change that logic in `cluster_name_from_run_id` if your naming differs.
- The client assumes Ray-style `.status.nodes` ordering (head first). If your CRD differs, adjust `is_scaling_complete`.
- All requests use short connect/read timeouts by default; tune as needed.

---

That’s it. Drop this client into your run, set the env vars, and go.
