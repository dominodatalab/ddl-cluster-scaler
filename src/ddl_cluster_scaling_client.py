"""
Cluster Scaler Client (requests)

Endpoints expected (server-side):
  - GET    /healthz
  - GET    /ddl_cluster_scaler/cluster/<kind>/<name>
  - PATCH  /ddl_cluster_scaler/scale/<kind>/<name>               (json: {"replicas": int, "worker_hw_tier_id": "..."} optional)
  - PATCH  /ddl_cluster_scaler/restart-head/<kind>/<name>        (query: started_at=ISO8601)
  - GET    /ddl_cluster_scaler/restart_head_status/<kind>/<name>    (query: started_at=ISO8601)

Environment variables used:
  - DOMINO_API_PROXY   : base URL that returns a Bearer token at /access-token (preferred)
  - DOMINO_RUN_ID      : current run id used to derive the cluster name
  - CLUSTER_SCALER_BASE_URL : override base_url_of_cluster_scaler (optional)
"""

from __future__ import annotations

import os
import time
import json
import requests
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

base_url_of_cluster_scaler = "http://ddl-cluster-scaler-svc.domino-field.svc.cluster.local"
service_name="ddl_cluster_scaler"

DEFAULT_BASE = "http://ddl-cluster-scaler-svc.domino-field.svc.cluster.local"
SERVICE_PREFIX = "/ddl_cluster_scaler"
DEFAULT_COMPUTE_NAMESPACE = os.getenv("COMPUTE_NAMESPACE", "domino-compute")

BASE_URL = os.getenv("CLUSTER_SCALER_BASE_URL", DEFAULT_BASE)



def is_cluster_auto_scaler_healthy() -> Tuple[int, Any]:
    """
    Probe the scaler's /healthz endpoint.

    Returns:
      (status_code, body)
        - body is parsed JSON if available; otherwise plain text; None on 204.
        - on network error, returns (0, {"error": "..."}).
    """
    url = f"{BASE_URL}/healthz"
    try:
        resp = requests.get(url, headers={"Accept": "application/json"}, timeout=(3.05, 10))
        status = resp.status_code

        if status == 204 or not resp.content:
            body = None
        else:
            try:
                body = resp.json()
            except Exception:
                body = resp.text

        return status, body

    except requests.RequestException as e:
        return 0, {"error": str(e)}



def get_auth_headers() -> Dict[str, str]:
    """
    Build auth headers for the scaler.

    Resolution order:
      1) DOMINO_API_PROXY -> GET <proxy>/access-token (Bearer)
      2) DOMINO_TOKEN     -> Bearer
      3) DOMINO_API_KEY   -> X-Domino-Api-Key

    Returns headers with Accept + auth. (No need to set Content-Type; `requests` adds it
    automatically when you use `json=...`.)
    """
    headers: Dict[str, str] = {"Accept": "application/json"}

    proxy = os.getenv("DOMINO_API_PROXY")
    if proxy:
        token_url = proxy.rstrip("/") + "/access-token"
        try:
            resp = requests.get(token_url, timeout=(3.05, 10))
            resp.raise_for_status()
            token = resp.text.strip()
            if not token:
                raise RuntimeError("Empty token from DOMINO_API_PROXY /access-token")
            headers["Authorization"] = f"Bearer {token}"
            return headers
        except requests.RequestException as e:
            raise RuntimeError(f"Failed to fetch token from DOMINO_API_PROXY: {e}") from e

    token = os.getenv("DOMINO_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token.strip()}"
        return headers

    api_key = os.getenv("DOMINO_API_KEY")
    if api_key:
        headers["X-Domino-Api-Key"] = api_key.strip()
        return headers

    raise RuntimeError(
        "No auth source found. Set DOMINO_API_PROXY or DOMINO_TOKEN or DOMINO_API_KEY."
    )


def get_cluster_status(cluster_kind: str = "rayclusters"):
    """
    Fetch the cluster CR for the current run.

    Returns:
      Parsed JSON (dict) for the cluster object.

    Raises:
      requests.HTTPError on non-2xx
      RuntimeError if response isn't JSON
    """
    run_id = os.environ.get("DOMINO_RUN_ID")
    if not run_id:
        raise RuntimeError("DOMINO_RUN_ID is not set")

    cluster_id_prefix = cluster_kind.replace("clusters", "")
    cluster_id = f"{cluster_id_prefix}-{run_id}"
    print(cluster_id)

    # NOTE: server route is /ddl_cluster_scaler/cluster/<kind>/<name>
    url = f"{BASE_URL}{SERVICE_PREFIX}/cluster/{cluster_kind}/{cluster_id}"
    print(url)

    resp = requests.get(url, headers=get_auth_headers(), timeout=(3.05, 15))
    print(f"Status code {resp.status_code}")
    resp.raise_for_status()

    # Expect JSON; fail loudly if not
    content_type = resp.headers.get("Content-Type", "")
    if "application/json" not in content_type.lower():
        # try anyway, then error
        try:
            return resp.json()
        except Exception:
            raise RuntimeError(f"Expected JSON from {url}, got Content-Type={content_type!r}")

    return resp.json()


def scale_cluster(cluster_kind: str = "rayclusters", worker_hw_tier_name="Small", replicas: int = 1):
    """
    Scale a cluster's workers to `replicas`. Keeps the old interface:
      - `worker_hw_tier_name` is optional and passed through.
    Returns parsed JSON from the scaler.

    Notes:
      - Server currently expects either `worker_hw_tier_id` (new) or `worker_hw_tier` (old).
        We send both with the same value for compatibility.
      - If your server only accepts IDs, pass the ID in `worker_hw_tier_name`.
    """
    run_id = os.environ.get("DOMINO_RUN_ID")
    if not run_id:
        raise RuntimeError("DOMINO_RUN_ID is not set")

    print(replicas)

    cluster_id_prefix = cluster_kind.replace("clusters", "")
    cluster_id = f"{cluster_id_prefix}-{run_id}"

    url = f"{BASE_URL}{SERVICE_PREFIX}/scale/{cluster_kind}/{cluster_id}"

    body: Dict[str, Any] = {"replicas": int(replicas)}
    if worker_hw_tier_name:
        # Backward/forward compatibility: send both keys
        body["worker_hw_tier"] = worker_hw_tier_name       # legacy (by name)
        body["worker_hw_tier_id"] = worker_hw_tier_name    # new (by id) — pass same value unless you have the id

    resp = requests.patch(
        url,
        headers=get_auth_headers(),
        json=body,
        timeout=(3.05, 15),
    )
    print(f"Status code {resp.status_code}")
    resp.raise_for_status()

    # Expect JSON; fall back to text if not
    try:
        return resp.json()
    except Exception:
        return resp.text


def is_scaling_complete(cluster_kind: str = "rayclusters") -> bool:
    """
    True when the number of worker pods equals desired minReplicas.
    Assumes .status.nodes is [head, worker1, worker2, ...] (Ray-style).
    """
    result = get_cluster_status(cluster_kind)
    try:
        # Defensive lookups
        spec = result.get("spec", {})
        worker = spec.get("worker", {})
        effective_replicas = int(worker.get("replicas"))

        status = result.get("status", {})
        nodes = status.get("nodes", []) or []

        # workers = total nodes minus 1 head (if any nodes exist)
        actual_workers = max(0, len(nodes) - 1) if nodes else 0
        is_complete = (actual_workers == effective_replicas)

        print(f"Expected worker nodes {effective_replicas}")
        # Safe slice: show worker names if present
        try:
            print(f"Current worker nodes {nodes[1:]}")
        except Exception:
            print("Current worker nodes: <unavailable>")

        return is_complete
    except Exception as e:
        print(f"[scaler] unable to compute scaling completeness: {e}")
        return False


def wait_until_scaling_complete(cluster_kind: str = "rayclusters") -> bool:
    """
    Poll until scaling completes. (No interface change: fixed 2s poll, no explicit timeout.)
    Returns True when complete.
    """
    is_complete = is_scaling_complete(cluster_kind)
    while not is_complete:
        print("Scaling not yet done...")
        time.sleep(2)
        is_complete = is_scaling_complete(cluster_kind)
    return is_complete

def restart_head_node(cluster_kind: str = "rayclusters"):
    """
    Initiate a head restart by deleting the head pod.
    Server expects a started_at timestamp in the query; we generate one here.
    Returns the server's JSON (or text fallback).
    """
    run_id = os.environ.get("DOMINO_RUN_ID")
    if not run_id:
        raise RuntimeError("DOMINO_RUN_ID is not set")

    cluster_id_prefix = cluster_kind.replace("clusters", "")
    cluster_id = f"{cluster_id_prefix}-{run_id}"

    # Server route: PATCH /ddl_cluster_scaler/restart-head/<kind>/<name>?started_at=ISO8601
    url = f"{BASE_URL}{SERVICE_PREFIX}/restart_head/{cluster_kind}/{cluster_id}"
    stamp = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

    resp = requests.patch(
        url,
        headers=get_auth_headers(),
        params={"started_at": stamp},
        timeout=(3.05, 15),
    )
    print(f"Status code {resp.status_code}")
    resp.raise_for_status()

    try:
        return resp.json()
    except Exception:
        return resp.text


def restart_head_node_status(cluster_kind: str = "rayclusters", restarted_since: str = "2099-12-31T00:00:00Z"):
    """
    Check whether the head pod was restarted on/after `restarted_since` AND is Ready now.
    Uses namespace + head pod name behind the scenes (no interface change).
    """
    run_id = os.environ.get("DOMINO_RUN_ID")
    if not run_id:
        raise RuntimeError("DOMINO_RUN_ID is not set")

    cluster_id_prefix = cluster_kind.replace("clusters", "")
    cluster_id = f"{cluster_id_prefix}-{run_id}"

    # Server route: GET /ddl_cluster_scaler/restart_head_status/<namespace>/<pod>?started_at=ISO8601
    # Derive the head pod name (e.g., "<cluster>-head-0" for Ray)
    pod_name = f"{cluster_id}-head-0"
    url = f"{BASE_URL}{SERVICE_PREFIX}/restart_head_status/{cluster_kind}/{cluster_id}"
    params = {"started_at": restarted_since}

    resp = requests.get(
        url,
        headers=get_auth_headers(),
        params=params,
        timeout=(3.05, 15),
    )
    print(f"Status code {resp.status_code}")
    resp.raise_for_status()

    try:
        return resp.json()
    except Exception:
        return resp.text


def wait_until_node_restarted(cluster_kind: str = "rayclusters", restarted_since: str = "2099-12-31T00:00:00Z"):
    """
    Poll until the head reports 'restarted_and_ready'.
    (No interface change: fixed 3s poll; no explicit timeout.)
    """
    status = restart_head_node_status(cluster_kind, restarted_since).get("status")
    print(status)
    while status != "restarted_and_ready":
        sleep_time = 3
        print(f"Wait {sleep_time} seconds before checking again")
        time.sleep(sleep_time)
        status = restart_head_node_status(cluster_kind, restarted_since).get("status")
        print(status)

import json
if __name__ == "__main__":
    result = get_auth_headers()
    print(result['Authorization'][0:20])

    print(get_cluster_status()['status'])

    j = scale_cluster(cluster_kind="rayclusters",worker_hw_tier_name="Medium", replicas=2)
    json.dumps(j, indent=2, sort_keys=True, ensure_ascii=False)
    wait_until_scaling_complete(cluster_kind="rayclusters")

    restart_head_node(cluster_kind="rayclusters")
    restarts_at = j['started_at']
    print(restarts_at)

    print(restart_head_node_status(cluster_kind="rayclusters",restarted_since=restarts_at))
    wait_until_node_restarted(cluster_kind="rayclusters",restarted_since=restarts_at)