from __future__ import annotations

"""
Cluster Scaler API blueprint (Ray/Dask/Spark)
- Hardened logging + config
- K8s client init (in-cluster or local)
- Domino auth via global, pooled requests.Session (Bearer or API key)
- Generic authz across cluster kinds (owner-or-admin)
- Endpoints: list, get, scale
"""

# stdlib
import logging
import os
from dataclasses import dataclass
from typing import Literal, Dict, Any, List, Final, Optional, Mapping

# third-party
from flask import Blueprint, jsonify, request
from kubernetes import client, config
from kubernetes.client import ApiClient, CustomObjectsApi, ApiException, CoreV1Api, V1DeleteOptions
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from typing import Mapping

try:
    config.load_incluster_config()
except config.ConfigException:
    try:
        config.load_kube_config()
    except config.ConfigException:
        raise Exception("Could not configure kubernetes python client")
from datetime import datetime, timezone

# ---------- Logging (configure once) ----------

def _coerce_log_level(name: str) -> int:
    lvl = logging.getLevelName(str(name).upper())
    return lvl if isinstance(lvl, int) else logging.WARNING


LOG_LEVEL: Final[int] = _coerce_log_level(os.getenv("LOG_LEVEL", "INFO"))
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)

logger = logging.getLogger("ddl-cluster-scaler-api")
logger.setLevel(LOG_LEVEL)


# ---------- Flask blueprint ----------

ddl_cluster_scaler_api = Blueprint("ddl_cluster_scaler_api", __name__)


# ---------- Constants / Config ----------

AllowedKind = Literal["rayclusters", "daskclusters", "sparkclusters"]

LABEL_OWNER_ID: Final[str] = "dominodatalab.com/starting-user-id"
NO_CACHE: Final[Dict[str, str]] = {"Cache-Control": "no-store"}

DEFAULT_COMPUTE_NAMESPACE: Final[str] = "domino-compute"
COMPUTE_NAMESPACE: Final[str] = os.getenv("COMPUTE_NAMESPACE", DEFAULT_COMPUTE_NAMESPACE)

GROUP: Final[str] = "distributed-compute.dominodatalab.com"
VERSION: Final[str] = "v1alpha1"

#DOMINO_NUCLEUS_URI_RAW = os.getenv("DOMINO_NUCLEUS_URI", "http://nucleus-frontend.domino-platform:80")
DOMINO_NUCLEUS_URI_RAW = os.getenv("DOMINO_NUCLEUS_URI", "https://marcdo77364.cs.domino.tech")
DOMINO_NUCLEUS_URI: Final[str] = DOMINO_NUCLEUS_URI_RAW.rstrip("/")
WHO_AM_I_ENDPOINT: Final[str] = "v4/auth/principal"

DEBUG_MODE: Final[bool] = os.getenv("FLASK_ENV") == "development"


# ---------- Kubernetes client (init after config load) ----------

def _init_k8s_clients() -> tuple[ApiClient, CustomObjectsApi]:
    # Load config: prefer in-cluster, fall back to local kubeconfig.
    try:
        config.load_incluster_config()
        logger.info("Loaded in-cluster Kubernetes config")
    except config.ConfigException:
        try:
            config.load_kube_config()
            logger.info("Loaded local kubeconfig")
        except config.ConfigException as e:
            logger.exception("Could not configure Kubernetes python client")
            raise RuntimeError("Kubernetes config error") from e

    api_client: ApiClient = client.ApiClient()
    return api_client, client.CustomObjectsApi(api_client)


K8S_API_CLIENT, K8S_CUSTOM_OBJECTS = _init_k8s_clients()


# ---------- Domino HTTP session (global + pooled) ----------

def _build_domino_session(base_url: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({"Accept": "application/json"})

    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.3,
        status_forcelist=(502, 503, 504),
        allowed_methods=("GET", "POST", "PUT", "PATCH", "DELETE"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=32, pool_maxsize=128)
    s.mount("http://", adapter)
    s.mount("https://", adapter)

    # convenience: stash normalized base_url for helpers
    s.base_url = base_url.rstrip("/")  # type: ignore[attr-defined]
    return s


_DOMINO_SESSION = _build_domino_session(DOMINO_NUCLEUS_URI)


def domino_get(path: str, *, headers: Dict[str, str], timeout: tuple[float, float] = (3.05, 8)) -> requests.Response:
    """GET helper using the shared Domino session."""
    url = f"{_DOMINO_SESSION.base_url}/{path.lstrip('/')}"  # type: ignore[attr-defined]
    return _DOMINO_SESSION.get(url, headers=headers, timeout=timeout)


# ---------- Auth utilities ----------

def get_auth_headers(headers: Mapping[str, str]) -> Dict[str, str]:
    """
    Forward only supported auth headers to Domino Nucleus.
    Prefer API key if present; otherwise pass Authorization as-is.
    """
    out: Dict[str, str] = {}
    api_key = headers.get("X-Domino-Api-Key") or headers.get("x-domino-api-key")
    if api_key:
        out["X-Domino-Api-Key"] = api_key
    else:
        auth = headers.get("Authorization") or headers.get("authorization")
        if auth:
            out["Authorization"] = auth
    return out


@dataclass(frozen=True)
class UserIdentity:
    username: Optional[str]
    canonical_id: Optional[str]
    is_admin: bool
    is_anonymous: bool

    @property
    def has_identity(self) -> bool:
        return bool(self.canonical_id) and not self.is_anonymous


def fetch_current_user(auth_headers: Mapping[str, str]) -> UserIdentity:
    """
    Ask Domino Nucleus 'who am I?' using the provided auth headers.
    Returns a best-effort identity; never raises to callers.
    """
    try:
        resp = domino_get(WHO_AM_I_ENDPOINT, headers=dict(auth_headers))
        if resp.status_code != 200:
            logger.warning("WHOAMI non-200 (%s): %s", resp.status_code, resp.text[:200])
            return UserIdentity(username=None, canonical_id=None, is_admin=False, is_anonymous=True)

        payload: Dict[str, Any] = resp.json() if resp.content else {}
        ident = UserIdentity(
            username=payload.get("canonicalName"),
            canonical_id=payload.get("canonicalId") if not payload.get("isAnonymous") else None,
            is_admin=bool(payload.get("isAdmin", False)),
            is_anonymous=bool(payload.get("isAnonymous", True)),
        )
        logger.info("Caller username=%s admin=%s anon=%s", ident.username, ident.is_admin, ident.is_anonymous)
        return ident

    except Exception as e:
        logger.exception("WHOAMI call failed: %s", e)
        return UserIdentity(username=None, canonical_id=None, is_admin=False, is_anonymous=True)


def is_authorized_to_view_cluster(caller: UserIdentity, owner_id: Optional[str]) -> bool:
    """
    Authorization policy (generic across Ray/Dask/Spark):
      - Domino admins can view all clusters.
      - Otherwise, caller canonical_id must equal the cluster's owner_id label.
    """
    if caller.is_admin:
        is_auth = True
    else:
        is_auth = bool(caller.canonical_id) and caller.canonical_id == (owner_id or "")

    logger.info(
        "Authz check: caller_id=%s admin=%s owner_id=%s -> %s",
        caller.canonical_id, caller.is_admin, owner_id, is_auth
    )
    return is_auth


# ---------- Routes ----------

@ddl_cluster_scaler_api.get("/ddl_cluster_scaler/list/<kind>")
def list_clusters(kind: AllowedKind):
    """
    List clusters for the given CRD kind (plural): rayclusters|daskclusters|sparkclusters.
    Filters to those the caller can view.
    """
    logger.warning("GET /ddl_cluster_scaler/list/%s", kind)

    allowed = {"rayclusters", "daskclusters", "sparkclusters"}
    if kind not in allowed:
        return (
            jsonify(error="invalid_kind", message=f"kind must be one of {sorted(allowed)}"),
            400,
            NO_CACHE,
        )

    try:
        clusters: Dict[str, Any] = K8S_CUSTOM_OBJECTS.list_namespaced_custom_object(
            group=GROUP,
            version=VERSION,
            namespace=COMPUTE_NAMESPACE,
            plural=kind,
        )

        auth_headers = get_auth_headers(request.headers)
        caller = fetch_current_user(auth_headers)

        items: List[Dict[str, Any]] = clusters.get("items", [])
        visible: List[Dict[str, Any]] = []
        for obj in items:
            labels = obj.get("metadata", {}).get("labels", {}) or {}
            owner_id = labels.get(LABEL_OWNER_ID)
            if is_authorized_to_view_cluster(caller, owner_id):
                visible.append(obj)

        return (jsonify(kind=kind, count=len(visible), clusters=visible), 200, NO_CACHE)

    except Exception as e:
        logger.exception("Failed to list clusters for kind=%s: %s", kind, e)
        return (
            jsonify(error="list_failed", message="Failed to list clusters", details=str(e)),
            500,
            NO_CACHE,
        )


@ddl_cluster_scaler_api.get("/ddl_cluster_scaler/get/<kind>/<name>")
def get_cluster(kind: AllowedKind, name: str) -> object:
    """
    Fetch a specific cluster by kind + name.
    Only returns the object if the user is authorized.
    """
    logger.warning("GET /cluster/%s/%s", kind, name)

    allowed = {"rayclusters", "daskclusters", "sparkclusters"}
    if kind not in allowed:
        return (
            jsonify(error="invalid_kind", message=f"kind must be one of {sorted(allowed)}"),
            400,
            NO_CACHE,
        )

    try:
        out: Dict[str, Any] = K8S_CUSTOM_OBJECTS.get_namespaced_custom_object(
            group=GROUP,
            version=VERSION,
            namespace=COMPUTE_NAMESPACE,
            plural=kind,
            name=name,
        )

        labels = out.get("metadata", {}).get("labels", {}) or {}
        owner_id = labels.get(LABEL_OWNER_ID)

        auth_headers = get_auth_headers(request.headers)
        caller = fetch_current_user(auth_headers)
        if not is_authorized_to_view_cluster(caller, owner_id):
            return (
                jsonify(error="unauthorized", message=f"Not authorized to access cluster {name}"),
                403,
                NO_CACHE,
            )

        return (jsonify(out), 200, NO_CACHE)

    except Exception as e:
        logger.exception("Failed to fetch cluster kind=%s name=%s", kind, name)
        return (
            jsonify(error="get_failed", message=f"Failed to fetch cluster {name}", details=str(e)),
            500,
            NO_CACHE,
        )

from typing import Mapping

def get_hw_tiers(auth_headers: Dict[str, str], requested_hw_tier: str) -> Dict[str, Any]:
    """
    Fetch a single hardware tier by name from Domino Nucleus.
    Returns the tier object dict, or {} if not found / error.
    """
    try:
        # Expect caller to pass sanitized headers (Bearer or API key)
        if not auth_headers.get("X-Domino-Api-Key") and not auth_headers.get("Authorization"):
            logger.warning("No auth headers provided for HW TIERS call")
            return {}

        resp = domino_get("v4/hardwareTier", headers=auth_headers)
        if resp.status_code != 200:
            logger.warning("HW TIERS non-200 (%s): %s", resp.status_code, resp.text[:200])
            return {}

        payload: Dict[str, Any] = resp.json() if resp.content else {}
        for item in payload.get("hardwareTiers", []):
            if item.get("name") == requested_hw_tier:
                logger.info("Fetched hardware tier: %s", requested_hw_tier)
                return item

        logger.warning("Requested hardware tier not found: %s", requested_hw_tier)
        return {}

    except Exception as e:
        logger.exception("HW TIERS call failed: %s", e)
        return {}

def get_head_node_name(name: str) -> str:
    cluster_type = name.split("-")[0]
    return f"{name}-{cluster_type}-head-0"

@ddl_cluster_scaler_api.post("/ddl_cluster_scaler/restart-head/<kind>/<name>")
def restart_head_pod(name: str) -> Dict[str, Any]:
    """
    Initiate a restart of the head node by deleting its Pod.
    This function does NOT wait for the replacement to come up.

    Returns a small status dict; caller can log/inspect as needed.
    """
    pod_name = get_head_node_name(name)

    v1: CoreV1Api = CoreV1Api(K8S_API_CLIENT)  # reuse your module-level ApiClient
    namespace = COMPUTE_NAMESPACE
    grace_period_seconds = 20
    try:
        resp = v1.delete_namespaced_pod(
            name=pod_name,
            namespace=namespace,
            body=V1DeleteOptions(grace_period_seconds=grace_period_seconds),
        )
        logger.warning("head_restart initiated ns=%s pod=%s grace=%s",
                       namespace, pod_name, grace_period_seconds)
        return {
            "ok": True,
            "namespace": namespace,
            "pod": pod_name,
            "grace_period_seconds": grace_period_seconds,
            "k8s_status": getattr(resp, "status", None),
            "k8s_details": getattr(resp, "details", None),
        }
    except ApiException as e:
        # 404: already gone or wrong name/namespace
        if e.status == 404:
            logger.warning("head_restart not_found ns=%s pod=%s", namespace, pod_name)
            return {"ok": False, "namespace": namespace, "pod": pod_name, "error": "NotFound", "status": 404}
        logger.exception("head_restart api_error ns=%s pod=%s status=%s", namespace, pod_name, getattr(e, "status", None))
        return {"ok": False, "namespace": namespace, "pod": pod_name, "error": str(e), "status": getattr(e, "status", None)}
    except Exception as e:
        logger.exception("head_restart unexpected_error ns=%s pod=%s", namespace, pod_name)
        return {"ok": False, "namespace": namespace, "pod": pod_name, "error": str(e)}



@ddl_cluster_scaler_api.post("/ddl_cluster_scaler/scale/<kind>/<name>")
def scale_cluster(kind: AllowedKind, name: str) -> object:
    """
    Scale a cluster (kind in: rayclusters|daskclusters|sparkclusters).

    Body JSON:
      {
        "replicas": <int>,            # may be 0; capped to maxReplicas
        "worker_hw_tier_name": "<name>"    # optional; validates restrictions and applies resources/labels if present
      }
    """
    logger.warning("POST /ddl_cluster_scaler/scale/%s/%s", kind, name)

    allowed = {"rayclusters", "daskclusters", "sparkclusters"}
    if kind not in allowed:
        return jsonify(error="invalid_kind", message=f"kind must be one of {sorted(allowed)}"), 400, NO_CACHE

    payload = request.get_json(silent=True) or {}
    logger.info("scale_cluster payload kind=%s name=%s payload_keys=%s",
                 kind, name, list(payload.keys()))

    # replicas validation (allow 0)
    try:
        requested_replicas = int(payload.get("replicas"))
        if requested_replicas < 0:
            raise ValueError
    except Exception:
        logger.info("scale_cluster invalid_replicas kind=%s name=%s value=%r",
                       kind, name, payload.get("replicas"))
        return jsonify(error="bad_request", message="replicas must be a non-negative integer"), 400, NO_CACHE

    try:
        # Fetch object
        obj: Dict[str, Any] = K8S_CUSTOM_OBJECTS.get_namespaced_custom_object(
            group=GROUP, version=VERSION, namespace=COMPUTE_NAMESPACE, plural=kind, name=name
        )

        # Auth
        labels = (obj.get("metadata") or {}).get("labels") or {}
        owner_id = labels.get(LABEL_OWNER_ID)
        auth_headers = get_auth_headers(request.headers)
        caller = fetch_current_user(auth_headers)
        logger.info("authz_check kind=%s name=%s caller_admin=%s caller_id=%s owner_id=%s",
                     kind, name, caller.is_admin, caller.canonical_id, owner_id)
        if not is_authorized_to_view_cluster(caller, owner_id):
            logger.info("scale_cluster unauthorized kind=%s name=%s", kind, name)
            return jsonify(error="unauthorized", message=f"Not authorized to update cluster {name}"), 403, NO_CACHE

        # Autoscaling checks
        spec = obj.get("spec") or {}
        autoscaling = spec.get("autoscaling")
        if not isinstance(autoscaling, dict):
            logger.warning("scale_cluster not_scalable kind=%s name=%s reason=no_autoscaling", kind, name)
            return jsonify(error="not_scalable", message="Cannot scale this cluster: autoscaling not enabled"), 409, NO_CACHE

        try:
            max_replicas = int(autoscaling.get("maxReplicas"))
        except Exception:
            logger.warning("scale_cluster invalid_autoscaling kind=%s name=%s maxReplicas=%r",
                           kind, name, autoscaling.get("maxReplicas") if isinstance(autoscaling, dict) else None)
            return jsonify(error="invalid_autoscaling", message="autoscaling.maxReplicas is missing or invalid"), 409, NO_CACHE

        # Cap to [0, maxReplicas]; do NOT change maxReplicas
        effective_replicas = min(max(requested_replicas, 0), max_replicas)
        logger.info("replica_plan kind=%s name=%s requested=%d effective=%d max=%d",
                    kind, name, requested_replicas, effective_replicas, max_replicas)

        # Optional: apply worker hardware tier resources
        worker_tier_name = payload.get("worker_hw_tier_name")
        worker_tier: Dict[str, Any] = {}
        hwt_resources: Dict[str, Any] = {}
        gpu_configuration: Dict[str, Any] = {}
        nodepool = ""
        if worker_tier_name:
            logger.info("hw_tier_requested kind=%s name=%s tier=%s",
                        kind, name, worker_tier_name)
            worker_tier = get_hw_tiers(auth_headers=auth_headers, requested_hw_tier=worker_tier_name)
            if not worker_tier:
                logger.warning("hw_tier_not_found kind=%s name=%s tier=%s",
                               kind, name, worker_tier_name)
                return jsonify(error="invalid_hw_tier", message=f"Hardware tier not found: {worker_tier_name}"), 400, NO_CACHE

            # Validate restrictions: if any 'restrictTo*' is True, kind must match one of them.
            ccr = worker_tier.get("computeClusterRestrictions") or {}
            restrict_flags = {
                "sparkclusters": bool(ccr.get("restrictToSpark", False)),
                "rayclusters":   bool(ccr.get("restrictToRay",   False)),
                "daskclusters":  bool(ccr.get("restrictToDask",  False)),
                # note: if tier is MPI-only, this will reject all current kinds
                "mpicluster":    bool(ccr.get("restrictToMpi",   False)),
            }
            logger.info("hw_tier_restrictions kind=%s name=%s tier=%s flags=%s",
                         kind, name, worker_tier_name, restrict_flags)
            if any(restrict_flags.values()) and not restrict_flags.get(kind, False):
                allowed_kinds = [k for k, v in restrict_flags.items() if v]
                logger.warning("hw_tier_restricted kind=%s name=%s tier=%s allowed=%s",
                               kind, name, worker_tier_name, allowed_kinds)
                return (
                    jsonify(
                        error="invalid_hw_tier",
                        message=f"Hardware tier '{worker_tier_name}' is restricted to {allowed_kinds}",
                    ),
                    400,
                    NO_CACHE,
                )

            # Extract resource hints from the tier (guard for missing keys)
            nodepool = worker_tier.get("nodePool")
            hwt_resources = worker_tier.get("hwtResources") or {}
            gpu_configuration = (worker_tier.get("gpuConfiguration") or {}) if worker_tier else {}
            logger.info("hw_tier_resources kind=%s name=%s tier=%s cores=%r coresLimit=%r mem=%r memLimit=%r gpus=%r",
                        kind, name, worker_tier_name,
                        hwt_resources.get("cores"),
                        hwt_resources.get("coresLimit"),
                        hwt_resources.get("memory"),
                        hwt_resources.get("memoryLimit"),
                        gpu_configuration.get("numberOfGpus") if isinstance(gpu_configuration, dict) else None)

        # Build a minimal strategic-merge patch
        #patch_body: Dict[str, Any] = {"spec": {"autoscaling": {"minReplicas": effective_replicas,"maxReplicas": effective_replicas}}}
        patch_body: Dict[str, Any] = {
            "spec": {"autoscaling": {"minReplicas": effective_replicas}}}

        # If worker stanza exists, update it (replicas and optional resources/labels)
        worker_spec = spec.get("worker")
        if isinstance(worker_spec, dict):
            worker_patch = patch_body["spec"].setdefault("worker", {})
            worker_patch["replicas"] = effective_replicas

            if worker_tier_name and worker_tier:
                node_selector = worker_patch.setdefault("nodeSelector", {})
                labels_patch    = worker_patch.setdefault("labels", {})
                resources_patch = worker_patch.setdefault("resources", {})
                limits_patch    = resources_patch.setdefault("limits", {})
                requests_patch  = resources_patch.setdefault("requests", {})

                # Label with the tier id (if present)
                tier_id = worker_tier.get("id")
                if tier_id:
                    labels_patch["dominodatalab.com/hardware-tier-id"] = str(tier_id)
                    node_selector["dominodatalab.com/node-pool"] = nodepool

                # CPU (prefer explicit cores/coresLimit if provided)
                cores = hwt_resources.get("cores")
                cores_limit = hwt_resources.get("coresLimit")
                if cores is not None:
                    requests_patch["cpu"] = str(cores)
                if cores_limit is not None:
                    limits_patch["cpu"] = str(cores_limit)

                # Memory helper
                def _mem_to_qty(m: Optional[Dict[str, Any]]) -> Optional[str]:
                    if not isinstance(m, dict):
                        return None
                    v = m.get("value")
                    u = m.get("unit")
                    if v is None or u is None:
                        return None
                    unit_abbrev = str(u).strip()[:2]  # e.g., "GiB" -> "Gi"
                    return f"{v}{unit_abbrev}" if unit_abbrev else None

                mem_req = _mem_to_qty(hwt_resources.get("memory"))
                mem_lim = _mem_to_qty(hwt_resources.get("memoryLimit"))
                if mem_req:
                    requests_patch["memory"] = mem_req
                if mem_lim:
                    limits_patch["memory"] = mem_lim

                # GPUs (nvidia.com/gpu expects an integer)
                if isinstance(gpu_configuration, dict):
                    count = gpu_configuration.get("numberOfGpus")
                    if isinstance(count, int) and count >= 0:
                        requests_patch["nvidia.com/gpu"] = count
                        limits_patch["nvidia.com/gpu"] = count

                logger.info("hw_tier_patch_applied kind=%s name=%s tier=%s "
                            "cpu_req=%s cpu_lim=%s mem_req=%s mem_lim=%s gpus=%s label_id_set=%s",
                            kind, name, worker_tier,
                            requests_patch.get("cpu"), limits_patch.get("cpu"),
                            requests_patch.get("memory"), limits_patch.get("memory"),
                            requests_patch.get("nvidia.com/gpu"),
                            "dominodatalab.com/hardware-tier-id" in labels_patch)

        else:
            if worker_tier:
                logger.info("hw_tier_requested_but_no_worker_spec kind=%s name=%s tier=%s",
                               kind, name, worker_tier)

        # Apply the patch
        logger.debug("k8s_patch kind=%s name=%s patch_keys=%s", kind, name, list(patch_body.get("spec", {}).keys()))
        patched = K8S_CUSTOM_OBJECTS.patch_namespaced_custom_object(
            group=GROUP, version=VERSION, namespace=COMPUTE_NAMESPACE, plural=kind, name=name, body=patch_body
        )
        logger.info("k8s_patch_success kind=%s name=%s requested=%d effective=%d max=%d tier=%s",
                    kind, name, requested_replicas, effective_replicas, max_replicas, worker_tier or "None")


        logger.info("Initiated restart of the head node for  kind=%s name=%s",
                    kind, name)
        return (
            jsonify(
                kind=kind,
                name=name,
                requested_replicas=requested_replicas,
                effective_replicas=effective_replicas,
                maxReplicas=max_replicas,
                capped=(requested_replicas > max_replicas),
                worker_hw_tier_id=worker_tier or None,
                object=patched,
            ),
            200,
            NO_CACHE,
        )

    except Exception as e:
        logger.exception("Failed to scale cluster kind=%s name=%s", kind, name)
        return jsonify(error="scale_failed", message=f"Failed to scale cluster {name}", details=str(e)), 500, NO_CACHE





def _parse_started_at(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"  # make it ISO 8601 tz-aware
    try:
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None

def _is_ready(pod) -> bool:
    conds = (pod.status and pod.status.conditions) or []
    return any(c.type == "Ready" and c.status == "True" for c in conds)

@ddl_cluster_scaler_api.get("/ddl_cluster_scaler/head/restart_status/<namespace>/<pod_name>")
def head_restart_status(namespace: str, pod_name: str):
    """
    Did the head pod restart since the given timestamp?

    Query params (required/optional):
      started_at     REQUIRED ISO-8601 time (e.g., 2025-09-02T15:40:00Z)
      require_ready  optional bool (default: true). If true, ok=true only if pod is Ready.
    """
    started_at_raw = request.args.get("started_at")
    require_ready = (request.args.get("require_ready", "true").lower() in {"true", "1", "yes"})

    started_at = _parse_started_at(started_at_raw)
    if not started_at:
        return (
            jsonify(error="bad_request", message="started_at (ISO-8601) is required"),
            400,
            NO_CACHE,
        )

    try:
        v1 = CoreV1Api(K8S_API_CLIENT)
        pod = v1.read_namespaced_pod(pod_name, namespace)
    except client.exceptions.ApiException as e:
        if e.status == 404:
            return (jsonify(ok=False, reason="head pod not found", namespace=namespace, pod=pod_name), 404, NO_CACHE)
        logger.exception("head_restart_status k8s error ns=%s pod=%s", namespace, pod_name)
        return (jsonify(error="k8s_error", status=e.status, message=str(e)), 500, NO_CACHE)
    except Exception as e:
        logger.exception("head_restart_status unexpected error ns=%s pod=%s", namespace, pod_name)
        return (jsonify(error="unexpected_error", message=str(e)), 500, NO_CACHE)

    cts = pod.metadata.creation_timestamp  # tz-aware
    restarted = bool(cts and cts >= started_at)
    ready_ok = _is_ready(pod) if require_ready else True
    ok = restarted and ready_ok

    return (
        jsonify(
            ok=ok,
            restarted=restarted,
            ready=ready_ok,
            reason=None if ok else ("not_restarted" if not restarted else "not_ready"),
            namespace=namespace,
            pod=pod_name,
            creationTimestamp=cts.isoformat() if cts else None,
            started_at=started_at.isoformat(),
            require_ready=require_ready,
            evaluated_with="started_at",
        ),
        200,
        NO_CACHE,
    )

