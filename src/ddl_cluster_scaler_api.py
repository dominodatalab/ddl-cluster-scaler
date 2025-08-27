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
from kubernetes.client import ApiClient, CustomObjectsApi
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter


# ---------- Logging (configure once) ----------

def _coerce_log_level(name: str) -> int:
    lvl = logging.getLevelName(str(name).upper())
    return lvl if isinstance(lvl, int) else logging.WARNING


LOG_LEVEL: Final[int] = _coerce_log_level(os.getenv("LOG_LEVEL", "WARNING"))
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

DOMINO_NUCLEUS_URI_RAW = os.getenv("DOMINO_NUCLEUS_URI", "http://nucleus-frontend.domino-platform:80")
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


@ddl_cluster_scaler_api.post("/ddl_cluster_scaler/scale/<kind>/<name>")
def scale_cluster(kind: AllowedKind, name: str) -> object:
    """
    Scale a cluster (kind in: rayclusters|daskclusters|sparkclusters).

    Body JSON:
      { "replicas": <int> }   # may be 0; capped to maxReplicas; never increases maxReplicas
    """
    logger.warning("POST /ddl_cluster_scaler/scale/%s/%s", kind, name)

    allowed = {"rayclusters", "daskclusters", "sparkclusters"}
    if kind not in allowed:
        return (
            jsonify(error="invalid_kind", message=f"kind must be one of {sorted(allowed)}"),
            400,
            NO_CACHE,
        )

    payload = request.get_json(silent=True) or {}
    replicas_raw = payload.get("replicas")

    # Validate input: allow 0; disallow negatives and non-ints
    try:
        requested_replicas = int(replicas_raw)
        if requested_replicas < 0:
            raise ValueError("replicas must be >= 0")
    except Exception:
        return (
            jsonify(error="bad_request", message="replicas must be a non-negative integer"),
            400,
            NO_CACHE,
        )

    try:
        # Fetch object
        obj: Dict[str, Any] = K8S_CUSTOM_OBJECTS.get_namespaced_custom_object(
            group=GROUP,
            version=VERSION,
            namespace=COMPUTE_NAMESPACE,
            plural=kind,
            name=name,
        )

        # Auth
        labels = obj.get("metadata", {}).get("labels", {}) or {}
        owner_id = labels.get(LABEL_OWNER_ID)
        auth_headers = get_auth_headers(request.headers)
        caller = fetch_current_user(auth_headers)
        if not is_authorized_to_view_cluster(caller, owner_id):
            return (
                jsonify(error="unauthorized", message=f"Not authorized to update cluster {name}"),
                403,
                NO_CACHE,
            )

        # Ensure autoscaling + valid maxReplicas
        spec = obj.get("spec") or {}
        autoscaling = spec.get("autoscaling")
        if not isinstance(autoscaling, dict):
            return (
                jsonify(error="not_scalable", message="Cannot scale this cluster: autoscaling not enabled"),
                409,
                NO_CACHE,
            )

        try:
            max_replicas = int(autoscaling.get("maxReplicas"))
        except Exception:
            return (
                jsonify(
                    error="invalid_autoscaling",
                    message="Cannot scale: autoscaling.maxReplicas is missing or invalid",
                ),
                409,
                NO_CACHE,
            )

        # Cap to [0, maxReplicas]; allow 0 explicitly; DO NOT change maxReplicas
        effective_replicas = min(max(requested_replicas, 0), max_replicas)

        # Minimal strategic-merge patch: set minReplicas (and worker.replicas if present)
        patch_body: Dict[str, Any] = {"spec": {"autoscaling": {"minReplicas": effective_replicas}}}
        if isinstance(spec.get("worker"), dict):
            patch_body["spec"]["worker"] = {"replicas": effective_replicas}

        patched = K8S_CUSTOM_OBJECTS.patch_namespaced_custom_object(
            group=GROUP,
            version=VERSION,
            namespace=COMPUTE_NAMESPACE,
            plural=kind,
            name=name,
            body=patch_body,
        )

        return (
            jsonify(
                kind=kind,
                name=name,
                requested_replicas=requested_replicas,
                effective_replicas=effective_replicas,
                maxReplicas=max_replicas,
                capped=(requested_replicas > max_replicas),
                object=patched,
            ),
            200,
            NO_CACHE,
        )

    except Exception as e:
        logger.exception("Failed to scale cluster kind=%s name=%s", kind, name)
        return (
            jsonify(error="scale_failed", message=f"Failed to scale cluster {name}", details=str(e)),
            500,
            NO_CACHE,
        )
