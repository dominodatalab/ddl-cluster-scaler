import logging
import os
from typing import Optional

from flask import Flask, jsonify
from ddl_cluster_scaler_api import ddl_cluster_scaler_api


# ---------- Config ----------

class Config:
    """App config sourced from environment variables."""
    HOST: str = os.getenv("FLASK_HOST", "0.0.0.0")
    PORT: int = int(os.getenv("FLASK_PORT", "6000"))
    DEBUG: bool = os.getenv("FLASK_DEBUG", "0") in ("1", "true", "True")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "WARNING")


def _coerce_log_level(level_name: str) -> int:
    """Map string level to logging constant with sane fallback."""
    level = logging.getLevelName(level_name.upper())
    return level if isinstance(level, int) else logging.WARNING


def configure_logging(level_name: str) -> None:
    """Configure root logger once."""
    logging.basicConfig(
        level=_coerce_log_level(level_name),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


# ---------- App Factory ----------

def create_app(config: Optional[Config] = None) -> Flask:
    cfg = config or Config()
    configure_logging(cfg.LOG_LEVEL)

    app = Flask(__name__)
    app.config.from_object(cfg)

    # Blueprints
    app.register_blueprint(ddl_cluster_scaler_api)

    # Health endpoints (JSON, cache-safe)
    @app.get("/healthz")
    def healthz():
        return jsonify(status="healthy"), 200, {"Cache-Control": "no-store"}

    # Optional: readiness separate from liveness
    @app.get("/readyz")
    def readyz():
        # add quick dependency checks here if needed
        return jsonify(status="ready"), 200, {"Cache-Control": "no-store"}

    # Example: log at startup once app exists
    app.logger.info("Cluster Scaler API initialized")
    return app


# ---------- CLI Runner (dev only) ----------

if __name__ == "__main__":
    cfg = Config()
    app = create_app(cfg)
    app.run(host=cfg.HOST, port=cfg.PORT, debug=cfg.DEBUG)
