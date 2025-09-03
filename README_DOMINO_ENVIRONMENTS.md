# Domino Environment Definitions

This document defines the Domino environments used for clusters and compute environments.

## Ray Cluster Environment

**Base Image** : `quay.io/domino/ray-cluster-environment:ray2.36.0-py3.10-domino6.0`

**Dockerfile** :

```Dockerfile
RUN pip install pyarrow==14.0.2
RUN pip install hyperopt
RUN pip uninstall -y bson
RUN pip install pymongo
RUN pip install -q "xgboost>=2.0,<3"
RUN pip install mlflow==2.18.0
RUN pip install --no-cache-dir ipython 
```

## Ray Compute Environment


**Base Image** : `quay.io/domino/domino-ray-environment:ubuntu22-py3.10-r4.4-ray2.36.0-domino6.0`

**Dockerfile** :

```Dockerfile
USER root
RUN apt update && apt install -y unixodbc unixodbc-dev
RUN /opt/conda/bin/pip install h11==0.16.0
RUN pip install pyarrow==14.0.2
RUN pip install hyperopt
RUN pip uninstall -y bson
RUN pip install pymongo
RUN pip install -q "xgboost>=2.0,<3"
RUN pip install hydra-core 
USER ubuntu
```

**Pluggable Workspace Tools** :

```yaml
jupyter:
  title: "Jupyter (Python, R, Julia)"
  iconUrl: "/assets/images/workspace-logos/Jupyter.svg"
  start: [ "/opt/domino/workspaces/jupyter/start" ]
  supportedFileExtensions: [ ".ipynb" ]
  httpProxy:
    port: 8888
    rewrite: false
    internalPath: "/{{ownerUsername}}/{{projectName}}/{{sessionPathComponent}}/{{runId}}/{{#if pathToOpen}}tree/{{pathToOpen}}{{/if}}"
    requireSubdomain: false
jupyterlab:
  title: "JupyterLab"
  iconUrl: "/assets/images/workspace-logos/jupyterlab.svg"
  start: [  "/opt/domino/workspaces/jupyterlab/start" ]
  httpProxy:
    internalPath: "/{{ownerUsername}}/{{projectName}}/{{sessionPathComponent}}/{{runId}}/{{#if pathToOpen}}tree/{{pathToOpen}}{{/if}}"
    port: 8888
    rewrite: false
    requireSubdomain: false
vscode:
  title: "vscode"
  iconUrl: "/assets/images/workspace-logos/vscode.svg"
  start: [ "/opt/domino/workspaces/vscode/start" ]
  httpProxy:
    port: 8888
    requireSubdomain: false
rstudio:
  title: "RStudio"
  iconUrl: "/assets/images/workspace-logos/Rstudio.svg"
  start: [ "/opt/domino/workspaces/rstudio/start" ]
  httpProxy:
    port: 8888
    requireSubdomain: false
```