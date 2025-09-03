   
# Installation
Follow these instructions to install the Helm chart for this service


## New Install
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

helm upgrade -f ./values.yaml ddl-cluster-scaler helm/ddl-cluster-scaler -n ${field_namespace}
```

## Delete 

```shell
export field_namespace=domino-field
helm delete  ddl-cluster-scaler -n ${field_namespace}
```

## Testing the installation

Follow the [notebook](notebooks/ddl_cluster_scaling_client.ipynb) `notebook/ddl_cluster_scaling_client.ipynb` to test the installation.