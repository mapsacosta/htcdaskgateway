# HTCdaskGateway

* A Dask Gateway client extension for heterogeneous cluster mode combining the Kubernetes backend for pain-free scheduler networking, with COFFEA-powered HTCondor workers and/or OKD [coming soon].
* Latest [![PyPI version](https://badge.fury.io/py/htcdaskgateway.svg)](https://badge.fury.io/py/htcdaskgateway)
 is installed by default and deployed to the COFFEA-DASK notebook on EAF (https://analytics-hub.fnal.gov). A few lines will get you going!
* The current image for workers/schedulers is: coffeateam/coffea-dask-cc7-gateway:0.7.12-fastjet-3.3.4.0rc9-g8a990fa

## Basic usage @ Fermilab [EAF](https://analytics-hub.fnal.gov)
* Make sure the notebook launched supports this functionality (COFFEA-DASK notebook)

```
from htcdaskgateway import HTCGateway

gateway = HTCGateway()
cluster = gateway.new_cluster()

# Scale my cluster to 5 HTCondor workers
cluster.scale(5)

# Obtain a client for connecting to your cluster scheduler
# Your cluster should be ready to take requests
client = cluster.get_client()
client

# When computations are finished, shutdown the cluster
cluster.shutdown()
```
