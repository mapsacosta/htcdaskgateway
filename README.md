# lpcdaskgateway
A Dask Gateway client extension for heterogeneous cluster mode combining the Kubernetes backend with the CMS Tier 1 HTCondor batch pool

## Basic usage @ Fermilab [EAF](https://analytics-hub.fnal.gov)
* Make sure the notebook launched supports this functionality (COFFEA-DASK notebook)

```
from lpcdaskgateway import LPCGateway

gateway = LPCGateway()
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
