import asyncio
import logging
import os
import random
import shutil
import socket
import sys
import pwd
import tempfile
import weakref

import dask
import yaml
from dask_jobqueue.htcondor import (
    quote_arguments,
    quote_environment,
)
 
from distributed.core import Status
from dask_gateway import Gateway
from dask_gateway import GatewayCluster
from dask_gateway import JupyterHubAuth

class LPCGateway(Gateway):
    
    def __init__(self, **kwargs):
        # These settings are static
        address="http://172.30.227.32"
        config = {
            "gateway": {
                "address": address,
                "auth": {"type": "jupyterhub"},
            }
        }
        dask.config.set(config)
        super().__init__(address=address, auth=JupyterHubAuth, **kwargs)

    async def _scale_cluster(self, cluster_name, n):
        url = f"{self.address}/api/v1/clusters/{cluster_name}/scale"
        resp = await self._request("POST", url, json={"count": n})
        try:
            msg = await resp.json()
        except Exception:
            msg = {}
        if not msg.get("ok", True) and msg.get("msg"):
            warnings.warn(GatewayWarning(msg["msg"]))

    def scale_cluster(self, cluster_name, n, **kwargs):
        """Scale a cluster to n workers.
        Parameters
        ----------
        cluster_name : str
            The cluster name.
        n : int
            The number of workers to scale to.
        """
        return self.sync(self._scale_cluster, cluster_name, n, **kwargs)

    async def _adapt_cluster(
        self, cluster_name, minimum=None, maximum=None, active=True
    ):
        resp = await self._request(
            "POST",
            f"{self.address}/api/v1/clusters/{cluster_name}/adapt",
            json={"minimum": minimum, "maximum": maximum, "active": active},
        )
        try:
            msg = await resp.json()
        except Exception:
            msg = {}
        if not msg.get("ok", True) and msg.get("msg"):
            warnings.warn(GatewayWarning(msg["msg"]))

    def adapt_cluster(
        self, cluster_name, minimum=None, maximum=None, active=True, **kwargs
    ):
        """Configure adaptive scaling for a cluster.
        Parameters
        ----------
        cluster_name : str
            The cluster name.
        minimum : int, optional
            The minimum number of workers to scale to. Defaults to 0.
        maximum : int, optional
            The maximum number of workers to scale to. Defaults to infinity.
        active : bool, optional
            If ``True`` (default), adaptive scaling is activated. Set to
            ``False`` to deactivate adaptive scaling.
        """
        return self.sync(
            self._adapt_cluster,
            cluster_name,
            minimum=minimum,
            maximum=maximum,
            active=active,
            **kwargs,
        )