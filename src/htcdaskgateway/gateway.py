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
import pprint
import subprocess
import dask
import yaml

# @author Maria A. - mapsacosta
 
from distributed.core import Status
from dask_gateway import Gateway
from .cluster import HTCGatewayCluster

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger("htcdaskgateway.HTCGateway")


class HTCGateway(Gateway):
    
    def __init__(self, **kwargs):
        address = kwargs.pop('address', 'http://traefik-dask-gateway.dask-gateway.svc.cluster.local')
        super().__init__(address, auth="jupyterhub", **kwargs)
    
    def new_cluster(self, cluster_options=None, shutdown_on_close=True, **kwargs):
        """
        Submit a new cluster to the gateway, and wait for it to be started.
        Same as calling ``submit`` and ``connect`` in one go.
        Parameters
        ----------
        cluster_options : dask_gateway.options.Options, optional
            An ``Options`` object describing the desired cluster configuration.
            
            Usage: options = gateway.cluster_options()
            Opens a gui to configure the options object with allowed available options.
            
            If ``cluster_options`` is provided, these are applied afterwards as overrides. Available 
            options are specific to each deployment of dask-gateway, see **kwargs.
        shutdown_on_close : bool, optional
            If True (default), the cluster will be automatically shutdown on
            close. Set to False to have cluster persist until explicitly
            shutdown.
        **kwargs :
            Additional cluster configuration options.

            image_registry: str
                Image registry for chosen image as exemplified here: {registry}/{image_repo}/{image_name}.
                Default is registry.hub.docker.com. (Optional for docker images) 
            apptainer_image: str
                Image name from a chosen repo as exemplified here: {registry}/{image_repo}/{image_name}. 
                Default is coffeateam/coffea-base-almalinux8:0.7.22-py3.10. (Optional for notebooks with 
                default coffea install 0.7.22)
            worker_memory: float
                Desired memory for scheduler/workers in GB. Must be in range 1-8. Individual worker memory 
                is found by dividing worker_memory by worker_cores. Default is 4 GB. (Optional)
            worker_cores: int
                Desired number of cores for scheduler/workers. Must be in range 1-4. Default is 2 cores. 
                (Optional)
            
        Returns
        -------
        cluster : GatewayCluster
        """
        logger.info(" Creating HTCGatewayCluster ")

        try:
            return HTCGatewayCluster(
                address=self.address,
                proxy_address=self.proxy_address,
                public_address='https://dask-gateway.fnal.gov',
                auth=self.auth,
                asynchronous=self.asynchronous,
                loop=self.loop,
                shutdown_on_close=shutdown_on_close,
                cluster_options = cluster_options,
                **kwargs,
            )
        except Exception as e:
            msg = str(e) + ". See usage: " + '\n' + HTCGateway.new_cluster.__doc__
            raise Exception(msg) from e
            

    def scale_cluster(self, cluster_name, n, worker_type, **kwargs):
        """Scale a cluster to n workers.
        Parameters
        ----------
        cluster_name : str
            The cluster name.
        n : int
            The number of workers to scale to.
        """
        return self.sync(self._scale_cluster, cluster_name, n, **kwargs)
    
    async def _stop_cluster(self, cluster_name):
        url = f"{self.address}/api/v1/clusters/{cluster_name}"
        await self._request("DELETE", url)
        HTCGatewayCluster.from_name(cluster_name).close(shutdown=True)

    def stop_cluster(self, cluster_name, **kwargs):
        """Stop a cluster.
        Parameters
        ----------
        cluster_name : str
            The cluster name.
        """
        return self.sync(self._stop_cluster, cluster_name, **kwargs)
    
    async def _cluster_report(self, cluster_name, wait=False):
        params = "?wait" if wait else ""
        url = f"{self.address}/api/v1/clusters/{cluster_name}{params}"
        print(url)
        resp = await self._request("GET", url)
        data = await resp.json()
        print(data)
        return ClusterReport._from_json(self._public_address, self.proxy_address, data)
