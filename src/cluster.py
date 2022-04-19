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

class LPCGatewayCluster(GatewayCluster):
    
    def __init__(self, **kwargs):
        # These settings are static
        config = {
            "gateway": {
                "address": "http://172.30.227.32",
                "auth": {"type": "jupyterhub"},
            }
        }
        dask.config.set(config)
        super().__init__(address=gw_address, auth=JupyterHubAuth, **kwargs)


    def scale_cluster(self, n):
        """Scale a cluster to n workers.
        Parameters
        ----------
        cluster_name : str
            The cluster name.
        n : int
            The number of workers to scale to.
        """
#        username = pwd.getpwuid( os.getuid() )[ 0 ]
        username = 'macosta'
        security = self.cluster.security
        tmproot = f"/uscmst1b_scratch/lpc1/3DayLifetime/{username}/{cluster_name}"
        condor_logdir = f"{tmproot}/condor"
        credentials_dir = f"{tmproot}/dask-credentials"
        worker_space_dir = f"{tmproot}/dask-worker-space"
        image_name = f"/cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-dask-cc7-gateway:0.7.12-fastjet-3.3.4.0rc9-g8a990fa"
        os.makedirs(tmproot, exist_ok=True)
        os.makedirs(condor_logdir, exist_ok=True)
        os.makedirs(credentials_dir, exist_ok=True)
        os.makedirs(worker_space_dir, exist_ok=True)
        
        with open(f"{credentials_dir}/dask.crt", 'w') as f:
            f.write(security.tls_cert)
        with open(f"{credentials_dir}/dask.pem", 'w') as f:
            f.write(security.tls_key)
        with open(f"{credentials_dir}/api-token", 'w') as f:
            f.write(os.environ['JUPYTERHUB_API_TOKEN'])
        
        # Prepare JDL
        jdl = """executable = start.sh
arguments = """+cluster_name+""" htcdask-worker_$(Cluster)_$(Process)
output = condor/htcdask-worker$(Cluster)_$(Process).out
error = condor/htcdask-worker$(Cluster)_$(Process).err
log = condor/htcdask-worker$(Cluster)_$(Process).log
requirements = TARGET.FERMIHTC_EAFWorker=?=True
request_cpus = 4
request_memory = 2100
should_transfer_files = yes
+FERMIHTC_EAFJob = True
transfer_input_files = """+credentials_dir+""", """+worker_space_dir+""" , """+condor_logdir+"""
Queue """+str(n)+""
    
        with open(f"{tmproot}/htcdask_submitfile.jdl", 'w+') as f:
            f.writelines(jdl)
            
        # Prepare singularity command
        sing = """#!/bin/bash
export SINGULARITYENV_DASK_GATEWAY_WORKER_NAME=$2
export SINGULARITYENV_DASK_GATEWAY_API_URL="https://dask-gateway-api.fnal.gov/api"
export SINGULARITYENV_DASK_GATEWAY_CLUSTER_NAME=$1
export SINGULARITYENV_DASK_GATEWAY_API_TOKEN=/etc/dask-credentials/api-token
export SINGULARITYENV_DASK_DISTRIBUTED__LOGGING__DISTRIBUTED="debug"

worker_space_dir=${PWD}/dask-worker-space/$2
mkdir $worker_space_dir

singularity exec -B ${worker_space_dir}:/srv/dask-worker-space -B dask-credentials:/etc/dask-credentials /cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-dask-cc7-gateway:0.7.12-fastjet-3.3.4.0rc9-g8a990fa \
dask-worker --name $2 --tls-ca-file /etc/dask-credentials/dask.crt --tls-cert /etc/dask-credentials/dask.crt --tls-key /etc/dask-credentials/dask.pem --worker-port 10000:10070 --no-nanny --no-dashboard --local-directory /srv --nthreads 1 --nprocs 1 tls://dask-gateway-tls.fnal.gov:443"""
    
        with open(f"{tmproot}/start.sh", 'w+') as f:
            f.writelines(sing)
        os.chmod(f"{tmproot}/start.sh", 0o775)
        
        # Submitting HTCondor job(s)
        import subprocess
        import sys

        # We add this to avoid a bug on Farruk's condor_submit wrapper (a fix is in progress)
        os.environ['LS_COLORS']="ExGxBxDxCxEgEdxbxgxcxd"

        # Submit our jdl, print the result and call the cluster widget
        result = subprocess.check_output(['sh','-c','/usr/local/bin/condor_submit htcdask_submitfile.jdl'], cwd=tmproot)
        
        return result
    
    async def _start(self):
        try:
            await self.cluster = gateway.new_cluster()
        except OSError:
            raise RuntimeError(
                f"Likely failed to bind to local port {self._port}, try rerunning"
            )

    async def _close(self):
        await super()._close()
        await self.loop.run_in_executor(None, self._clean_scratch)

class LPCGatewayCluster(GatewayCluster):
    schedd_safe_paths = [
        os.path.expanduser("~"),
        "/uscmst1b_scratch/lpc1/3DayLifetime",
        "/uscms_data",
    ]

    def __init__(self, **kwargs):
        # These settings are static
        config = {
            "gateway": {
                "address": "http://127.0.0.1:8888",
                "public-address": None,
                "proxy-address": 8786,
                "auth": {"type": "basic", "kwargs": {"username": "bruce"}},
                "http-client": {"proxy": None},
            }
        }
 
        dask.config.set(config)
        self.gateway = Gateway()
        gw_address="http://172.30.227.32",
        gw_auth='jupyterhub'
        super().__init__(address=gw_address, auth=JupyterHubAuth, **kwargs)


    def scale_cluster(self, n):
        """Scale a cluster to n workers.
        Parameters
        ----------
        cluster_name : str
            The cluster name.
        n : int
            The number of workers to scale to.
        """
#        username = pwd.getpwuid( os.getuid() )[ 0 ]
        username = 'macosta'
        security = self.cluster.security
        tmproot = f"/uscmst1b_scratch/lpc1/3DayLifetime/{username}/{cluster_name}"
        condor_logdir = f"{tmproot}/condor"
        credentials_dir = f"{tmproot}/dask-credentials"
        worker_space_dir = f"{tmproot}/dask-worker-space"
        image_name = f"/cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-dask-cc7-gateway:0.7.12-fastjet-3.3.4.0rc9-g8a990fa"
        os.makedirs(tmproot, exist_ok=True)
        os.makedirs(condor_logdir, exist_ok=True)
        os.makedirs(credentials_dir, exist_ok=True)
        os.makedirs(worker_space_dir, exist_ok=True)
        
        with open(f"{credentials_dir}/dask.crt", 'w') as f:
            f.write(security.tls_cert)
        with open(f"{credentials_dir}/dask.pem", 'w') as f:
            f.write(security.tls_key)
        with open(f"{credentials_dir}/api-token", 'w') as f:
            f.write(os.environ['JUPYTERHUB_API_TOKEN'])
        
        # Prepare JDL
        jdl = """executable = start.sh
arguments = """+cluster_name+""" htcdask-worker_$(Cluster)_$(Process)
output = condor/htcdask-worker$(Cluster)_$(Process).out
error = condor/htcdask-worker$(Cluster)_$(Process).err
log = condor/htcdask-worker$(Cluster)_$(Process).log
requirements = TARGET.FERMIHTC_EAFWorker=?=True
request_cpus = 4
request_memory = 2100
should_transfer_files = yes
+FERMIHTC_EAFJob = True
transfer_input_files = """+credentials_dir+""", """+worker_space_dir+""" , """+condor_logdir+"""
Queue """+str(n)+""
    
        with open(f"{tmproot}/htcdask_submitfile.jdl", 'w+') as f:
            f.writelines(jdl)
            
        # Prepare singularity command
        sing = """#!/bin/bash
export SINGULARITYENV_DASK_GATEWAY_WORKER_NAME=$2
export SINGULARITYENV_DASK_GATEWAY_API_URL="https://dask-gateway-api.fnal.gov/api"
export SINGULARITYENV_DASK_GATEWAY_CLUSTER_NAME=$1
export SINGULARITYENV_DASK_GATEWAY_API_TOKEN=/etc/dask-credentials/api-token
export SINGULARITYENV_DASK_DISTRIBUTED__LOGGING__DISTRIBUTED="debug"

worker_space_dir=${PWD}/dask-worker-space/$2
mkdir $worker_space_dir

singularity exec -B ${worker_space_dir}:/srv/dask-worker-space -B dask-credentials:/etc/dask-credentials /cvmfs/unpacked.cern.ch/registry.hub.docker.com/coffeateam/coffea-dask-cc7-gateway:0.7.12-fastjet-3.3.4.0rc9-g8a990fa \
dask-worker --name $2 --tls-ca-file /etc/dask-credentials/dask.crt --tls-cert /etc/dask-credentials/dask.crt --tls-key /etc/dask-credentials/dask.pem --worker-port 10000:10070 --no-nanny --no-dashboard --local-directory /srv --nthreads 1 --nprocs 1 tls://dask-gateway-tls.fnal.gov:443"""
    
        with open(f"{tmproot}/start.sh", 'w+') as f:
            f.writelines(sing)
        os.chmod(f"{tmproot}/start.sh", 0o775)
        
        # Submitting HTCondor job(s)
        import subprocess
        import sys

        # We add this to avoid a bug on Farruk's condor_submit wrapper (a fix is in progress)
        os.environ['LS_COLORS']="ExGxBxDxCxEgEdxbxgxcxd"

        # Submit our jdl, print the result and call the cluster widget
        result = subprocess.check_output(['sh','-c','/usr/local/bin/condor_submit htcdask_submitfile.jdl'], cwd=tmproot)
        
        return result
    
    async def _start(self):
        try:
            await self.cluster = gateway.new_cluster()
        except OSError:
            raise RuntimeError(
                f"Likely failed to bind to local port {self._port}, try rerunning"
            )

    async def _close(self):
        await super()._close()
        await self.loop.run_in_executor(None, self._clean_scratch)