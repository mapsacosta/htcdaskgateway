import asyncio
import logging
import os
import random
import shutil
import socket
import sys
import pwd
import tempfile
import subprocess
import weakref

import dask
import yaml
from dask_jobqueue.htcondor import (
    quote_arguments,
    quote_environment,
)

# @author Maria A. - mapsacosta
 
from distributed.core import Status
from dask_gateway import GatewayCluster

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger("lpcdaskgateway.GatewayCluster")

class LPCGatewayCluster(GatewayCluster):
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        logger.info(self.name)

    # We only want to override what's strictly necessary, scaling and adapting are the most important ones

    def scale(self, n, **kwargs):
        """Scale the cluster to ``n`` workers.
        Parameters
        ----------
        n : int
            The number of workers to scale to.
        """
        print("Hello, I am the interrupted scale method")
        print("I have two functions:")
        print("1. Communicate to the Gateway server the new cluster state")
        print("2. Call the scale_cluster method on my LPCGateway")
        print("In the future, I will allow for Kubernetes workers as well")
        self.batchWorkerJobs = []
        self.kubeWorkerJobs = []
        batchWorkers = True
        if batchWorkers:
            self.batchWorkerJobs.append(self.scale_batch_workers(n))
        elif kubeWorkers:
            self.kubeWorkerJobs.append(self.scale_kube_workers(n))
        return self.gateway.scale_cluster(self.name, n, **kwargs)
    
    def scale_batch_workers(self, n):
        username = pwd.getpwuid( os.getuid() )[ 0 ]
        security = self.security
        cluster_name = self.name
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
+FERMIHTC_HTCDaskCluster = """+cluster_name+"""
+FERMIHTC_HTCDaskClusterOwner = """+username+"""
request_cpus = 4
request_memory = 2100
should_transfer_files = yes
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
        
        print("Sandbox folder located at: "+tmproot)

        print("Submitting HTCondor job(s) for "+str(n)+" workers")

        # We add this to avoid a bug on Farruk's condor_submit wrapper (a fix is in progress)
        os.environ['LS_COLORS']="ExGxBxDxCxEgEdxbxgxcxd"

        # Submit our jdl, print the result and call the cluster widget
        result = subprocess.check_output(['sh','-c','/usr/local/bin/condor_submit htcdask_submitfile.jdl'], cwd=tmproot)
        logger.info(result)
        
    def scale_kube_workers(self, n):
        username = pwd.getpwuid( os.getuid() )[ 0 ]
        logger.info("[WIP] Feature to be added ")
        logger.info("[NOOP] Scaled "+str(n)+"Kube workers, startup may take uo to 30 seconds")

    def adapt(self, minimum=None, maximum=None, active=True, **kwargs):
        """Configure adaptive scaling for the cluster.
        Parameters
        ----------
        minimum : int, optional
            The minimum number of workers to scale to. Defaults to 0.
        maximum : int, optional
            The maximum number of workers to scale to. Defaults to infinity.
        active : bool, optional
            If ``True`` (default), adaptive scaling is activated. Set to
            ``False`` to deactivate adaptive scaling.
        """
        print("Hello, I am the interrupted adapt method")
        print("I have two functions:")
        print("1. Communicate to the Gateway server the new cluster state")
        print("2. Call the adapt_cluster method on my LPCGateway")
        
        return self.gateway.adapt_cluster(
            self.name, minimum=minimum, maximum=maximum, active=active, **kwargs
        )