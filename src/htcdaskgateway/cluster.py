import asyncio
import logging
import os
import socket
import sys
import pwd
import tempfile
import subprocess
import weakref
import pprint
import traceback

# @author Maria A. - mapsacosta
 
from distributed.core import Status
from dask_gateway import GatewayCluster

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger("htcdaskgateway.GatewayCluster")

class HTCGatewayCluster(GatewayCluster):
    
    def __init__(self, image_registry="registry.hub.docker.com", **kwargs):
        self.scheduler_proxy_ip = kwargs.pop('', '131.225.218.222')
        self.batchWorkerJobs = []
        self.defaultImage = 'coffeateam/coffea-base-almalinux8:0.7.22-py3.10'
        self.cluster_options = kwargs.get('cluster_options')
        self.image_registry = image_registry
        
        #set default image if the image is not specified by user
        if not kwargs.get('image') and (not self.cluster_options or not self.cluster_options.image):
            kwargs['image'] = self.defaultImage
            print("Apptainer_image: ", kwargs['image'])
            self.apptainer_image = self.defaultImage
        else:
            print("Apptainer_image: ", kwargs['image'])
            self.apptainer_image = kwargs.get('image')
            
        kwargs['image'] = self.image_registry + "/" + self.apptainer_image

        dir_command = "[ -d \"/cvmfs/unpacked.cern.ch/" + self.image_registry + "/" + self.apptainer_image + "\" ]" 
        if os.system(dir_command):
            sys.exit("Image not allowed. Images must be from /cvmfs/unpacked.cern.ch")

        super().__init__(**kwargs)
   
    # We only want to override what's strictly necessary, scaling and adapting are the most important ones
        
    async def _stop_async(self):
        self.destroy_all_batch_clusters()
        await super()._stop_async()

        self.status = "closed"
    
    def scale(self, n, **kwargs):
        """Scale the cluster to ``n`` workers.
        Parameters
        ----------
        n : int
            The number of workers to scale to.
        """
        #print("Hello, I am the interrupted scale method")
        #print("I have two functions:")
        #print("1. Communicate to the Gateway server the new cluster state")
        #print("2. Call the scale_cluster method on my LPCGateway")
        #print("In the future, I will allow for Kubernetes workers as well"
        worker_type = 'htcondor'
        logger.warn(" worker_type: "+str(worker_type))
        try:
            if 'condor' in worker_type:
                self.batchWorkerJobs = []
                logger.info(" Scaling: "+str(n)+" HTCondor workers")
                self.batchWorkerJobs.append(self.scale_batch_workers(n))
                logger.debug(" New Cluster state ")
                logger.debug(self.batchWorkerJobs)
                return self.gateway.scale_cluster(self.name, n, **kwargs)

        except: 
            print(traceback.format_exc())
            logger.error("A problem has occurred while scaling via HTCondor, please check your proxy credentials")
            return False
    
    def scale_batch_workers(self, n):
        username = pwd.getpwuid( os.getuid() )[ 0 ]
        x509_file = f"x509up_u{os.getuid()}"
        security = self.security
        cluster_name = self.name
        tmproot = f"/uscmst1b_scratch/lpc1/3DayLifetime/{username}/{cluster_name}"
        condor_logdir = f"{tmproot}/condor"
        credentials_dir = f"{tmproot}/dask-credentials"
        worker_space_dir = f"{tmproot}/dask-worker-space"

        image_name = "/cvmfs/unpacked.cern.ch/" + self.image_registry + "/" + self.apptainer_image
        
        logger.info("Creating with image " + image_name)

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
            
        # Just pick a random Schedd
        #schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd)
            
        #schedd = htcondor.Schedd()
        #sub = htcondor.Submit({
        #    "executable": "/bin/sleep",
        #    "arguments": "5m",
        #    "hold": "True",
        #})
        #submit_result = schedd.submit(sub, count=10)
        #print(submit_result.cluster())
        #+FERMIHTC_HTCDaskCluster = """+cluster_name+"""
        #+FERMIHTC_HTCDaskClusterOwner = """+username+"""
        
        # Prepare JDL
        jdl = """executable = start.sh
arguments = """+cluster_name+""" htcdask-worker_$(Cluster)_$(Process)
output = condor/htcdask-worker$(Cluster)_$(Process).out
error = condor/htcdask-worker$(Cluster)_$(Process).err
log = condor/htcdask-worker$(Cluster)_$(Process).log
request_cpus = 4
request_memory = 8GB
should_transfer_files = yes
transfer_input_files = """+credentials_dir+""", """+worker_space_dir+""" , """+condor_logdir+"""
Queue """+str(n)+""
    
        with open(f"{tmproot}/htcdask_submitfile.jdl", 'w+') as f:
            f.writelines(jdl)
        
        # Prepare singularity command
        singularity_cmd = """#!/bin/bash
export APPTAINERENV_DASK_GATEWAY_WORKER_NAME=$2
export APPTAINERENV_DASK_GATEWAY_API_URL="https://dask-gateway-api.fnal.gov/api"
export APPTAINERENV_DASK_GATEWAY_CLUSTER_NAME=$1
export APPTAINERENV_DASK_GATEWAY_API_TOKEN=/etc/dask-credentials/api-token
#export APPTAINERENV_DASK_DISTRIBUTED__LOGGING__DISTRIBUTED="debug"

worker_space_dir=${PWD}/dask-worker-space/$2
mkdir $worker_space_dir

cp """+x509_file+""" $worker_space_dir

/cvmfs/oasis.opensciencegrid.org/mis/apptainer/current/bin/apptainer exec -B ${worker_space_dir}:/srv/ -B dask-credentials:/etc/dask-credentials """+image_name+""" \
dask worker --name $2 --tls-ca-file /etc/dask-credentials/dask.crt --tls-cert /etc/dask-credentials/dask.crt --tls-key /etc/dask-credentials/dask.pem --worker-port 10000:10070 --no-nanny --local-directory /srv --scheduler-sni daskgateway-"""+cluster_name+""" --nthreads 1 tls://"""+self.scheduler_proxy_ip+""":80"""
    
        with open(f"{tmproot}/start.sh", 'w+') as f:
            f.writelines(singularity_cmd)
        os.chmod(f"{tmproot}/start.sh", 0o775)
        
        logger.info(" Sandbox : "+tmproot)
        logger.info(" Using image: "+image_name)
        logger.debug(" Submitting HTCondor job(s) for "+str(n)+" workers")

        # We add this to avoid a bug on Farruk's condor_submit wrapper (a fix is in progress)
        os.environ['LS_COLORS']="ExGxBxDxCxEgEdxbxgxcxd"

        # Submit our jdl, print the result and call the cluster widget
        cmd = "/usr/local/bin/condor_submit htcdask_submitfile.jdl | grep -oP '(?<=cluster )[^ ]*'"
        call = subprocess.check_output(['sh','-c',cmd], cwd=tmproot)
        
        worker_dict = {}
        clusterid = call.decode().rstrip()[:-1]
        worker_dict['ClusterId'] = clusterid
        worker_dict['Iwd'] = tmproot
        try:
            cmd = "/usr/local/bin/condor_q "+clusterid+" -af GlobalJobId | awk '{print $1}'| awk -F '#' '{print $1}' | uniq"
            call = subprocess.check_output(['sh','-c',cmd], cwd=tmproot)
        except CalledProcessError:
            logger.error("Error submitting HTCondor jobs, make sure you have a valid proxy and try again")
            return None
        scheddname = call.decode().rstrip()
        worker_dict['ScheddName'] = scheddname
        
        logger.info(" Success! submitted HTCondor jobs to "+scheddname+" with  ClusterId "+clusterid)
        return worker_dict
        
    def scale_kube_workers(self, n):
        username = pwd.getpwuid( os.getuid() )[ 0 ]
        logger.debug(" [WIP] Feature to be added ")
        logger.debug(" [NOOP] Scaled "+str(n)+"Kube workers, startup may take uo to 30 seconds")
        
    def destroy_batch_cluster_id(self, clusterid):
        logger.info(" Shutting down HTCondor worker jobs from cluster "+clusterid)
        cmd = "condor_rm "+self.batchWorkerJobs['ClusterId']+" -name "+self.batchWorkerJobs['ScheddName']
        result = subprocess.check_output(['sh','-c',cmd], cwd=self.batchWorkerJobs['Iwd'])
        logger.info(" "+result.decode().rstrip())

    def destroy_all_batch_clusters(self):
        logger.info(" Shutting down HTCondor worker jobs (if any)")
        if not self.batchWorkerJobs:
            return
        
        for htc_cluster in self.batchWorkerJobs:
            try:
                cmd = "condor_rm "+htc_cluster['ClusterId']+" -name "+htc_cluster['ScheddName']
                result = subprocess.check_output(['sh','-c',cmd], cwd=htc_cluster['Iwd'])
                logger.info(" "+result.decode().rstrip())
            except:
                logger.info(" "+result.decode().rstrip())

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
#        print("Hello, I am the interrupted adapt method")
#        print("I have two functions:")
#        print("1. Communicate to the Gateway server the new cluster state")
#        print("2. Call the adapt_cluster method on my HTCGateway")
        
        return self.gateway.adapt_cluster(
            self.name, minimum=minimum, maximum=maximum, active=active, **kwargs
        )
