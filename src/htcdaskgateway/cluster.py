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

def jdl_conversion(w_mem,w_cores):
    
    try:
        if type(w_mem) == str:
            print("ERROR: ")
            raise TypeError(f"Invalid type for worker memory. worker_memory={w_mem} not a float or convertable to a float. Please run cluster.shutdown().")

        if type(w_cores) == str:
            print("ERROR: ")
            raise TypeError(f"Invalid type for worker cores. worker_cores={w_cores} not an int. Please run cluster.shutdown().")
        
        work_mem = str(w_mem)
        test_mem = float(work_mem)

        work_mem = work_mem + "GB"
        
        n_cores = str(w_cores)
        test_cores = int(n_cores)

        if w_mem < 1 or w_mem > 8:
            raise ValueError(f"Invalid worker memory. worker_memory={w_mem} not in the range 1-8. Please run cluster.shutdown().")

        if w_cores < 1 or w_cores > 4:
            raise ValueError(f"Invalid number of worker cores. worker_cores={w_cores} not in the range 1-4. Please run cluster.shutdown().")

        return work_mem, n_cores

    except ValueError as e:
        print("ERROR: ")
        if "invalid literal" in str(e):
            if str(e).startswith("invalid literal for int"):
                print(f"Invalid type for number of worker cores. worker_cores={w_cores} not an int. Please run cluster.shutdown().")
            else:
                print(f"Invalid type for worker memory. worker_memory={w_mem} not a float or convertable to a float. Please run cluster.shutdown().")
        else:
            print(e)

        return None, None
    

class HTCGatewayCluster(GatewayCluster):

    def __init__(self, image_registry="registry.hub.docker.com", apptainer_image='coffeateam/coffea-base-almalinux8:0.7.22-py3.10',
                 worker_memory=4, worker_cores=2, **kwargs):
        self.scheduler_proxy_ip = kwargs.pop('', '131.225.218.222')
        self.batchWorkerJobs = []
        self.image_registry = image_registry
        self.cluster_options = kwargs.get('cluster_options')
        self.apptainer_image = apptainer_image
        self.worker_memory = worker_memory
        self.worker_cores = worker_cores

        if self.cluster_options:
            if 'worker_memory' in self.cluster_options:
                self.worker_memory = self.cluster_options.worker_memory
                
            if 'worker_cores' in self.cluster_options:
                self.worker_cores = self.cluster_options.worker_cores
                
            if 'image' in self.cluster_options:
                self.apptainer_image = self.cluster_options.image

        kwargs['image'] = self.image_registry + "/" + self.apptainer_image
        kwargs['worker_memory'] = self.worker_memory
        kwargs['worker_cores'] = self.worker_cores
        
        print("Apptainer_image: ", self.apptainer_image)
        print("Worker_memory: ", kwargs['worker_memory'], "GB")
        print("Worker_cores: ", kwargs['worker_cores'])
        print("Image_registry: ", self.image_registry)

        dir_command = "[ -d \"/cvmfs/unpacked.cern.ch/" + kwargs['image'] + "\" ]" 
        if os.system(dir_command):
            sys.exit("Image not allowed. Images must be from /cvmfs/unpacked.cern.ch. Check for typos or check cvmfs using ls /cvmfs/unpacked.cern.ch/")
        
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
                
        worker_mem, num_cores = jdl_conversion(self.worker_memory,self.worker_cores)

        if worker_mem == None or num_cores == None:
            sys.exit("See memory or core errors. Please run cluster.shutdown().")

        image_name = "/cvmfs/unpacked.cern.ch/" + self.image_registry + "/" + self.apptainer_image
        

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
request_cpus = """+num_cores+"""
request_memory = """+worker_mem+"""
+isDaskJob = True
requirements = (isDaskNode == True)
should_transfer_files = yes
transfer_input_files = """+credentials_dir+""", """+worker_space_dir+""" , """+condor_logdir+"""
when_to_transfer_output = ON_EXIT_OR_EVICT
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
