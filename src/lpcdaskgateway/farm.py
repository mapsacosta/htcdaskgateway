class LPCGatewayWorkerFarm():
    
    def __init__(self, cluster, schedd_name, condor_cluster_id, iwd, machine, n_workers):
        self.cluster = sched_name
        self.schedd_name = schedd_name
        self.condor_cluster_id = condor_cluster_id
        self.iwd = iwd
        self.machine = machine
        self.n_workers = n_workers
        self.workers = self.create_batch_workers(cluster, n_workers)
        logger.info(" New LPCGatewayWorkerFarm : "+str(n)+" HTCondor workers")
        
    def create_batch_workers(self, cluster, n):
        username = pwd.getpwuid( os.getuid() )[ 0 ]
        security = cluster.security
        cluster_name = cluster.name
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
        
        logger.info(" Sandbox folder located at: "+tmproot)

        logger.info(" Submitting HTCondor job(s) for "+str(n)+" workers")

        # We add this to avoid a bug on Farruk's condor_submit wrapper (a fix is in progress)
        os.environ['LS_COLORS']="ExGxBxDxCxEgEdxbxgxcxd"

        # Submit our jdl, print the result and call the cluster widget
        result = subprocess.check_output(['sh','-c','/usr/local/bin/condor_submit htcdask_submitfile.jdl'], cwd=tmproot)
        logger.info(" Success! submitted HTCondor jobs")
        
        cmd = "/usr/local/bin/condor_q -all -const 'regexp( \""+cluster_name+"\", Args )' -af GlobalJobId | awk '{print $1}'| awk -F '#' '{print $1}' | uniq"
        scheddname = subprocess.check_output(['sh','-c',cmd], cwd=tmproot)
        
        cmd = "condor_q -all -const 'regexp( \""+cluster_name+"\", Args )' -af ClusterId | sort | uniq"
        clusterid = subprocess.check_output(['sh','-c',cmd], cwd=tmproot)
        
        worker_dict = {}
        worker_dict['ClusterId'] = clusterid.decode().rstrip()
        worker_dict['ScheddName'] = scheddname.decode().rstrip()
        worker_dict['Iwd'] = tmproot
        
        return worker_dict