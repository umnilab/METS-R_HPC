# How to run?
Update the `run.config.json` file with your configurations and run HPC framework using,
```
python run_hpc.py -s 0 -c 0 -tf 2000 -bf 20
```  
# How it works?
`run.config.json' file contains all the settings needed to run the simulation instances and remote data client (RDClient) instances.

* `java_path` : location of the java installation in your machine
* `java_options` : jvm options
* `evacsim_dir` : location of the METS-R simulator code
* `repast_plugin_dir` : repast eclipse plugin location
* `num_sim_instances` : number of parallel simulation instances to run
* `socket_port_numbers` : socket port numbers each simulation instance will be listening on

`run_hpc.py` script uses this information to launch multiple simulation instances as independent processes. It also runs the Remote Data Clients (RDClient) in separate python threads to listens to the simulation instances and record the data received. RDClients are launched asynchronously i.e. simulation socket server does not have to be up before launching the corresponding RDClient. RDClient will wait until the simulation socket server is up before establishment a connection. After establishing the connection RDClient will continue to receive messages until the simulation is finished and socket connection is terminated.

**IMPORTANT** :  All messages are sent and received in JSON format. 
# Implementing the ML algorithms
ML algorithms for route selection must go in `rdcm.py`. For example MAB algorithm can be called inside 'run_rdcm' function to compute the optimal route candidate. Computed route result can be sent to simulation instance in JSON format using something,
```
ws_client.ws.send(route_result_json)
``` 







