import sys
import os
import argparse
import time
from runner.HPCRunner import HPCRunner
from utils.util import read_run_config, prepare_sim_dirs, run_simulations, run_simulations_in_background

"""
This is the entrance for METSR-HPC module
usage example: python hpc_example.py -s 0 -c 0 -tf 4000 -bf 40 -e -v
"""

# This script is used to run the METS-R simulation for the NYC case:
# The scenario is associate with to the different demand patterns.
# The case is associate with the different days.

# The default demand is from 2019-04-25
# The full data is available at https://drive.google.com/drive/folders/1r6bsCjGYrg4ckLQeud4yMWfQ8qy85zJS?usp=sharing
# To use it, copy it to METS_R/data in METS-R_SIM (https://github.com/umnilab/METS-R_SIM)
# Scenario 0 - Sunday pattern, Scenario 1 - Saturday pattern, Scenario 2 - Weekday pattern, Scenario 3 - Anomalies


def get_arguments(argv):
    parser = argparse.ArgumentParser(description='METS-R simulation')
    parser.add_argument('-r','--run_config', default='configs/run_hpc_NYC_win.json',
                        help='the folder that contains all the input data')
    parser.add_argument('-s','--scenario_index', type=int, 
                        help='the index of to simulate scenario')
    parser.add_argument('-c','--case_index', type=int, 
                        help='the index within the scenario')
    parser.add_argument('-e', '--eco_routing', action='store_true', default=False,
                        help='enable ecorouting')
    parser.add_argument('-eb', '--eco_routing_bus', action='store_true', default=False,
                        help='enable eco routing for bus')
    parser.add_argument('-b', '--bus_scheduling', action='store_true', default=False,
                        help='enable bus scheduling')
    parser.add_argument('-tf', '--taxi_fleet', type=int, default=2000,
                        help='number of AEV taxis initialized')
    parser.add_argument('-bf', '--bus_fleet', type=int, default=0,
                        help='number of AEV buses initialized')
    parser.add_argument('-co', '--cooperative', action='store_true', default=False,
                        help='enable taxi-bus or bus-taxi cooperation')
    parser.add_argument('-ds', '--demand_sharable', action='store_true', default=False,
                        help='whether the request is sharable')
    parser.add_argument('-df', '--demand_factor', type=float, default  = 1.0, 
                        help='ratio of demand')
    parser.add_argument('-v', '--verbose', action='store_true', default=False,
                        help='verbose mode')
    args = parser.parse_args(argv)

    config = read_run_config(args.run_config)
    config.scenario_index = args.scenario_index
    config.case_index = args.case_index
    config.eco_routing =  args.eco_routing
    config.eco_routing_bus = args.eco_routing_bus
    config.bus_scheduling = args.bus_scheduling
    config.cooperative = args.cooperative
    config.demand_sharable = args.demand_sharable
    config.bus_fleet_size = args.bus_fleet
    config.taxi_fleet_size = args.taxi_fleet
    config.verbose = args.verbose
    config.demand_factor = args.demand_factor

    return config

if __name__ ==  "__main__":
    # Load configs
    config = get_arguments(sys.argv[1:])
    
    print("---------------- HPC options ----------------")
    print(config)
    print("---------------------------------------------")

    # Start the Kafka brokers and databases
    # Note that you need to run docker build -t postgres postgres first
    # cd docker
    # docker-compose up -d
    os.chdir("docker")
    os.system("docker-compose up -d")
    os.chdir("..")

    time.sleep(10) # wait 10s for the Kafka servers to be up

    # Prepare simulation directories
    prepare_sim_dirs(config)

    # Launch the simulations
    # run_simulations(config) # for debugging
    run_simulations_in_background(config)
    
    # Run RDCM (remote data client manager) 
    rdcm = HPCRunner(config)
    rdcm.run()
