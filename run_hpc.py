import sys
import argparse

from rdcm import run_rdcm
from util import read_run_config, prepare_sim_dirs, run_simulations

"""
This is the entrance for METSR-HPC module
usage example: python run_hpc.py -s 3 -c 2 -tf 2000 -bf 20 -co
"""

def get_arguments(argv):
    parser = argparse.ArgumentParser(description='METS-R simulation')
    parser.add_argument('-r','--run_config', default='run.config.scenario.json',
                        help='the folder that contains all the input data')
    parser.add_argument('-s','--scenario_index', type=int, 
                        help='the index of to simulate scenario, values from 0 to 3')
    parser.add_argument('-c','--case_index', type=int, 
                        help='the index within the scenario, take values from 0 to 9')
    parser.add_argument('-e', '--eco_routing', action='store_true', default=False,
                        help='enable ecorouting')
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
    args = parser.parse_args(argv)

    return args
    
# Main function for running the rdcm and simulations
def main():
    # Load configs
    args = get_arguments(sys.argv[1:])
    options = read_run_config(args.run_config)
    options.case_index = args.case_index
    options.scenario_index = args.scenario_index
    options.eco_routing = "true" if args.eco_routing else "false"
    options.bus_scheduling = "true" if args.bus_scheduling else "false"
    options.cooperative = "true" if args.cooperative else "false"
    options.demand_sharable = "true" if args.demand_sharable else "false"
    options.bus_fleet_size = args.bus_fleet
    options.taxi_fleet_size = args.taxi_fleet

    
    print("---------------- HPC options ----------------")
    print(options)
    print("---------------------------------------------")

    # Prepare simulation directories
    options.data_dir = prepare_sim_dirs(options)
    print(options.data_dir)

    # Launch the simulations
    run_simulations(options)
    
    # Run RDCM (remote data colient manager) 
    run_rdcm(options, options.num_simulations, options.ports)

if __name__ ==  "__main__":
    main()
