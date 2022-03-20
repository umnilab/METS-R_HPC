import sys
import argparse
from rdcm import run_rdcm
from util import read_run_config, prepare_sim_dirs, run_simulations

# example: python run_hpc.py -s 1 -c 0 -p 1 -e -b
def get_arguments(argv):
    parser = argparse.ArgumentParser(description='METSR simulation')
    parser.add_argument('-r','--run_config', default='run.config.scenario.json',
                        help='the folder that contains all the input data')
    parser.add_argument('-s','--scenario_index', type=int, 
                        help='the index of to simulate scenario, values from 0 to 3')
    parser.add_argument('-c','--case_index', type=int, 
                        help='the index within the scenario, take values from 0 to 9')
    parser.add_argument('-p', '--share_percentage', type=float, default=0.5,
                        help='percentage of sharable requests, 0 to 1')
    parser.add_argument('-e', '--eco_routing', action='store_true', default=False,
                        help='enable ecorouting')
    parser.add_argument('-b', '--bus_scheduling', action='store_true', default=False,
                        help='enable bus scheduling')

    args = parser.parse_args(argv)

    return args
    
# main function for running the rdcm and simulations
def main():
    args = get_arguments(sys.argv[1:])

    options = read_run_config(args.run_config)
    
    options.case_index = args.case_index
    options.scenario_index = args.scenario_index
    options.eco_routing = "true" if args.eco_routing else "false"
    options.bus_scheduling = "true" if args.bus_scheduling else "false"
    options.share_percentage = args.share_percentage
    
    print("---------------- HPC options ----------------")
    print(options)
    print("---------------------------------------------")

    # prepare simulation directories
    prepare_sim_dirs(options)
    # launch the simulations
    run_simulations(options)
    # run rdcm 
    # add scneario index for quick
    run_rdcm(options, options.num_simulations, options.ports)

if __name__ ==  "__main__":
    main()
