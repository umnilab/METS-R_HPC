import sys
import argparse
import os

import subprocess

"""
This is the entrance for METSR-HPC module
usage example: python run_hpc.py -s 3 -c 2 -tf 2000 -bf 20 -co
"""

def get_arguments(argv):
    parser = argparse.ArgumentParser(description='METS-R simulation')
    parser.add_argument('-s','--scenario_index', type=int, 
                        help='the index of to simulate scenario, values from 0 to 3')
    parser.add_argument('-e', '--eco_routing', action='store_true', default=False,
                        help='enable ecorouting')
    parser.add_argument('-b', '--bus_scheduling', action='store_true', default=False,
                        help='enable bus scheduling')
    parser.add_argument('-tf', '--taxi_fleet', type=int, default=20000,
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
    args = get_arguments(sys.argv[1:])
    for casename in [1]:
        for demand_factor in [0.01,0.05,0.1,0.25,0.5]:
            for thread in [1,2,4,8,16]:
                commands = []
                if (args.scenario_index==3):
                    case = casename + 10
                else:
                    case = casename
                commands=['python', 'run_hpc.py','-r', 'run.config.scenario_full.json','-s',str(args.scenario_index),'-c',\
                str(case),'-tf',str(args.taxi_fleet),'-bf',str(args.bus_fleet), '-df', str(demand_factor), '-th', str(thread)]
                if(args.eco_routing):
                    commands += ['-e']
                if(args.demand_sharable):
                    commands += ['-ds']
                if (args.bus_fleet>0):
                    if(args.cooperative):
                        commands += ['-co']
                    if(args.bus_scheduling):
                        commands += ['-b']
                sim_dir = "output/scenario_" + str(args.scenario_index) +"_case_"+ str(case) + "_seed_42_"
                sim_dir += "eco"+"_"+('true' if args.eco_routing else 'false')+ "_"
                sim_dir += "bus"+"_"+('true' if args.bus_scheduling  else 'false')+ "_"
                sim_dir += "share"+"_"+('true' if args.demand_sharable else 'false') + "_"
                sim_dir += "demand"+"_"+str(100) + "_"
                sim_dir += "taxi_" + str(args.taxi_fleet) + "_bus_" + str(args.bus_fleet)
                sim_dir += "_co" if args.cooperative else ""
                sim_dir += "_pass" 
                sim_dir += "_" + str(int(demand_factor*100))
                sim_dir += "_" + str(thread)
                print(sim_dir)
                if(not os.path.exists(sim_dir)):
                    subprocess.call(commands)
                else:
                    print("Result already exists! Skip")

if __name__ ==  "__main__":
    main()
