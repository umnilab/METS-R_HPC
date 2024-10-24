import sys
import argparse
import os

import subprocess

"""
This is the script for running the experiments in batch
"""

def get_arguments(argv):
    parser = argparse.ArgumentParser(description='METS-R simulation')
    parser.add_argument('-s','--scenario_index', type=int, 
                        help='the index of to simulate scenario, values from 0 to 3')
    parser.add_argument('-tf', '--taxi_fleet', type=int, default=3000,
                        help='number of AEV taxis initialized')
    parser.add_argument('-bf', '--bus_fleet', type=int, default=40,
                        help='number of AEV buses initialized')
    parser.add_argument('-ds', '--demand_sharable', action='store_true', default=False,
                        help='whether the request is sharable')
    args = parser.parse_args(argv)

    return args
    
# Main function for running the rdcm and simulations
def main():
    args = get_arguments(sys.argv[1:])
    for casename in range(1,10,2):
        for eco_routing in [False, True]:
            for bus_scheduling in [False, True]:
                for cooperative in [False, True]:
                    commands = []
                    if (args.scenario_index==3):
                        case = casename + 10
                    else:
                        case = casename
                    commands=['python', 'main.py','-s',str(args.scenario_index),'-c',\
                    str(case),'-tf',str(args.taxi_fleet),'-bf',str(args.bus_fleet)]
                    if(eco_routing):
                        commands += ['-e']
                    if(args.demand_sharable):
                        commands += ['-ds']
                    if (args.bus_fleet>0):
                        if(cooperative):
                            commands += ['-co']
                        if(bus_scheduling):
                            commands += ['-b']
                    sim_dir = "output/scenario_" + str(args.scenario_index) +"_case_"+ str(case) + "_seed_42_"
                    sim_dir += "eco"+"_"+('true' if eco_routing else 'false')+ "_"
                    sim_dir += "bus"+"_"+('true' if bus_scheduling  else 'false')+ "_"
                    sim_dir += "share"+"_"+('true' if args.demand_sharable else 'false') + "_"
                    sim_dir += "demand"+"_"+str(100) + "_"
                    sim_dir += "taxi_" + str(args.taxi_fleet) + "_bus_" + str(args.bus_fleet)
                    sim_dir += "_co" if cooperative else ""
                    sim_dir += "_pass" 
                    print(sim_dir)
                    if(not os.path.exists(sim_dir)):
                        subprocess.call(commands)
                    else:
                        print("Result already exists! Skip")

if __name__ ==  "__main__":
    main()
