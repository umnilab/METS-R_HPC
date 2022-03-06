import sys
from rdcm import run_rdcm
from util import read_run_config, prepare_sim_dirs, run_simulations


# main function for running the rdcm and simulations
def main():
    if len(sys.argv) < 3:
        print("Specify the config file name and case index!")
        print("python3 run_hpc.py <config_file> <case_index [0-9]>")
        sys.exit(-1)

    options = read_run_config(sys.argv[1])
    options.case_index = int(sys.argv[2])
    print("---------------- HPC options ----------------")
    print(options)
    print("---------------------------------------------")

    # prepare simulation directories
    prepare_sim_dirs(options)
    # launch the simulations
    run_simulations(options)
    # run rdcm 
    # add scneario index for quick
    run_rdcm(options.num_simulations, options.ports)

if __name__ ==  "__main__":
    main()
