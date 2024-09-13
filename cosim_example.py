import sys
import os
import argparse
import time
from runner.CoSimRunner import CoSimRunner
from utils.util import read_run_config, prepare_sim_dirs, run_simulations, run_simulations_in_background, run_simulation_in_docker 
from utils.carla_util import open_carla

# use case: python cosim_example.py -r configs/run_cosim_CARLAT5_docker.json -v
def get_arguments(argv):
    parser = argparse.ArgumentParser(description='METS-R simulation')
    parser.add_argument('-r','--run_config', default='configs/run_cosim_CARLAT5_docker.json',
                        help='the folder that contains all the input data')
    parser.add_argument('-v', '--verbose', action='store_true', default=False,
                        help='verbose mode')
    args = parser.parse_args(argv)

    config = read_run_config(args.run_config)
    config.verbose = args.verbose

    return config

if __name__ == '__main__':
    config = get_arguments(sys.argv[1:])
    os.chdir("docker")
    os.system("docker-compose up -d")
    os.chdir("..")

    time.sleep(10) # wait 10s for the Kafka servers to be up

    # Prepare simulation directories
    dest_data_dirs = prepare_sim_dirs(config)

    # Launch the simulations
    # run_simulations(config)
    # run_simulations_in_background(config)
    container_ids = run_simulation_in_docker(config)

    # run_co_simulation
    carla_client, carla_tm = open_carla(config)

    runner = CoSimRunner(config, container_ids, carla_client, carla_tm)
    runner.run(container_ids)


