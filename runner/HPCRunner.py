import os
import sys
import signal
import json
import ast
import time
import pandas as pd
import psutil
import traceback

from models.eco_routing.MabManager import MABManager
from models.bus_scheduling.BusPlanningManager import BusPlanningManager
from types import SimpleNamespace as SN
from collections import defaultdict
from clients.METSRClient import METSRClient

"""
Implementation of the HPC Runner

A  HPC runner communicates with multiple METSRClient to manage the 
data flow between corresponding simulation instances.

Currently we assume that all simulation instances are ran with the same 
configurations, which can be extended to cover instances with different 
settings.
"""

class HPCRunner:
    def __init__(self, config, docker_ids):
        # Obtain simulation arguments from the configuration file
        args = {}

        with open(config.data_dir + '/Data.properties', "r") as f:
            for line in f:
                if "#" in line:
                    continue
                fields = line.replace(" ","").strip().split("=")
                if len(fields) != 2:
                    continue
                else:
                    try:
                        args[fields[0]] = ast.literal_eval(fields[1])
                    except:
                        args[fields[0]] = fields[1]
        
        self.sim_args = SN(**args)

        self.total_hour = int(self.sim_args.SIMULATION_STOP_TIME/60)

        self.config = config

        # Create and maintain a list for RDCs
        self.rd_clients = []
    
        for i in range(config.num_simulations):
            ws_client = METSRClient("localhost", int(config.ports[i]), i, docker_ids[i], self, verbose = config.verbose)
            ws_client.start()
            self.rd_clients.append(ws_client)
        print("Created all clients!")

    def run(self, container_ids):
        ''' 
        ---------- OPERATIONAL ALGS START HERE ------------------------------
        Remarks : 
        1) you need to acquire a lock if you read or write to rd_client's 
        data maps, somthing like,
        with rd_client[i].lock:
            do somthing here with rd_client data maps
        2) all messages are encoded in json format, so route_result must
        also be in JSON format, also change the route_result reception side
        in the simulator to facilitate this.
        ''' 
        print("Initializing operational models!")
        self.mab_manager= MABManager(self.config, self.sim_args)

        if (self.config.eco_routing):
            while not self.mab_manager.received_route_ucb:
                time.sleep(1)
            print("Candidate routes received")

        if (self.config.eco_routing_bus):
            while not self.mab_manager.received_route_ucb_bus:
                time.sleep(1)
            print("Bus candidate routes received")

        # Initialize bus scheduling data
        if (self.config.bus_scheduling):
            self.bus_planning_manager = BusPlanningManager(self.config, self.sim_args)

        # wait for the sim to start
        time.sleep(5)

        # Recurrent data flow
        continue_flag = True
        while continue_flag:
            try:
                for rd_client in self.rd_clients:
                    if rd_client.state == "connected":
                        tmp_tick = rd_client.current_tick
                        if tmp_tick >= 0:
                            tmp_hour = int((tmp_tick * self.sim_args.SIMULATION_STEP_SIZE) // 3600)

                            if(tmp_tick > rd_client.prev_tick):
                                if(self.config.eco_routing):
                                    # Process the link energy data
                                    self.mab_manager.mab_data_processor.process()
                                    # Sending back the eco-routing results every 5 minutes
                                    if (tmp_tick * self.sim_args.SIMULATION_STEP_SIZE) % 300 == 0:
                                        res = self.mab_manager.predict(tmp_hour, type = "taxi")
                                        rd_client.ws.send(json.dumps(res))
                                                
                                if(self.config.eco_routing_bus):
                                    # Process the link energy and link travel time data
                                    self.mab_manager.mab_data_processor.process_bus()
                                    # Sending back the bus scheduling results every 5 minutes
                                    if (tmp_tick * self.sim_args.SIMULATION_STEP_SIZE) % 300 == 0:
                                        res = self.mab_manager.predict(tmp_hour, type = "bus")
                                        rd_client.ws.send(json.dumps(res))
                            
                                if(self.config.bus_scheduling):
                                    # Sending back the bus scheduling results every 2 hours
                                    if tmp_tick % self.sim_args.SIMULATION_BUS_REFRESH_INTERVAL == 0:
                                        res = self.bus_planning_manager.predict(tmp_hour)
                                        rd_client.ws.send(json.dumps(res))

                                # More operational models can be added here
                                rd_client.send_step_message(tmp_tick)

                            # Send back the step function if wait for more than 10 seconds
                            elif (time.time() - rd_client.prev_time > 10):
                                rd_client.send_step_message(tmp_tick)

                            else:
                                time.sleep(0.001)

            except Exception as e:
                traceback.print_exc()
                continue_flag = False

        try:  
            # Wait until all rd_clients finish their work
            for rd_client in self.rd_clients:
                rd_client.terminate()
                
        finally:
            os.chdir("docker")
            os.system("docker-compose down")
            for container_id in container_ids:
                os.system("docker stop " + container_id)
            


