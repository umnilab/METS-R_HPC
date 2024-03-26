#BusPlanningManager 

# We use the cached results as the optimization is much slower than the simulator

#Author: Jiawei Xue, Zengxiang Lei
#Time: October 2021

import json

class BusPlanningManager(object):
    def __init__(self, config, args):        
        self.prev_hour = -1 # the previous hour that the bus schedule was sent
        self.total_hour = int(args.SIMULATION_STOP_TIME * args.SIMULATION_STEP_SIZE/3600)

        # directly use the cached results as the optimization is much slower than the simulator     
        date_sim=args.BT_EVENT_FILE.split("scenario")[1].split("speed_")[1].split(".csv")[0]
        scenario_index=args.BT_EVENT_FILE.split("scenario")[1].split("/speed")[0]                   
        bus_scheduling_read = "bus_scheduling/offline_cache_cleaned/scenario_"+scenario_index+"_speed_"+date_sim + "_" + str(args.NUM_OF_BUS)+"_bus_scheduling.json"
        print("Using cached bus schedule from: " + bus_scheduling_read)
        bus_scheduling_read_raw = open(bus_scheduling_read)
        self.busPlanningResults = json.load(bus_scheduling_read_raw)

    def predict(self, hour):
        for f in ['JFK','LGA','PENN']:
            bus_planning_prepared = True
            if f not in self.busPlanningResults[str(hour)]:
                bus_planning_prepared = False
        if bus_planning_prepared:
            print("Sending bus scheduling results for hour {}".format(hour))
            busPlanningResults_combine={}
            # comment the following three lines if the schedules are generated in real time
            JFK_json=json.loads(self.busPlanningResults[str(hour)]['JFK'])
            LGA_json=json.loads(self.busPlanningResults[str(hour)]['LGA'])
            PENN_json=json.loads(self.busPlanningResults[str(hour)]['PENN'])

            busPlanningResults_combine['TYPE']="CTRL_scheduleBus"
            busPlanningResults_combine['Bus_route']=list(JFK_json['Bus_route'])+list(LGA_json['Bus_route'])+list(PENN_json['Bus_route'])
            busPlanningResults_combine['Bus_num']=list(JFK_json['Bus_num'])+list(LGA_json['Bus_num'])+list(PENN_json['Bus_num'])
            busPlanningResults_combine['Bus_gap']=list(JFK_json['Bus_gap'])+list(LGA_json['Bus_gap'])+list(PENN_json['Bus_gap'])
            busPlanningResults_combine['Bus_routename']=list(JFK_json['Bus_routename'])+list(LGA_json['Bus_routename'])+list(PENN_json['Bus_routename'])
            busPlanningResults_combine['Bus_currenthour']=JFK_json['Bus_currenthour']
            self.prev_hour = hour
            return busPlanningResults_combine




