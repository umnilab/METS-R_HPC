import os
import sys
import json
from os import path
from .Mab import MAB
from .MabBus import MABBus
from .MabDataProcessor import MabDataProcessor

class MABManager(object):
    def __init__(self, config, args):
        self.mab = {} # MAB model for E-Taxis, every hour we train a new model
        self.mabBus = {} # MAB model for E-Buses
        self.initialLinkSpeedLength = []
        self.roadLengthMap = {}
        
        self.path_info = {}
        self.path_info_bus = {}
        self.valid_path = {}
        self.valid_path_bus = {}

        self.config = config
        self.args=args

        self.total_hour = int(args.SIMULATION_STOP_TIME * args.SIMULATION_STEP_SIZE/3600)

        self.received_route_ucb = False
        self.received_route_ucb_bus = False

        self.mab_data_processor = MabDataProcessor(config, self)

    def initialize(self, candidate_paths, size, type):
        if type == 'taxi':
            for od in candidate_paths.keys():
                self.path_info[od] = candidate_paths[od]
                self.valid_path[od] = list(range(len(candidate_paths[od])))
            
            if len(self.path_info) == size:
                self.initialLinkSpeedLength = []
                for hour in range(self.total_hour+1):
                    self.mab[hour] = MAB(self.path_info, self.valid_path)
                    self.initialLinkSpeedLength.append({})
                self.warm_up(self.config.road_file)
                self.received_route_ucb = True

        elif type == 'bus':
            for bod in candidate_paths.keys():
                self.path_info_bus[bod] = candidate_paths[bod]
                self.valid_path_bus[od] = list(range(len(candidate_paths[od])))
            
            if len(self.path_info_bus) == size:
                self.initialLinkSpeedLength = []
                for hour in range(self.total_hour+1):
                    self.mabBus[hour] = MABBus(self.path_info_bus, self.valid_path_bus)
                    self.initialLinkSpeedLength.append({})
                self.warm_up_bus(self.config.road_file)
                self.received_route_ucb_bus = True

    def consume(self, dataMap, type):
        hour = (dataMap.utc_time * self.args.SIMULATION_STEP_SIZE)//3600
        if type == 'taxi':
            self.mab[hour].updateLinkUCB(dataMap.roadID, dataMap.linkEnergy)
        elif type == 'bus':
            self.mabBus[hour].updateLinkUCB(dataMap.roadID, dataMap.linkEnergy)
        elif type == 'speed':
            self.mabBus[hour].updateShadowBus(dataMap.roadID, dataMap.link_travel_time, self.initialLinkSpeedLength[hour][dataMap.roadID][1])

    def predict(self, hour, type):
        res = {}
        if type == 'taxi':
            od_list = []
            action_list = []
            for od in self.path_info.keys():
                od_list.append(od)
                action_list.append(self.mab[hour].play(od))
            res['TYPE'] = 'CTRL_routingTaxi'
            res['OD'] = od_list
            res['result'] = action_list
        elif type == 'bus':
            bod_list = []
            action_list = []
            for od in self.path_info_bus.keys():
                bod_list.append(od)
                action_list.append(self.mabBus[hour].play(od))
            res['TYPE'] = 'CTRL_routingBus'
            res['BOD'] = bod_list
            res['result'] = action_list
        return res

    def refreshLinkUCBShadow(self, new_speedUCBMap):
        hour = 0
        for IDhour in new_speedUCBMap.keys():
            hour = int(IDhour.split(";")[1])
        
        lengthUCB=self.roadLengthMap
        self.mabBus[hour].updateShadowBus(new_speedUCBMap, lengthUCB)
  
        return hour

    # Initialize ucb data for each link
    def warm_up(self, fileName):   
        with open(fileName, 'r') as f:
            f.readline()
            for line in f.readlines():
                result = line.split(",")
                roadID = int(result[0])
                roadLength = float(result[-1])
                roadType = int(float(result[2]))
                for i in range(self.total_hour+1):
                    backgroundSpeed =  35 if roadType==2 else 25
                    speedLength = [backgroundSpeed, roadLength]
                    self.initialLinkSpeedLength[i][roadID] = speedLength
                    self.roadLengthMap[roadID] = roadLength

        for i in range(self.total_hour+1):
            self.mab[i].warm_up(self.initialLinkSpeedLength[i])
    
    def warm_up_bus(self, fileName):   
        with open(fileName, 'r') as f:
            f.readline()
            for line in f.readlines():
                result = line.split(",")
                roadID = int(result[0])
                roadLength = float(result[-1])
                roadType = int(float(result[2]))
                for i in range(self.total_hour+1):
                    backgroundSpeed =  35 if roadType==2 else 25
                    speedLength = [backgroundSpeed, roadLength]
                    self.initialLinkSpeedLength[i][roadID] = speedLength
                    self.roadLengthMap[roadID] = roadLength

        for i in range(self.total_hour+1):
            self.mabBus[i].warm_up(self.initialLinkSpeedLength[i])

