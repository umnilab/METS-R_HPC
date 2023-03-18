import os
import sys
import json
from os import path
from eco_routing.Mab import MAB
# from eco_routing.MabBus import MABBus

class MABManager(object):
    def __init__(self, working_dir, args):
        self.mab = {} # MAB model for E-Taxis, every hour we train a new model
        # self.mabBus = {} # MAB model for E-Buses
        self.initialLinkSpeedLength = []
        self.roadLengthMap = {}
        self.path_info = {}
        self.valid_path = {}
        self.path_info_bus = {}
        self.valid_path_bus = {}

        self.working_dir = working_dir
        self.args=args
        for hour in range(int(args.SIMULATION_STOP_TIME * args.SIMULATION_STEP_SIZE//3600)+1):
            self.mab[hour] = MAB(self.path_info, self.valid_path)
            # self.mabBus[hour] = MABBus(self.path_info_bus, self.valid_path_bus)
            self.initialLinkSpeedLength.append({})

    def ucbRouting(self, od, hour):
        self.mab[hour].play(od)
        return self.mab[hour].getAction()

    def refreshLinkUCB(self, new_linkUCBMap):
        hour = 0
        for IDhour in new_linkUCBMap.keys():
            hour = int(IDhour.split(";")[1])
        #    break
        #key_hour='hour_int'
        #if key_hour in new_linkUCBMap.keys():
        #   hour=new_linkUCBMap[key_hour]
        self.mab[hour].updateLinkUCB(new_linkUCBMap)
        return hour

    def refreshRouteUCB(self, new_routeUCBMap):
        for hour in range(int(self.args.SIMULATION_STOP_TIME * self.args.SIMULATION_STEP_SIZE//3600)+1):
            self.mab[hour].updateRouteUCB(new_routeUCBMap)

    # def ucbRoutingBus(self, od, hour):
    #     self.mabBus[hour].play(od)
    #     return self.mabBus[hour].getAction()

    # def refreshLinkUCBBus(self, new_linkUCBMapBus):
    #     hour = 0
    #     # TODO: Change to json
    #     for IDhour in new_linkUCBMapBus.keys(): 
    #         #print('hour is')
    #         #print(IDhour)
    #         hour = int(IDhour.split(";")[1])
    #     #    break
 
    #     #key_hour='hour_int'
    #     #if key_hour in new_linkUCBMapBus.keys():
    #     #    hour=new_linkUCBMapBus[key_hour]
    #     self.mabBus[hour].updateLinkUCB(new_linkUCBMapBus)
    #     return hour

    # def refreshRouteUCBBus(self, new_routeUCBMapBus):
    #     for hour in range(int(self.args.SIMULATION_STOP_TIME * self.args.SIMULATION_STEP_SIZE//3600)+1):
    #         self.mabBus[hour].updateRouteUCB(new_routeUCBMapBus)

    # def refreshLinkUCBShadow(self, new_speedUCBMap):
    #     hour = 0
    #     for IDhour in new_speedUCBMap.keys():
    #         hour = int(IDhour.split(";")[1])
    #     #key_hour='hour_int'
    #     #if key_hour in new_speedUCBMap.keys():
    #     #    hour=new_speedUCBMap[key_hour]
    #     lengthUCB=self.roadLengthMap
    #     self.mabBus[hour].updateShadowBus(new_speedUCBMap, lengthUCB)
    #     #self.mabBus[hour].updateShadowBus(new_speedUCBMap)
    #     return hour


    # Initialize speed data for each link
    def initializeLinkEnergy1(self, fileName1):
        with open(fileName1, 'r') as f:
            f.readline() # skip the first line
            for line in f.readlines():
                result = line.split(",")
                roadID = int(result[0])
                roadType = int(float(result[2]))
                for i in range(int(self.args.SIMULATION_STOP_TIME * self.args.SIMULATION_STEP_SIZE//3600)+1):
                    backgroundSpeed = 35 if roadType==2 else 25
                    speedLength  = [backgroundSpeed]
                    self.initialLinkSpeedLength[i][roadID] = speedLength

    # Initialize link length data for each link
    def initializeLinkEnergy2(self, fileName2):   
        with open(fileName2, 'r') as f:
            f.readline()
            for line in f.readlines():
                result = line.split(",")
                roadID = int(result[0])
                roadLength = float(result[-1])
                roadType = int(float(result[2]))
                for i in range(int(self.args.SIMULATION_STOP_TIME * self.args.SIMULATION_STEP_SIZE//3600)+1):
                    backgroundSpeed =  35 if roadType==2 else 25
                    speedLength = [backgroundSpeed, roadLength]
                    self.initialLinkSpeedLength[i][roadID] = speedLength
                    self.roadLengthMap[roadID] = roadLength

        for i in range(int(self.args.SIMULATION_STOP_TIME * self.args.SIMULATION_STEP_SIZE//3600)+1):
            self.mab[i].warm_up(self.initialLinkSpeedLength[i])
            # self.mabBus[i].warm_up(self.initialLinkSpeedLength[i])

    def getRoadLengthMap(self):
        return self.roadLengthMap

