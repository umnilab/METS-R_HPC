import os
import sys
import json
from os import path
from eco_routing.Mab import MAB
from eco_routing.MabBus import MABBus

class MABManager(object):
    def __init__(self, working_dir, args):
        self.mab = {} # MAB model for E-Taxis, every hour we train a new model
        self.mabBus = {} # MAB model for E-Buses
        self.initialLinkSpeedLength = []
        self.roadLengthMap = {}

        self.path_info = {}
        self.valid_path = {}
        self.path_info_bus = {}
        self.valid_path_bus = {}

        self.working_dir = working_dir

        for hour in range(int(args.SIMULATION_STOP_TIME * args.SIMULATION_STEP_SIZE//3600)):
            self.mab[hour] = MAB(self.path_info, self.valid_path)
            self.mabBus[hour] = MABBus(self.path_info_bus, self.valid_path_bus)
            self.initialLinkSpeedLength.append({})

    def ucbRouting(self, od_str, hour):
        self.mab[hour].play(od_str)
        return mab[hour].getAction()

    def refreshLinkUCB(self, new_linkUCBMap):
        hour = 0
        # TODO: Change to json
        for IDhour in new_linkUCBMap:
            hour = int(IDhour.split(";")[1])
        self.mab[hour].updateLinkUCB(new_linkUCBMap)
        return hour

    def refreshRouteUCB(self, new_routeUCBMap):
        for hour in range(int(args.SIMULATION_STOP_TIME * args.SIMULATION_STEP_SIZE//3600)):
            self.mab[hour].updateRouteUCB(new_routeUCBMap)

    def ucbRoutingBus(self, od_str, hour):
        self.mabBus[hour].play(od_str)
        return mabBus[hour].getAction()

    def refreshLinkUCBBus(self, new_linkUCBMapBus):
        hour = 0
        # TODO: Change to json
        for IDhour in new_linkUCBMapBus:
            hour = int(IDhour.split(";")[1])
        self.mabBus[hour].updateLinkUCBBus(new_linkUCBMapBus)
        return hour

    def refreshRouteUCBBus(self, new_routeUCBMapBus):
        for hour in range(int(args.SIMULATION_STOP_TIME * args.SIMULATION_STEP_SIZE//3600)):
            self.mabBus[hour].updateRouteUCB(new_routeUCBMapBus)

    def refreshLinkUCBShadow(self, new_speedUCBMap, lengthUCB):
        hour = 0
        # TODO: Change to json
        for IDhour in new_speedUCBMap:
            hour = int(IDhour.split(";")[1])
        self.mabBus[hour].updateShadowBus(new_speedUCBMap, lengthUCB)
        return hour


    # Initialize speed data for each link
    def initializeLinkEnergy1(self):
        try:
            fileName1 = self.working_dir + "data/NYC/background_traffic/background_traffic_NYC_one_week.csv";
            with open(fileName1, 'r') as f:
                f.readline()
                for line in f.readlines():
                    result = line.split(",")
                    roadID = int(result[0])
                    backgroundSpeed = 0
                    for i in range(int(args.SIMULATION_STOP_TIME * args.SIMULATION_STEP_SIZE//3600)):
                        backgroundSpeed = float(result[i])
                        speedLength  = [backgroundSpeed]
                        self.initialLinkSpeedLength[i][roadID] = speedLength
        except:
            pass

    # Initialize link length data for each link
    def intializeLinkEnergy2(self):
        try:
            fileName2 = self.working_dir + "data/NYC/background_traffic/background_traffic_NYC_one_week.csv";
            with open(fileName2, 'r') as f:
                f.readline()
                for line in f.readlines():
                    result = line.split(",")
                    roadID = int(result[0])
                    roadLength = float(result[-1])
                    for i in range(int(args.SIMULATION_STOP_TIME * args.SIMULATION_STEP_SIZE//3600)):
                        speedLength = [self.initialLinkSpeedLength[i][roadID][0], roadLength]
                        self.initialLinkSpeedLength[i][roadID] = speedLength
                        self.roadLengthMap[roadID] = roadLength
        except:
            pass

        for i in range(int(args.SIMULATION_STOP_TIME * args.SIMULATION_STEP_SIZE//3600)):
            self.mab[i].warm_up(self.initialLinkSpeedLength[i])
            self.mabBus[i].warm_up_bus(self.initialLinkSpeedLength[i])

    def getRoadLengthMap(self):
        return self.roadLengthMap

