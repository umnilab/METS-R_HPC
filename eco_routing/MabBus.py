import numpy as np 
import math
from eco_routing.Mab import MAB

class MABBus(MAB):
    def __init__(self, path_info, valid_path):
        super().__init__(path_info, valid_path)
        self.visit_speed_vehicle = {} # the historical speed information transfered from the EV.
        self.visit_count_vehicle = {} # the count of links that is visited by EV.
        self.generated_visit_energy = {} # combination of historical data from bus and vehicle, from both the Bus energy data and taxi speed data
        self.generated_visit_count = {} # combination of historical data from bus and vehicle.

    # Override the play() in Mab.java
    def play(self, od):
        if od in self.time_od:
            time_od[od] += 1
        else:
            time_od[od] = 1
        UCB = {}
        mean_award = {}
        for i in range(len(self.valid_path)):
            path_id = self.valid_path[odstr][i]
            path = self.path_info[odstr][path_id]
            A = 0
            B = 0
            path_length = len(path)
            for j in range(path_length):
                A += self.generated_visit_energy[path[j]]/self.generated_visit_count[path[j]] # mean
                B += -np.sqrt((1.5*(np.log(time_od[od])))/(generated_visit_count[path[j]]))/path_length # Upper confidence bound
            UCB[path_id] =  A+B
            mean_award[path_id] = AT

        minIndex = min(UCB, key=UCB.get)
        minEnergy  = UCB[minIndex]

        self.action = minIndex
        self.minEnergy = minEnergy
        self.mean_award = mean_award.values()

    # Override the updateLinkUCB in Mab.java
    def updateLinkUCB(self, linkUCB):
        for IDhour in self.linkUCB.keys():
            ID = int(IDhour.split(";")[0])
            energyRecordAdd = len(self.linkUCB[ID]) - self.visit_count[ID]
            if energyRecordAdd > 0:
                self.visit_count[ID] += energyRecordAdd
                self.generated_visit_count[ID] += energyRecordAdd # also updated the combined counter
                energyAdd = 0
                for j in range(len(energyRecordAdd)):
                    energyAdd += linkUCB[ID][-1-j]
                self.visit_energy[ID] += energyAdd
                self.generated_visit_energy[ID] += energyAdd

    def updateShadowBus(self, speedUCB, lengthUCB):
        for IDhour in speedUCB.keys():
            ID = int(IDhour.split(";")[0])
            speedRecordAdd = len(speedUCB[ID]) - self.visit_count_vehicle[ID]
            if speedRecordAdd > 0:
                visit_count_vehicle[ID] += speedRecordAdd
                generated_visit_count +=  speedRecordAdd
                energyAdd = 0
                for j in range(speedRecordAdd):
                    newSpeed = speedUCB[ID][-1 - j]
                    self.visit_speed_vehicle[ID].append(newSpeed)
                    energyAdd += calculateBusEnergy(newSpeed, lengthUCB[ID])
                generated_visit_energy[ID] += energyAdd

    def updateRouteUCB(self, routeUCB):
        for od in self.routeUCB.keys():
            roads = routeUCB[od].copy()
            self.path_info[od] = roads
            vpath = list(range(len(roads))) # assume all the path are valid
            self.valid_path[od] = vpath

    def calculateBusEnergy(speed, length):
        x = speed/(1609.3/3600.0);  #unit of x: mile/hour
        energy = (length/1609.3)/(0.001*0.006*x*x*x - 0.001*x*x + 0.0402*x + 0.1121);  #unit: kWh
        return energy;

    def warm_up(self, initialLinkSpeedLength):
        for linkID in initialLinkSpeedLength.keys():
            self.visit_count[linkID] = 1
            self.visit_count_vehilce = 0
            self.generated_visit_count = 1
            velocity = initialLinkSpeedLength[linkID][0]
            length = initialLinkSpeedLength[linkID][1]
            velocity *= 3600/1609.34
            energy = 0.000006*velocity*velocity*velocity - 0.001*velocity*velocity + 0.0402*velocity + 0.1121
            energy = (length/1609.34)/energy
            self.visit_energy[linkID] = energy
            self.visit_speed_vehicle[linkID] = []
            self.generated_visit_energy[linkID] = energy
