import numpy as np 
import math
from .Mab import MAB

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
            self.time_od[od] += 1
        else:
            self.time_od[od] = 1
        UCB = {}
        mean_award = {}
        for i in range(len(self.valid_path[od])):
            path_id = self.valid_path[od][i]
            path = self.path_info[od][path_id]
            A = 0
            B = 0
            path_length = len(path)
            for j in range(path_length):
                A += self.generated_visit_energy[path[j]]/self.generated_visit_count[path[j]] # mean
                B += -np.sqrt((1.5*(np.log(self.time_od[od])))/(self.generated_visit_count[path[j]]))/path_length # Upper confidence bound
            UCB[path_id] =  A+B
            mean_award[path_id] = A

        minIndex = min(UCB, key=UCB.get)

        return minIndex


    # Override the updateLinkUCB in Mab.java
    def updateLinkUCB(self, road_id, energy, count = 1):
        # update should be only summation of linkucb  delete information after updation
        if road_id in self.visit_count:
            self.visit_count[road_id] += count
            self.visit_energy[road_id] += energy

    def updateShadowBus(self, road_id, travel_time, length):
        self.visit_count_vehicle[road_id] += 1
        self.generated_visit_count[road_id] +=  1
        energyAdd = 0
        self.visit_speed_vehicle[road_id].append(travel_time)
        energyAdd += self.calculateBusEnergy(travel_time, length)
        self.generated_visit_energy[road_id] += energyAdd

    def calculateBusEnergy(self, travel_time, length):
        speed = length/travel_time
        x = speed/(1609.3/3600.0);  #unit of x: mile/hour
        energy = (length/1609.3)/(0.001*0.006*x*x*x - 0.001*x*x + 0.0402*x + 0.1121);  #unit: kWh
        return energy

    def warm_up(self, initialLinkSpeedLength):
        for linkID in initialLinkSpeedLength.keys():
            self.visit_count[linkID] = 1
            self.visit_count_vehicle[linkID] = 0
            self.generated_visit_count[linkID] = 1
            velocity = initialLinkSpeedLength[linkID][0]
            length = initialLinkSpeedLength[linkID][1]
            velocity *= 3600/1609.34
            energy = 0.000006*velocity*velocity*velocity - 0.001*velocity*velocity + 0.0402*velocity + 0.1121
            energy = (length/1609.34)/energy
            self.visit_energy[linkID] = energy
            self.visit_speed_vehicle[linkID] = []
            self.generated_visit_energy[linkID] = energy
