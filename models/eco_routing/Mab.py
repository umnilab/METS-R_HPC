import numpy as np 
import math

class MAB(object):
    def __init__(self, path_info, valid_path):
        self.path_info = path_info # K-shortest pahts for n ODs
        self.valid_path = valid_path # Valid OD ID for each path
        for i in path_info: # How many paths per OD pair
            self.npath = len(path_info[i])
            break

        # self.tT = 0 
        self.T = 0
        self.visit_energy = {} # Cumulative energy consumption for each link
        self.visit_count = {} # Cumulative visited time for each link since time 0
        self.eliminate_threshold = 5 # a threshold in action elimination
        self.time_od = {} # visiting time for each OD pair

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
                A += self.visit_energy[path[j]]/self.visit_count[path[j]] # mean
                B += -np.sqrt((1.5*(np.log(self.time_od[od])))/(self.visit_count[path[j]]))/path_length # Upper confidence bound
            UCB[path_id] = A + B
            mean_award[path_id] = A

        minIndex = min(UCB, key=UCB.get)

        return minIndex

    def updateLinkUCB(self, road_id, energy, count = 1):
        # update should be only summation of linkucb  delete information after updation
        if road_id in self.visit_count:
            self.visit_count[road_id] += count
            self.visit_energy[road_id] += energy

    def warm_up(self, initialLinkSpeedLength):
        for linkID in initialLinkSpeedLength.keys():
            self.visit_count[linkID] = 1
            velocity = 10 # 10 m/s
            length = initialLinkSpeedLength[linkID][1]
            velocity *= 3600/1609.34
            energy = 0.00004*velocity*velocity*velocity - 0.0069*velocity*velocity + 0.3146*velocity + 3.0933
            energy = (length/1609.34)/energy
            self.visit_energy[linkID] = energy

    def m_eliminate(self, od, path_energy_list):
        random_alpha = np.exp(3*(self.T/100) - 1)
        if (self.T >= self.eliminate_threshold) and (len(self.valid_path[od])>10) and (math.random() < random_alpha):
            eliminate_alpha = np.percentile(self.path_energy_list, 90)
            temp_valid_path = []
            for i in range(len(path_energy_list)):
                if path_energy_list[i] <= eliminate_alpha:
                    temp_valid_path.append(self.valid_path[od][i])
            self.valid_path[od] = temp_valid_path

