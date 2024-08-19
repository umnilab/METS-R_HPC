from scenic.core.simulators import Simulation, Simulator

# TODO: 
# 1. implement all APIs
# 2. Test with Eric
# 3. Write a Scneic Runner to use it
class METSRSimulator(Simulator):
      """ Implementation of the Simulator for METS-R """

      def __init__(self, config):
            pass

      def createSimulation(self, scene, *, timestep, **kwargs):
            pass

      def createObjectInSimulator(self, ob):
            pass

      def step(self):
            pass

      def getProperties(self, obj, properties):
            pass

      def destory(self):
            # remove all agents in METS-R
            super().destroy()

      def executeActions(self, allActions):
            pass

      