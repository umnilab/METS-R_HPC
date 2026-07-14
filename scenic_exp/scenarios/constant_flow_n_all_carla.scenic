param map = localPath(f'../../data/CARLA/{globalParameters.town}/facility/road/{globalParameters.town}.xodr') 
param xml_map = localPath(f'../../data/CARLA/{globalParameters.town}/facility/road/{globalParameters.town}.net.xml') 

param address = "10.0.0.122"
param bubble_size = 100000
param num_commuters = 500
param timestep = 0.1
param length = 300


model scenic.simulators.cosim.model

scenario Test():
    setup:
        num_commuters = globalParameters.num_commuters
        stime = 0
        etime = 50 * 10 # simulation length
    compose:
        for i in range(num_commuters):
            name = f"car_{i}"
            new NPCCar with origin -1, with destination -1, with name name
            wait

scenario Main():
    setup:
        ego = new EgoCar with name "ego"
    compose:
        foo = Test()
        do foo for globalParameters.length seconds
