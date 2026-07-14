param map = localPath(f'../../data/CARLA/{globalParameters.town}/facility/road/{globalParameters.town}.xodr') 
param address = "10.84.200.114"

model scenic.simulators.carla.model

param num_commuters = 500
param bubble_size = 50
param timestep = 0.1
param length = 300

scenario Test():
    setup:
        num_commuters = globalParameters.num_commuters
    compose:
        for i in range(num_commuters):
            name = f"car_{i}"
            new Car with name name
            wait

scenario Main():
    setup:
        ego = new Car with name "Ego"
    compose:
        foo = Test()
        do foo for globalParameters.length seconds


