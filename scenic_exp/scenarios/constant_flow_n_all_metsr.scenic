param map = "CARLA_TOWN5"
param num_commuters = 500

model scenic.simulators.metsr.model

param timestep = 0.1

scenario Test():
    setup:
        num_commuters = globalParameters.num_commuters
        stime = 0
        etime = 50 * 10 # simulation length
    compose:
        for i in range(num_commuters):
            new PrivateCar with origin -1, with destination -1
            wait

scenario Main():
    setup:
        GeneratePrivateTrip(-1,-1)
    compose:
        foo = Test()
        do foo for globalParameters.length seconds

