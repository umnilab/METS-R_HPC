"""CARLA-only hybrid physics with a 50 m full-physics radius. See README.md."""
param backend = "carla"
param num_commuters = 500
param length = 300
param bubble_size = 50
# This fork enables hybrid physics during rendering setup.
param render = 1
param spawn_interval_s = globalParameters.get("timestep", 0.1)
from traffic_template import Main
