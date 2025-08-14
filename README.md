This is the repository for the HPC module of [METS-R SIM](https://github.com/umnilab/METS-R_SIM). Docker is required and it is highly recommended to go through the interactive_example.ipynb notebook. For the latest instructions, please refer to the online [document](https://umnilab.github.io/METS-R_doc).

## New: CARLA Visualization Integration

This repository now includes enhanced CARLA visualization functionality that allows you to:

- **Start a dedicated CARLA instance** for METS-R simulation visualization
- **Display METS-R vehicles in real-time** within the CARLA environment
- **Automatically synchronize** vehicle positions between METS-R simulation and CARLA
- **Manage vehicle lifecycle** (spawning, updating, cleanup) automatically

### Quick Start with CARLA Visualization

```bash
# Start simulation with CARLA visualization
python cosim_example.py --carla_viz

# Use custom CARLA map and port
python cosim_example.py --carla_viz --carla_map Town02 --carla_port 2001

# Run the dedicated example script
python carla_viz_example.py
```

For detailed documentation, see [CARLA_VISUALIZATION_README.md](CARLA_VISUALIZATION_README.md).
