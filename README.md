This is the repository for the HPC module of [METS-R SIM](https://github.com/umnilab/METS-R_SIM). Docker is required, and it is highly recommended to start with the tutorials in the [`tutorials/`](tutorials/) folder. For the latest instructions, please refer to the online [document](https://umnilab.github.io/METS-R_doc).

## Setup

1. Install Python 3.10 or newer, then install the Python dependencies from the repository root:

   ```bash
   pip install -r requirements.txt
   ```

2. Install Docker:

   - Windows/macOS: install [Docker Desktop](https://docs.docker.com/get-started/introduction/get-docker-desktop/).
   - Linux: install Docker Engine and Docker Compose using the Docker instructions for your distribution.

3. Enable localhost access for Docker host networking. METS-R launches the simulator container with Docker host networking (`--net=host`) so the Python clients can connect to simulator ports on `localhost`.

   - Docker Desktop: open **Settings > Resources > Network**, turn on **Enable host networking**, then apply and restart Docker Desktop.
   - Linux Docker Engine: host networking is available by default.

4. Confirm that Docker and Docker Compose are available:

   ```bash
   docker --version
   docker-compose --version
   ```

## Tutorials

The tutorial notebooks and runnable examples are in [`tutorials/`](tutorials/). Run commands from the repository root so relative paths such as `configs/...`, `data/...`, and `docker/...` resolve correctly. The Python tutorial scripts also switch to the repository root automatically when launched from another folder.

Recommended starting point:

```bash
jupyter lab tutorials/basic_tutorial.ipynb
```

If JupyterLab is not installed:

```bash
pip install jupyterlab
jupyter lab tutorials/basic_tutorial.ipynb
```

Additional notebooks:

- [`tutorials/advanced_commands.ipynb`](tutorials/advanced_commands.ipynb): command patterns and longer workflows.
- [`tutorials/security_examples.ipynb`](tutorials/security_examples.ipynb): security and attack-scenario examples.

Runnable tutorial scripts:

```bash
# CARLA/METS-R co-simulation example
python tutorials/cosim_example.py -r configs/run_cosim_CARLAT5.json -v

# V2X sidecar example with the lightweight Python sidecar
python tutorials/v2x_veins_cosim_example.py -r configs/run_v2x_python_sidecar_Template.json
```

## New: CARLA Visualization Integration

This repository now includes enhanced CARLA visualization functionality that allows you to:

- **Start a dedicated CARLA instance** for METS-R simulation visualization
- **Display METS-R vehicles in real-time** within the CARLA environment
- **Automatically synchronize** vehicle positions between METS-R simulation and CARLA
- **Manage vehicle lifecycle** (spawning, updating, cleanup) automatically

### Quick Start with CARLA Visualization

```bash
# Start simulation with CARLA visualization
python tutorials/cosim_example.py -r configs/run_cosim_CARLAT5.json -v

# Use a different CARLA co-simulation config
python tutorials/cosim_example.py -r configs/run_cosim_CARLAT1.json -v
```

CARLA settings such as `carla_dir`, `carla_host`, `carla_port`, and `carla_map` are defined in the selected run config under [`configs/`](configs/).

## Experimental: OMNeT++/VEINS V2X Sidecar

The Python clients include an experimental sidecar bridge for packet-level V2X
experiments.  METS-R remains the mobility/control simulator, while an external
OMNeT++/VEINS process can receive vehicle states and intended BSMs, simulate
network delivery, and return delivered BSMs plus link metrics.

```bash
# Run with the lightweight sidecar process auto-launched
python tutorials/v2x_veins_cosim_example.py -r configs/run_v2x_python_sidecar_Template.json

# Run with a real OMNeT++/VEINS bridge already listening on the configured host/port
python tutorials/v2x_veins_cosim_example.py -r configs/run_v2x_veins_Template.json

# Test the Python/Kafka pipeline without OMNeT++/VEINS
python tutorials/v2x_veins_cosim_example.py -r configs/run_v2x_veins_Template.json --local_v2x_fallback
```

The bridge protocol is documented in [docs/veins_sidecar_protocol.md](docs/veins_sidecar_protocol.md);
installation notes are in [docs/veins_installation.md](docs/veins_installation.md).

## License

Copyright (c) 2026 UMNILAB.

This repository is licensed by UMNILAB under the Creative Commons Attribution 4.0 International License. See [LICENSE](LICENSE).
