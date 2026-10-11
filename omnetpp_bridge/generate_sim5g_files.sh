#!/usr/bin/env bash
# Materialize the shared app files and the requested Uu/PC5 network configs.
set -euo pipefail

cd "$(dirname "$0")"
generated_root="$1"
shift

mkdir -p "$generated_root/metsr/veinsbridge/sim5g" "$generated_root/sim5g"
cp MetsrVeinsBridge.ned "$generated_root/MetsrVeinsBridge.ned"
for app in MetsrBsmUuApp MetsrBsmPc5App MetsrExternalMobility; do
    sed 's/\.template$//' "sim5g/$app.ned.template" > \
        "$generated_root/metsr/veinsbridge/sim5g/$app.ned"
done

for backend in "$@"; do
    case "$backend" in
        uu) network=Sim5gCellularUuBridgeNetwork; routing=demo.xml ;;
        pc5) network=Sim5gCv2xPc5BridgeNetwork; routing=pc5.xml ;;
        *) echo "Unknown Simu5G backend: $backend" >&2; exit 2 ;;
    esac
    sed 's/\.template$//' "sim5g/$network.ned.template" > "$generated_root/$network.ned"
    sed 's/\.template$//' "sim5g/omnetpp-sim5g-$backend.ini.template" > \
        "$generated_root/omnetpp-sim5g-$backend.ini"
    cp "sim5g/$routing" "$generated_root/sim5g/$routing"
done
