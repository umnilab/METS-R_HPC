#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

bash ./check_sim5g_env.sh

: "${INET_LIB_NAME:=INET}"
: "${SIMU5G_LIB_NAME:=simu5g}"

GENERATED_NED_ROOT="${GENERATED_NED_ROOT:-../.generated/sim5g-ned}"
mkdir -p "$GENERATED_NED_ROOT/metsr/veinsbridge/sim5g"
mkdir -p "$GENERATED_NED_ROOT/sim5g"

sed 's/\.template$//' sim5g/Sim5gCellularUuBridgeNetwork.ned.template > \
    "$GENERATED_NED_ROOT/Sim5gCellularUuBridgeNetwork.ned"
sed 's/\.template$//' sim5g/MetsrBsmUuApp.ned.template > \
    "$GENERATED_NED_ROOT/metsr/veinsbridge/sim5g/MetsrBsmUuApp.ned"
sed 's/\.template$//' sim5g/MetsrExternalMobility.ned.template > \
    "$GENERATED_NED_ROOT/metsr/veinsbridge/sim5g/MetsrExternalMobility.ned"
sed 's/\.template$//' sim5g/omnetpp-sim5g-uu.ini.template > \
    "$GENERATED_NED_ROOT/omnetpp-sim5g-uu.ini"
cp sim5g/demo.xml "$GENERATED_NED_ROOT/sim5g/demo.xml"

echo "Generated Simu5G NED/INI/XML files under $GENERATED_NED_ROOT"

HAD_MAKEFILE=0
if [ -f Makefile ]; then
    HAD_MAKEFILE=1
    cp Makefile Makefile.abstract
    echo "Saved existing Makefile as Makefile.abstract"
fi

opp_makemake \
    -f \
    --deep \
    --make-so \
    -O out \
    -o metsr_veins_bridge_simu5g \
    -DMETSR_WITH_SIMU5G \
    -I./src \
    -I./sim5g/src \
    -I"$INET_HOME/src" \
    -I"$SIMU5G_HOME/src" \
    -L"$INET_HOME/src" \
    -L"$SIMU5G_HOME/src" \
    -l"$INET_LIB_NAME" \
    -l"$SIMU5G_LIB_NAME"

make -j"$(nproc 2>/dev/null || echo 4)"
cp Makefile Makefile.simu5g
if [ "$HAD_MAKEFILE" -eq 1 ]; then
    cp Makefile.abstract Makefile
    echo "Restored original Makefile after Simu5G build."
else
    rm -f Makefile
    echo "Stored generated Simu5G Makefile as Makefile.simu5g."
fi

echo
echo "Simu5G bridge build complete."
echo "Run it with:"
echo "  bash ./run_sim5g_uu.sh"
