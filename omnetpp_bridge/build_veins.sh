#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
: "${VEINS_HOME:?Set VEINS_HOME to a built Veins 5.3.1 checkout}"
command -v opp_makemake >/dev/null 2>&1 || {
    echo "opp_makemake was not found. Source OMNeT++ setenv first." >&2
    exit 1
}
test -f "$VEINS_HOME/src/veins/veins.h" || {
    echo "VEINS_HOME does not point to a Veins source tree: $VEINS_HOME" >&2
    exit 1
}

GENERATED_NED_ROOT="${VEINS_GENERATED_NED_ROOT:-.generated/veins-ned}"
mkdir -p "$GENERATED_NED_ROOT"
cp MetsrVeinsBridge.ned "$GENERATED_NED_ROOT/MetsrVeinsBridge.ned"
for ned_name in \
    MetsrVeins80211pNetwork \
    MetsrVeinsApp \
    MetsrVeinsExternalMobility \
    MetsrVeinsVehicle; do
    cp "veins/$ned_name.ned.template" "$GENERATED_NED_ROOT/$ned_name.ned"
done

echo "Generated Veins-only NED files under $GENERATED_NED_ROOT"

had_makefile=0
makefile_backup=""
if [ -f Makefile ]; then
    had_makefile=1
    makefile_backup="$(mktemp ./Makefile.pre-veins.XXXXXX)"
    cp Makefile "$makefile_backup"
fi

restore_makefile() {
    if [ "$had_makefile" -eq 1 ]; then
        cp "$makefile_backup" Makefile
        rm -f "$makefile_backup"
    else
        rm -f Makefile
    fi
}
trap restore_makefile EXIT

opp_makemake \
    -f \
    --deep \
    -Xsim5g \
    --make-so \
    -O out/veins \
    -o metsr_veins_bridge_80211p \
    -DMETSR_WITH_VEINS \
    -I./src \
    -I./src/veins \
    -I"$VEINS_HOME/src" \
    -I"$VEINS_HOME/src/veins" \
    -L"$VEINS_HOME/src" \
    -lveins

make -j"$(nproc 2>/dev/null || echo 4)"
cp Makefile Makefile.veins

echo "Real Veins 802.11p bridge build complete."
echo "Run: bash ./run_veins_80211p.sh"
