#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if ! command -v opp_makemake >/dev/null 2>&1; then
    echo "opp_makemake was not found. Source setenv from your OMNeT++ install first." >&2
    exit 1
fi

GENERATED_NED_ROOT="${ABSTRACT_GENERATED_NED_ROOT:-.generated/abstract-ned}"
mkdir -p "$GENERATED_NED_ROOT"
cp MetsrVeinsBridge.ned "$GENERATED_NED_ROOT/MetsrVeinsBridge.ned"
cp MetsrVeinsBridgeNetwork.ned "$GENERATED_NED_ROOT/MetsrVeinsBridgeNetwork.ned"

echo "Generated abstract-only NED files under $GENERATED_NED_ROOT"

opp_makemake -f --deep -Xsrc/veins --make-so -O out -o metsr_veins_bridge

make -j"$(nproc 2>/dev/null || echo 4)"

echo
echo "Abstract OMNeT++ bridge build complete."
echo "Run: bash ./run_abstract.sh"
