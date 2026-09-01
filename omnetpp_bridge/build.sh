#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if ! command -v opp_makemake >/dev/null 2>&1; then
    echo "opp_makemake was not found. Source setenv from your OMNeT++ install first." >&2
    exit 1
fi

opp_makemake -f --deep -Xsrc/veins --make-so -O out -o metsr_veins_bridge

make -j"$(nproc 2>/dev/null || echo 4)"
