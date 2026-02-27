#!/usr/bin/env bash
set -e

python -V
pip -V

# Force wheel-only installs (no building scipy from source)
pip install --upgrade pip
pip install --only-binary=:all: -r requirements.txt
