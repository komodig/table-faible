#!/bin/bash

MODULES="mappings"

source venv/bin/activate

for mod in $MODULES; do
  mod_=$mod.py
  echo "$mod_"
  python3 -m flake8 --statistics --count $mod_
  python3 -m isort --check-only $mod_
  python3 -m isort $mod_
done