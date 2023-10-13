#!/bin/bash

APP="core"

source venv/bin/activate

for mod in $APP; do
  echo "$mod"
  python3 -m flake8 --statistics --count $mod
  python3 -m isort --check-only $mod
  python3 -m isort $mod
done