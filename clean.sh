#!/usr/bin/bash

echo ">>> Clean python cache."
rm -rf `find . -name "__pycache__"`

echo ">>> Clean directories."
rm -rf db
rm -rf temp

echo ">>> Clean config file."
rm -rf monitor/conf/config.py

echo ">>> Clean compile file."
rm -rf openlavaMonitor.egg-info
