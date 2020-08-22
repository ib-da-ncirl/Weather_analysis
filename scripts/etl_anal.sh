#!/bin/bash
~/code/weather_analysis/etl/etl.py -a -i ~/etl_results/info.csv -j ~/etl_results/summary.txt -c ~/code/weather_analysis/etl/config.yaml -v
