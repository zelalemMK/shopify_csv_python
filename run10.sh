#!/bin/bash

for i in {1..10} 
do
    python3 convert_cpu.py ./pre-output/output_$i.csv &
done