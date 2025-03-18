#!/bin/bash
for x in `ls spark_udaf_acc | grep java`; do python3 processor.py spark_udaf_acc/$x AccumulatorV2 add; done
