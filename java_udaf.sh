#!/bin/bash
for x in `ls spark_udaf_udaf | grep java`; do python3 processor.py spark_udaf_udaf/$x UserDefinedAggregateFunction update; done
