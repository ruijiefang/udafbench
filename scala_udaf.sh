#!/bin/bash
for x in `ls spark_udaf_udaf | grep scala`; do python3 processor.py spark_udaf_udaf/$x UserDefinedAggregateFunction update; done
