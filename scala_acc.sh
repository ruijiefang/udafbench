#!/bin/bash
for x in `ls spark_udaf_acc | grep scala`; do python3 processor.py spark_udaf_acc/$x AccumulatorV2 add; done
