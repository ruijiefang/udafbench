#!/bin/bash
for x in `ls spark_udaf_agg | grep scala`; do python3 processor.py spark_udaf_agg/$x Aggregator reduce; done
