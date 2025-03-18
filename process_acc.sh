#!/bin/bash

scala_good=`cat scala_acc.out | grep GOOD | wc -l`
scala_bad=`cat scala_acc.out | grep BAD | wc -l`
java_good=`cat java_acc.out | grep GOOD | wc -l`
java_bad=`cat java_acc.out | grep BAD | wc -l`

echo "Total number of Good Scala Programs: "$scala_good
echo "Total number of Bad Scala Programs: "$scala_bad
echo "Total number of Good Java Programs: "$java_good
echo "Total number of Bad Java Programs: "$java_bad

echo "moving all the good programs ... "

for x in `cat java_acc.out | grep GOOD | awk '{print $1;}'`; do
  suff=`echo $x| tr "/" " " | awk '{print $2;}'`
  echo "moving "$x" to spark_udaf_acc_good/"$suff
  cp $x spark_udaf_acc_good/$suff
done

for x in `cat scala_acc.out | grep GOOD | awk '{print $1;}'`; do
  suff=`echo $x| tr "/" " " | awk '{print $2;}'`
  echo "moving "$x" to spark_udaf_acc_good/"$suff
  cp $x spark_udaf_acc_good/$suff
done

