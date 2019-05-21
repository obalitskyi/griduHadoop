#!/bin/bash

spark2-submit --class obalitskyi.gridu.spark.App --master yarn --deploy-mode client --executor-memory 480m --executor-cores 1 \
 griduHadoop-assembly-0.1.jar /user/obalitskyi/events/2019/*/* /user/obalitskyi/geodata/ip /user/obalitskyi/geodata/ipinfo 3
