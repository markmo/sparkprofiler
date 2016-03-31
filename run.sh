#!/usr/bin/env bash
java -cp ./libs/*:/Users/markmo/libs/spark-1.5.1-bin-hadoop2.6/lib/*:build/libs/sparkprofiler-1.0.jar io.metamorphic.sparkprofiler.Application $1 $2 $3
