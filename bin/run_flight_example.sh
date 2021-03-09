#!/bin/bash

#SPARK_HOME=$HOME/git/spark
#SPARK_SUBMIT=$SPARK_HOME/bin/spark-submit

SPARK_SUBMIT=spark-submit
M2_LOCAL_REPO=$HOME/.m2/repository

PYSPARK_PYTHON=`which python` $SPARK_SUBMIT \
    --jars "target/SparkFlightConnector-1.0-SNAPSHOT.jar,$M2_LOCAL_REPO/org/apache/arrow/flight-core/2.0.0/flight-core-2.0.0-shaded-ext.jar,$M2_LOCAL_REPO/org/apache/arrow/flight-core/2.0.0/flight-core-2.0.0-jar-with-dependencies.jar" \
    --py-files "src/main/python/spark_flight_connector.py" \
     src/main/python/flight_example.py

#,$M2_LOCAL_REPO/org/apache/arrow/flight-grpc/2.0.0/flight-grpc-2.0.0.jar