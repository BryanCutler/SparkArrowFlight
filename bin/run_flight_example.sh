#!/bin/bash

# Using tfio-test env

SPARK_HOME=$HOME/git/spark
M2_LOCAL_REPO=$HOME/.m2/repository

PYSPARK_PYTHON=`which python` $SPARK_HOME/bin/spark-submit \
    --jars "target/FlightConnector-1.0-SNAPSHOT.jar,$M2_LOCAL_REPO/org/apache/arrow/arrow-flight/0.13.0-SNAPSHOT/arrow-flight-0.13.0-SNAPSHOT-shaded.jar,$M2_LOCAL_REPO/org/apache/arrow/arrow-flight/0.13.0-SNAPSHOT/arrow-flight-0.13.0-SNAPSHOT-jar-with-dependencies.jar" \
    --py-files "src/main/python/spark_flight_connector.py" \
     src/main/python/flight_example.py