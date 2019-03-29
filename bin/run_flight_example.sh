#!/bin/bash

# Using tfio-test env

SPARK_HOME=/home/bryan/git/spark

PYSPARK_PYTHON=`which python` $SPARK_HOME/bin/spark-submit \
    --jars "/home/bryan/projects/SparkArrowFlight/target/FlightConnector-1.0-SNAPSHOT.jar,/home/bryan/.m2/repository/org/apache/arrow/arrow-flight/0.13.0-SNAPSHOT/arrow-flight-0.13.0-SNAPSHOT-shaded.jar,/home/bryan/.m2/repository/org/apache/arrow/arrow-flight/0.13.0-SNAPSHOT/arrow-flight-0.13.0-SNAPSHOT-jar-with-dependencies.jar" \
    --py-files "/home/bryan/projects/SparkArrowFlight/src/main/python/spark_flight_connector.py" \
     /home/bryan/projects/SparkArrowFlight/src/main/python/flight_example.py