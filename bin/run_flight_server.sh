#!/bin/bash

PORT_NUM=${1:-8888}

java -cp target/SparkFlightConnector-1.0-SNAPSHOT-jar-with-dependencies.jar com.ibm.codait.SparkFlightServer --port ${PORT_NUM}