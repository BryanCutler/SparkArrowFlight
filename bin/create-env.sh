#!/bin/bash

conda create -n spark-flight -c conda-forge python=3.8 pyarrow=2.0.0 pyspark=3.1.1 pandas

# conda activate spark-flight
# [optional] pip install tensorflow tensorflow-io