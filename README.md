# SparkArrowFlight

This is an example to demonstrate a basic Apache Arrow Flight data service with Apache Spark and TensorFlow clients. The service uses a simple producer with an `InMemoryStore` from the Arrow Flight examples. This allows clients to put/get Arrow streams to an in-memory store.

The Spark client maps partitions of an existing DataFrame to produce an Arrow stream for each partition that is put in the service under a string based `FlightDescriptor`. The TensorFlow client reads each Arrow stream, one at a time, into an `ArrowStreamDataset` so records can be iterated over as Tensors.

## How to Use

This example can be run using the shell script `./run_flight_example.sh` which starts the service, runs the Spark client to put data, then runs the TensorFlow client to get the data. NOTE: at the time this was made, it dependended on a working copy of unreleased Arrow v0.13.0. This might need to be updated in the example and in Spark before building.
