# SparkArrowFlight

## How to Use

This is an example to demonstrate a basic Apache Arrow Flight data service with Apache Spark 
and TensorFlow clients. The service uses a simple producer with an `InMemoryStore` from the 
Arrow Flight examples. This allows clients to put/get Arrow streams to an in-memory store.

The Spark client maps partitions of an existing DataFrame to produce an Arrow stream for each 
partition that is put in the service under a string based `FlightDescriptor`. Then a PyArrow
client reads each Arrow stream to produce a Pandas DataFrame. Optionally (if TF installed), 
a TensorFlow client reads each Arrow stream, one at a time, into an `ArrowStreamDataset` so 
records can be iterated over as Tensors.

## Python Prerequisites

* Python 3.8
* PySpark 3.1.1
* PyArrow 2.0.0
* Optionally: TensorFlow, TensorFlow I/O

## Usage

In one terminal, start the Arrow Flight service on port 8888

```bash
$ bin/run_flight_server.sh
```

In another terminal, start the example to run a PySpark application to put data to the 
service and then create pyarrow and TensorFlow clients to conusume it.

```bash
$ bin/run_flight_example.sh
```
>>>>>>> 051176b (Updated for Spark 3.1.1, made seperate script to run service, added pyarrow client read)
