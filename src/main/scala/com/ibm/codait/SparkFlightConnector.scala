package com.ibm.codait

import scala.collection.JavaConverters._
import org.apache.arrow.flight.{AsyncPutListener, FlightClient, FlightDescriptor, Location}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.ArrowRDD

class SparkFlightConnector() {

  def put(dataset: DataFrame, host: String, port: Int, descriptor: String): Unit = {
    val rdd = new ArrowRDD(dataset)

    rdd.mapPartitions { it =>
      val allocator = it.allocator.newChildAllocator("SparkFlightConnector", 0, Long.MaxValue)

      val client = FlightClient.builder(allocator, Location.forGrpcInsecure(host, port)).build()
      val desc = FlightDescriptor.path(descriptor)

      val stream = client.startPut(desc, it.root, new AsyncPutListener())

      // Use VectorSchemaRootIterator to convert Rows -> Vectors
      it.foreach { root =>
        // doPut on the populated VectorSchemaRoot
        stream.putNext()
      }

      stream.completed()
      // Need to call this, or exceptions from the server get swallowed
      stream.getResult

      client.close()

      Iterator.empty
    }.count()
  }

  def exchange(dataset: DataFrame,
               host: String,
               port: Int,
               putDescriptor: String,
               getDescriptor: String): RDD[InternalRow] = {

    val rdd = new ArrowRDD(dataset)

    rdd.mapPartitions { it =>

      // doPut
      val allocator = it.allocator.newChildAllocator("SparkFlightConnector", 0, Long.MaxValue)

      val client = FlightClient.builder(allocator, Location.forGrpcInsecure(host, port)).build()
      val desc = FlightDescriptor.path(putDescriptor)

      val stream = client.startPut(desc, it.root, new AsyncPutListener)

      // Use VectorSchemaRootIterator to convert Rows -> Vectors
      it.foreach { root =>
        // doPut on the populated VectorSchemaRoot
        stream.putNext()
      }

      stream.completed()
      // Need to call this, or exceptions from the server get swallowed
      stream.getResult

      // doGet
      val info = client.getInfo(FlightDescriptor.path(getDescriptor))

      val endpoints = info.getEndpoints.asScala
      if (endpoints.isEmpty) {
        throw new RuntimeException("No endpoints returned from Flight server.")
      }

      endpoints.foreach { endpoint =>
        val locations = endpoint.getLocations.asScala.toList
        if (locations.length > 1) {
          throw new UnsupportedOperationException("Only one location supported")
        }
        val location = locations.head
        val stream = client.getStream(endpoint.getTicket)
        val root = stream.getRoot
        try {
          //processStream(stream)
          while (stream.next()) {
            // convert root to ColumnarBatch
            /*
            val root = reader.getVectorSchemaRoot()
            val columns = root.getFieldVectors.asScala.map { vector =>
              new ArrowColumnVector(vector).asInstanceOf[ColumnVector]
            }.toArray

            val batch = new ColumnarBatch(columns)
            batch.setNumRows(root.getRowCount)
            batch.rowIterator().asScala
            */
          }
        } finally {
          root.close()
        }
      }

      client.close()

      Iterator.empty  // Need Iterator[ColumnarBatch].map(_.rowIterator().asScala)
    }
  }

}
