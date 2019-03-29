package bryan

import org.apache.arrow.flight.{FlightClient, FlightDescriptor, Location}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.arrow.ArrowRDD

class SparkFlightConnector() {

  def put(dataset: DataFrame, host: String, port: Int, descriptor: String): Unit = {
    val rdd = new ArrowRDD(dataset)

    rdd.mapPartitions { it =>
      val allocator = it.allocator.newChildAllocator("SparkFlightConnector", 0, Long.MaxValue)

      val client = new FlightClient(allocator, new Location(host, port))
      val desc = FlightDescriptor.path(descriptor)

      val stream = client.startPut(desc, it.root)

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

}
