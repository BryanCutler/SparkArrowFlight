package bryan

import java.io.BufferedOutputStream
import java.net.{InetAddress, ServerSocket, Socket}

import scala.collection.JavaConverters._

import scopt.OptionParser

import org.apache.arrow.flight._
import org.apache.arrow.memory.{BaseAllocator, BufferAllocator, RootAllocator}
import org.apache.arrow.util.AutoCloseables
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.spark.network.util.JavaUtils


class FlightToStream(incomingAllocator: BufferAllocator, location: Location) extends AutoCloseable {

  private val allocator = incomingAllocator.newChildAllocator("flight-to-stream-client", 0, Long.MaxValue)
  private val client = new FlightClient(allocator, location)

  def getFlightInfo(descriptor: FlightDescriptor): FlightInfo = {
    client.getInfo(descriptor)
  }

  def run(info: FlightInfo, processStream: FlightStream => Unit): Unit = {

    val endpoints = info.getEndpoints.asScala
    if (endpoints.isEmpty) {
      throw new RuntimeException("No endpoints returned from Flight server.")
    }

    endpoints.foreach { endpoint =>
      val locations = endpoint.getLocations.asScala.toList match {
        case Nil => location :: Nil
        case l => l
      }

      locations.foreach { l =>
        val readClient = if (location.getHost == l.getHost && location.getPort == l.getPort) {
          client
        } else {
          new FlightClient(allocator, l)  // TODO: need to close
        }
        val stream = readClient.getStream(endpoint.getTicket)
        val root = stream.getRoot
        try {
          processStream(stream)
        } finally {
          root.close()
        }
      }
    }
  }

  override def close(): Unit = AutoCloseables.close(client, allocator)
}

object FlightToStream {

  case class Config(
      host: String = "localhost",
      port: Int = 8888,
      streamPort: Int = 8889,
      flightDescriptor: String = "spark-flight-descriptor")

  def main(args: Array[String]): Unit = {

    val parser = new OptionParser[Config]("FlightToStream"){
      head("FlightToStream: Flight client that will translate a Flight to a single Arrow stream.")
      opt[String]("host")
        .optional()
        .text(s"IP address of the Flight Service")
        .action((x, c) => c.copy(host = x))
      opt[Int]("port")
        .optional()
        .text(s"Port number of the Flight Service")
        .action((x, c) => c.copy(port = x))
      opt[Int]("stream-port")
        .optional()
        .text(s"Port number to serve Arrow stream")
        .action((x, c) => c.copy(streamPort = x))
      opt[String]("flight-descriptor")
        .optional()
        .text(s"FlightDescriptor path string")
        .action((x, c) => c.copy(flightDescriptor = x))
    }

    System.setProperty(BaseAllocator.DEBUG_ALLOCATOR, "true")

    val defaultConfig = Config()
    parser.parse(args, defaultConfig) match {
      case Some(config) =>
        run(config)
      case _ => sys.exit(1)
    }
  }

  def run(config: Config): Unit = {
    val allocator = new RootAllocator(Long.MaxValue)
    val location = new Location(config.host, config.port)
    val client = new FlightToStream(allocator, location)

    new Thread("flight-to-stream-server") {
      setDaemon(true)
      override def run(): Unit = {
        println("FlightToStream server starting")

        while (true) {
          val serverSocket = new ServerSocket(config.streamPort, 1, InetAddress.getByAddress(Array(127, 0, 0, 1)))
          var sock: Socket = null
          try {
            sock = serverSocket.accept()
            val out = new BufferedOutputStream(sock.getOutputStream)

            val descriptor = FlightDescriptor.path(config.flightDescriptor)
            val info = client.getFlightInfo(descriptor)

            val writerAllocator = allocator.newChildAllocator("flight-to-stream-writer", 0, Long.MaxValue)
            val writerRoot = VectorSchemaRoot.create(info.getSchema, writerAllocator)
            val writer = new ArrowStreamWriter(writerRoot, null, out)

            client.run(info, (stream: FlightStream) => {
              val streamRoot = stream.getRoot
              val loader = new VectorLoader(writerRoot)
              val unloader = new VectorUnloader(streamRoot)
              while (stream.next()) {
                val batch = unloader.getRecordBatch
                loader.load(batch)
                writer.writeBatch()
                batch.close()
              }
            })

            writer.close()
            writerRoot.close()
            writerAllocator.close()

          } finally {
            JavaUtils.closeQuietly(serverSocket)
            JavaUtils.closeQuietly(sock)
          }
        }
      }
    }.start()

    Runtime.getRuntime().addShutdownHook(
      new Thread("shutdown-closing-thread") {
        override def run(): Unit = {
          println("FlightToStream server closing")
          AutoCloseables.close(client, allocator)
        }
      }
    )

    while (true) {
      Thread.sleep(60000)
    }
  }

}
