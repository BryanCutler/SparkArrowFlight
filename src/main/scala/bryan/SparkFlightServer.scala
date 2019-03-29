package bryan

import scopt.OptionParser

import org.apache.arrow.flight.{Location, FlightServer}
import org.apache.arrow.flight.auth.ServerAuthHandler
import org.apache.arrow.flight.example.InMemoryStore
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.util.AutoCloseables


class SparkFlightServer(incomingAllocator: BufferAllocator, val location: Location) extends AutoCloseable {
  private val allocator = incomingAllocator.newChildAllocator("spark-flight-server", 0, Long.MaxValue)
  private val mem = new InMemoryStore(this.allocator, location)
  private val flightServer = new FlightServer(allocator, location.getPort, mem, ServerAuthHandler.NO_OP)

  def start(): Unit = {
    println("Spark Flight server starting")
    flightServer.start()
  }

  override def close(): Unit = {
    AutoCloseables.close(flightServer, mem, allocator)
  }

  /*
  class SparkProducer() extends FlightProducer {
    override def getStream(ticket: Ticket, serverStreamListener: ServerStreamListener): Unit = {  }
    override def listFlights(criteria: Criteria, streamListener: StreamListener[FlightInfo]): Unit = {  }
    override def getFlightInfo(flightDescriptor: FlightDescriptor): FlightInfo = {  }
    override def acceptPut(flightStream: FlightStream): Callable[Flight.PutResult] = {  }
    override def doAction(action: Action): Result = {  }
    override def listActions(streamListener: StreamListener[ActionType]): Unit = {  }
  }
  */

}

object SparkFlightServer {

  case class Config(
      host: String = "localhost",
      port: Int = 8888)

  def main(args: Array[String]): Unit = {

    val parser = new OptionParser[Config]("SparkFlightServer"){
      head("SparkFlightServer: example server for Spark Flight usage.")
      opt[String]("host")
        .optional()
        .text(s"IP address of the Flight Service")
        .action((x, c) => c.copy(host = x))
      opt[Int]("port")
        .optional()
        .text(s"Port number of the Flight Service")
        .action((x, c) => c.copy(port = x))
    }

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

    val server = new SparkFlightServer(allocator, location)
    server.start()

    Runtime.getRuntime().addShutdownHook(
      new Thread("shutdown-closing-thread") {
        override def run(): Unit = {
          println("Spark Flight server closing")
          AutoCloseables.close(server, allocator)
        }
      }
    )

    while (true) {
      Thread.sleep(60000)
    }
  }
}
