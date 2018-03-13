package caustic.cluster
package protocol

import caustic.cluster

import org.apache.thrift.{TProcessor, TServiceClient, TServiceClientFactory}
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.server.TNonblockingServer
import org.apache.thrift.transport.{TFramedTransport, TNonblockingServerSocket, TSocket}

/**
 * An [[https://thrift.apache.org/ Apache Thrift]] implementation.
 */
object Thrift {

  /**
   * A Thrift service.
   *
   * @param factory Client factory.
   */
  case class Service[T <: TServiceClient](
    factory: TServiceClientFactory[T]
  ) extends cluster.Service[Thrift.Client[T]] {

    override def connect(address: Address): Client[T] =
      Thrift.Client(address, this.factory)

    override def disconnect(client: Client[T]): Unit =
      client.transport.close()

  }

  /**
   * A Thrift client.
   *
   * @param address Instance [[Address]].
   * @param factory Client factory.
   */
  case class Client[T <: TServiceClient](
    address: Address,
    factory: TServiceClientFactory[T]
  ) {

    val transport = new TFramedTransport(new TSocket(address.host, address.port))
    val protocol  = new TBinaryProtocol(this.transport)

    // Underlying Thrift client.
    lazy val connection: T = {
      this.transport.open()
      this.factory.getClient(this.protocol)
    }

  }

  /**
   * A Thrift server.
   *
   * @param address Server [[Address]].
   * @param processor Thrift request processor.
   */
  case class Server(
    address: Address,
    processor: TProcessor
  ) extends cluster.Server {

    //  Construct an asynchronous, non-blocking Thrift server.
    val transport = new TNonblockingServerSocket(this.address.port)
    val arguments = new TNonblockingServer.Args(this.transport).processor(this.processor)
    val server    = new TNonblockingServer(this.arguments)
    val thread    = new Thread(() => this.server.serve())

    override def serve(): Unit = {
      this.thread.start()
    }

    override def close(): Unit = {
      this.transport.close()
      this.server.stop()
    }

  }

}
