package beaker.client

import beaker.server.protobuf.{Address, Configuration, Revision}
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.Console._
import scala.io.StdIn
import scala.language.postfixOps
import scala.util.{Failure, Success}

object Shell extends App {

  // Usage Information.
  val usage: String =
    s"""get <key> ...                               Returns the values of the specified keys.
       |network                                     Returns the current network configuration.
       |help                                        Prints this help message.
       |kill <host>:<port>                          Removes the address from the configuration.
       |put <key> <value> ...                       Updates the values of the specified keys.
       |print                                       Prints the contents of the database.
       |quit                                        Exit.
     """.stripMargin

  // Connect Client.
  val address = if (args.isEmpty) Address("localhost", 9090) else args(0).toAddress
  val client = Client(this.address)

  // REPL.
  var continue = true
  while (this.continue) {
    print(s"${ GREEN }${ this.address.name }:${ this.address.port }>${ RESET } ")
    StdIn.readLine().split(" ").toList match {
      case "get" :: keys =>
        dump(this.client.get(keys))
      case "network" :: Nil =>
        dump(this.client.network())
      case "help" :: _ =>
        dump(this.usage)
      case "kill" :: host :: Nil =>
        val at = host.toAddress
        dump(this.client.network().map(_.configuration)
          .map(x => Configuration(x.acceptors.filter(_ != at), x.learners.filter(_ != at)))
          .flatMap(this.client.reconfigure))
      case "put" :: entries =>
        dump(this.client.put(entries.grouped(2) map { case List(k, v) => k -> v } toMap))
      case "print" :: Nil =>
        Await.result(this.client.foreach(dump), Duration.Inf)
      case "quit" :: _ =>
        this.continue = false
      case _ =>
        dump(this.usage)
    }
  }

  /**
   * Formats and prints the specified value.
   *
   * @param any Value.
   */
  def dump(any: Any): Unit = any match {
    case Success(x) => dump(x)
    case Failure(x) => println(s"${ RED }${ x.getMessage }${ RESET }")
    case x: Map[Key, Revision] => x foreach { case (k, r) => dump(k, r) }
    case (key: Key, revision: Revision) => dump(key, revision)
    case Unit => println()
    case x => println(x)
  }

  /**
   * Formats and prints the specified key-value pair.
   *
   * @param key Key.
   * @param revision Revision.
   */
  def dump(key: Key, revision: Revision): Unit =
    println("%-25s %05d %s".format(key, revision.version, revision.value))

}