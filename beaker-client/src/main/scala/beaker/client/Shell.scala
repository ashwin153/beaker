package beaker.client

import beaker.server.protobuf.{Revision, View}

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.Console._
import scala.io.StdIn
import scala.util.{Failure, Success, Try}

object Shell extends App {

  // Usage Information.
  val usage: String =
    s"""get <key> ...                               Returns the values of the specified keys.
       |network                                     Returns the current network configuration.
       |help                                        Prints this help message.
       |put <key> <value> ...                       Updates the values of the specified keys.
       |print                                       Prints the contents of the database.
       |quit                                        Exit.
     """.stripMargin

  val address  = if (args.isEmpty) Seq("localhost", "9090") else args(0).split(":").toSeq
  val client   = Client(this.address.head, this.address.last.toInt)
  var continue = true

  while (this.continue) {
    print(s"${ GREEN }${ this.address.head }:${ this.address.last }>${ RESET } ")
    StdIn.readLine().split(" ").toList match {
      case "get" :: keys => dump(this.client.get(keys.toSet))
      case "network" :: Nil => dump(this.client.network())
      case "help" :: _ => println(this.usage)
      case "put" :: key :: value :: Nil => dump(this.client.put(key, value))
      case "print" :: Nil => Await.result(this.client.foreach(dump), Duration.Inf)
      case "quit" :: _ => this.continue = false
      case _ => println(this.usage)
    }
  }

  /**
   * Prints the key-value pair to standard out.
   *
   * @param key Key.
   * @param revision Revision.
   */
  def dump(key: Key, revision: Revision): Unit =
    println(s"${ key.padTo(25, " ") }${ "%03d".format(revision.version) }${ revision.value }")

  /**
   * Prints the result of the request to standard out.
   *
   * @param request Request.
   */
  def dump[T](request: Try[T]): Unit = request match {
    case Success(x: Map[Key, Revision]) => x foreach { case (k, r) => dump(k, r) }
    case Success(x: View) => println(x)
    case Success(_) =>
    case Failure(e) => println(s"${ RED }${ e.getMessage }${ RESET }")
  }

}