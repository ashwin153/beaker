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
      case "get" :: keys => format(this.client.get(keys.toSet))
      case "network" :: Nil => format(this.client.network())
      case "help" :: _ => format(this.usage)
      case "put" :: key :: value :: Nil => format(this.client.put(key, value))
      case "print" :: Nil => Await.result(this.client.foreach(format), Duration.Inf)
      case "quit" :: _ => this.continue = false
      case _ => format(this.usage)
    }
  }

  /**
   *get
   * @param any
   * @return
   */
  def format(any: Any): Unit = any match {
    case x: Try[_] => format(x)
    case x: Map[Key, Revision] => format(x)
    case (key: Key, revision: Revision) => format(key, revision)
    case x => println(x)
  }

  /**
   *
   * @param x
   * @tparam T
   * @return
   */
  def format[T](x: Try[T]): Unit = x match {
    case Success(s) => format(s)
    case Failure(e) => println(s"${ RED }${ e.getMessage }${ RESET }")
  }

  /**
   *
   * @param key
   * @param revision
   */
  def format(key: Key, revision: Revision): Unit =
    println("%-25s %03d %s".format(key, revision.version, revision.value))

  /**
   *
   * @param entries
   * @return
   */
  def format(entries: Map[Key, Revision]): Unit =
    println(entries map { case (k, r) => format(k, r) } mkString "\n")

}