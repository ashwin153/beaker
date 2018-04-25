package beaker.client

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.Console._
import scala.io.StdIn

object Shell extends App {

  //
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
    print(s"${ GREEN }${ args(0) }>${ RESET } ")

    StdIn.readLine().split(" ").toList match {
      case "get" :: keys => this.client.get(keys.toSet).foreach(_.foreach(println))
      case "network" :: Nil => this.client.network().foreach(println)
      case "help" :: _ => println(this.usage)
      case "put" :: key :: value :: Nil => this.client.put(key, value)
      case "print" :: Nil => Await.result(this.client foreach { case (k, v) => println(s"$k -> $v") }, Duration.Inf)
      case "quit" :: _ => this.continue = false
      case _ => println(this.usage)
    }
  }

}