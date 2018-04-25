package beaker.client

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.io.StdIn

object Main extends App {

  val address  = if (args.isEmpty) Seq("localhost", "9090") else args(0).split(":").toSeq
  val client   = Client(this.address.head, this.address.last.toInt)
  var continue = true

  while (this.continue) {
    print("> ")
    val command = StdIn.readLine().split(" ").toList
    println()

    command match {
      case "get" :: key :: Nil => println(this.client.get(key))
      case "get" :: keys => this.client.get(keys.toSet).foreach(_.foreach(println))
      case "network" :: Nil => this.client.network().foreach(println)
      case "put" :: key :: value :: Nil => this.client.put(key, value)
      case "print" :: Nil => Await.result(this.client foreach { case (k, v) => println(s"$k -> $v") }, Duration.Inf)
      case "quit" :: _ => this.continue = false
      case _ => s"Unknown command $command"
    }
  }

}