package beaker.client

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.io.StdIn

object Main extends App {

  //
  if (args.length != 1)
    println("Usage: beaker host:port")

  //
  val address  = args(0).split(":")
  val client   = Client(this.address(0), this.address(1).toInt)
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