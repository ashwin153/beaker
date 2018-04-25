package beaker.client

import beaker.server.protobuf._

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.Console._
import scala.io.StdIn
import scala.language.postfixOps
import scala.util.{Failure, Success}

object Shell extends App {

  // Usage Information.
  val usage: String =
    s"""cas <key> <value> ... if <key> <version> ...  Conditionally updates the specified keys.
       |get <key> ...                                 Returns the values of the specified keys.
       |network                                       Returns the current network configuration.
       |help                                          Prints this help message.
       |kill <host>:<port>                            Removes the address from the configuration.
       |put <key> <value> ...                         Updates the values of the specified keys.
       |print                                         Prints the contents of the database.
       |quit                                          Exit.
     """.stripMargin

  // Connect Client.
  val address = if (args.isEmpty) Address("localhost", 9090) else args(0).toAddress
  val client = Client(this.address)

  // REPL.
  var continue = true
  while (this.continue) {
    print(s"${ GREEN }${ this.address.name }:${ this.address.port }>${ RESET } ")
    StdIn.readLine.split("\\s+(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)").toList match {
      case "cas" :: args if args.count(_ != "if") % 2 == 0 =>
        val depends = parse(args.slice(args.indexOf("if") + 1, args.size), _.toLong)
        val changes = parse(args.slice(0, args.indexOf("if")), identity)
        dump(this.client.cas(depends, changes))
      case "get" :: keys if keys.nonEmpty =>
        dump(this.client.get(keys.map(parse)))
      case "network" :: Nil =>
        dump(this.client.network())
      case "help" :: _ =>
        println(this.usage)
      case "kill" :: host :: Nil if host.split(":").length == 2 =>
        val at = host.toAddress
        dump(this.client.network().map(_.configuration)
          .map(x => Configuration(x.acceptors.filter(_ != at), x.learners.filter(_ != at)))
          .flatMap(this.client.reconfigure))
      case "put" :: entries if entries.size % 2 == 0 =>
        dump(this.client.put(parse(entries, identity)))
      case "print" :: Nil =>
        Await.result(this.client.scan(dump), Duration.Inf)
      case "quit" :: _ =>
        this.continue = false
      case _ =>
        println(this.usage)
    }
  }

  /**
   * Converts the specified string to a key by removing leading and trailing quotes.
   *
   * @param x String.
   * @return Key.
   */
  def parse(x: String): Key = x.replaceAll("(^\\\"|\\\"$)", "")

  /**
   * Converts the specified list to a map in which keys are at odd indicies and values at even ones.
   *
   * @param x List.
   * @param f Value transformer.
   * @return Map.
   */
  def parse[T](x: List[String], f: String => T): Map[Key, T] =
    x.grouped(2) map { case List(k, v) => parse(k) -> f(v) } toMap

  /**
   * Formats and prints the specified value.
   *
   * @param any Value.
   */
  def dump(any: Any): Unit = any match {
    case (k: Key, r: Revision) =>
      println(s"$CYAN%06d$RESET %-80s %s".format(r.version, k, r.value))
    case (k: Key, v: Version) =>
      dump((k, Revision(v, "")))
    case Success(x) =>
      dump(x)
    case Failure(x) =>
      println(s"${ RED }Failure. Please try again.${ RESET }")
    case x: Map[_, _] =>
      x foreach { case (k, r) => dump((k, r)) }
    case View(b, Configuration(a, l)) =>
      println(
        s"""${ YELLOW }ballot$RESET    ${ "%019d %019d".format(b.round, b.id)}
           |${ YELLOW }acceptors$RESET ${ a.map(x => s"${ x.name }:${ x.port }").mkString(", ") }
           |${ YELLOW }learners$RESET  ${ l.map(x => s"${ x.name }:${ x.port }").mkString(", ") }
         """.stripMargin)
    case Unit =>
      println
    case x =>
      println(x)
  }

}