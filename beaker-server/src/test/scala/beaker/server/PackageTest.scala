package beaker.server

import beaker.common.util._
import beaker.server.protobuf._

import beaker.server.protobuf.Revision
import org.junit.runner.RunWith
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}
import scala.math.Ordering.Implicits._

@RunWith(classOf[JUnitRunner])
class PackageTest extends FunSuite with Matchers with ScalaFutures {

  test("Ballots are totally ordered") {
    val x = Ballot(0, 1)
    val y = Ballot(1, 0)

    // Ordered lexicographically.
    (x < y) shouldBe true
  }

  test("Revisions are totally ordered") {
    val x = Revision(0L, "b")
    val y = Revision(1L, "a")

    // Ordered by version.
    (x < y) shouldBe true
  }

  test("Transactions are related") {
    val x = Transaction(Map.empty, Map("a" -> Revision(0L, "v")))
    val y = Transaction(Map("a" -> 0L), Map.empty)

    // Related by read-write conflicts.
    (x ~ y) shouldBe true
    (y ~ x) shouldBe true

    // Related by write-write conflicts.
    (x ~ x) shouldBe true

    // Not related by read-read conflicts.
    (y ~ y) shouldBe false
  }

}
