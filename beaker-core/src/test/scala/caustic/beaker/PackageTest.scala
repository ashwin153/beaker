package caustic.beaker

import caustic.beaker.thrift.{Ballot, Revision, Transaction}
import caustic.beaker.common._

import org.junit.runner.RunWith
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

import scala.math.Ordering.Implicits._

@RunWith(classOf[JUnitRunner])
class PackageTest extends FunSuite with Matchers with ScalaFutures {

  test("Ballots are totally ordered") {
    val x = new Ballot(0, 1)
    val y = new Ballot(1, 0)

    // Ordered lexicographically.
    (x < y) shouldBe true
  }

  test("Revisions are totally ordered") {
    val x = new Revision(0L, "b")
    val y = new Revision(1L, "a")

    // Ordered by version.
    (x < y) shouldBe true
  }

  test("Transactions are related") {
    val x = new Transaction
    x.putToChanges("a", new Revision(0L, ""))
    val y = new Transaction
    y.putToDepends("a", 0L)

    // Related by read-write conflicts.
    (x ~ y) shouldBe true
    (y ~ x) shouldBe true

    // Related by write-write conflicts.
    (x ~ x) shouldBe true

    // Not related by read-read conflicts.
    (y ~ y) shouldBe false
  }

}
