package beaker.common.concurrent

import beaker.common.relation._

import org.junit.runner.RunWith
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.util.Try

@RunWith(classOf[JUnitRunner])
class ExecutorTest extends FunSuite with Matchers with ScalaFutures {

  test("Related tasks are sequential") {
    val executor  = new Executor[Int](Relation.Total)
    val scheduled = new CountDownLatch(1)
    val barrier   = new CountDownLatch(1)

    // Schedule all tasks sequentially.
    executor.submit(0)(_ => Try(scheduled.await()))
    val x = executor.submit(1)(_ => Try(barrier.await(50, TimeUnit.MILLISECONDS)))
    val y = executor.submit(2)(_ => Try(barrier.countDown()))
    scheduled.countDown()

    // X will fail if and only if it is strictly executed before Y.
    whenReady(x)(r => r shouldBe false)
    executor.close()
  }

  test("Unrelated tasks are concurrent") {
    val executor  = new Executor[Int](Relation.Identity)
    val scheduled = new CountDownLatch(1)
    val barrier   = new CountDownLatch(2)

    // Schedule all tasks concurrently.
    executor.submit(0)(_ => Try(scheduled.await()))
    val x = executor.submit(1)(_ => Try { barrier.countDown(); barrier.await(50, TimeUnit.MILLISECONDS) })
    val y = executor.submit(2)(_ => Try { barrier.countDown(); barrier.await(50, TimeUnit.MILLISECONDS) })
    scheduled.countDown()

    // X and Y will both succeed if and only if they are executed concurrently.
    whenReady(x)(r => r shouldEqual true)
    whenReady(y)(r => r shouldEqual true)
    executor.close()
  }

}
