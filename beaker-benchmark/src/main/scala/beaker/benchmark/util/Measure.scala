package beaker.benchmark.util

import scala.concurrent.duration._
import scala.language.postfixOps

object Measure {

  /**
   * Returns the duration of the function with nanosecond granularity.
   *
   * @param f Function to test.
   * @return Latency.
   */
  def latency[T](f: => T): Duration = {
    val current = System.nanoTime()
    f
    Duration.fromNanos(System.nanoTime() - current)
  }

  /**
   * Returns the number of times per second the function can be called.
   *
   * @param f Function to test.
   * @return Throughput per second.
   */
  def throughput[T](f: => T): Double = (1 second) / latency(f)

}
