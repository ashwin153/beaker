package beaker.benchmark.util

import scala.concurrent.duration.Duration

object Measure {

  /**
   * Returns the duration of the function with nanosecond granularity.
   *
   * @param f Function to test.
   * @return Latency.
   */
  def latency[U](f: => U): Duration = {
    val current = System.nanoTime()
    f
    Duration.fromNanos(System.nanoTime() - current)
  }

  /**
   * Returns the number of times per second the function can be called.
   *
   * @param f Function to test.
   * @return Throughput.
   */
  def throughput(f: => Int): Double = {
    val current = System.nanoTime()
    1E9 * f / (System.nanoTime() - current)
  }

}
