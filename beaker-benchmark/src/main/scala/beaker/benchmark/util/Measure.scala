package beaker.benchmark.util

import scala.concurrent.duration.Duration

/**
 *
 */
object Measure {

  /**
   *
   * @param f
   * @tparam U
   * @return
   */
  def latency[U](f: => U): Duration = {
    val current = System.nanoTime()
    f
    Duration.fromNanos(System.nanoTime() - current)
  }

  /**
   *
   * @param f
   * @return
   */
  def throughput(f: => Int): Double = {
    val current = System.nanoTime()
    1E9 * f / (System.nanoTime() - current)
  }

}
