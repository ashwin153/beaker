package beaker.benchmark.util

object Statistic {

  /**
   *
   * @param numbers
   * @return
   */
  def mean(numbers: Seq[Double]): Double =
    numbers.sum / numbers.length

  /**
   *
   * @param numbers
   * @return
   */
  def variance(numbers: Seq[Double]): Double = {
    val avg = mean(numbers)
    numbers.map(x => math.pow(x - avg, 2)).sum / numbers.length
  }

  /**
   *
   * @param numbers
   * @return
   */
  def stdev(numbers: Seq[Double]): Double =
    math.sqrt(variance(numbers))

}