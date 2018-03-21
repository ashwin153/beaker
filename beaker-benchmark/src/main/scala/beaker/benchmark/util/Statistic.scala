package beaker.benchmark.util

object Statistic {

  /**
   * Returns the average of the sequence of values.
   *
   * @param numbers Values.
   * @return Average.
   */
  def mean(numbers: Seq[Double]): Double = {
    require(numbers.nonEmpty, "Mean undefined for empty sequences")
    numbers.sum / numbers.length
  }

  /**
   * Returns the variance in the sequence of values.
   *
   * @param numbers Values.
   * @return Variance.
   */
  def variance(numbers: Seq[Double]): Double = {
    require(numbers.nonEmpty, "Variance undefined for empty sequences")
    val avg = mean(numbers)
    numbers.map(x => math.pow(x - avg, 2)).sum / numbers.length
  }

  /**
   * Returns the standard deviation of the sequence of values.
   *
   * @param numbers Values.
   * @return Standard deviation.
   */
  def stdev(numbers: Seq[Double]): Double = {
    require(numbers.nonEmpty, "Standard deviation undefined for empty sequences")
    math.sqrt(variance(numbers))
  }

}