package beaker.benchmark.util

object Probability {

  /**
   *
   * @param x
   * @return
   */
  def factorial(x: BigInt): BigInt =
    if (x == 0) 1 else x * factorial(x - 1)

  /**
   *
   * @param n
   * @param k
   * @return
   */
  def combinations(n: BigInt, k: BigInt): BigInt =
    if (k > n) 0 else factorial(n) / (factorial(k) * factorial(n - k))

  /**
   *
   * @param n
   * @param k
   * @return
   */
  def permutations(n: BigInt, k: BigInt): BigInt =
    if (k > n) 0 else factorial(n) / factorial(n - k)

}
