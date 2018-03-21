package beaker.benchmark.util

import scala.annotation.tailrec

object Probability {

  /**
   * Returns the factorial of x computed tail-recursively.
   *
   * @param x Value.
   * @return Factorial.
   */
  def factorial(x: BigInt): BigInt = {
    @tailrec
    def fold(n: BigInt, v: BigInt): BigInt = if (n == 0) v else fold(n * v, n - 1)
    require(x >= 0, s"Factorial undefined for $x")
    fold(x, 1)
  }

  /**
   * Returns the number of ways to choose unordered k-subsets from a set of size n.
   *
   * @param n Size of set.
   * @param k Size of subsets.
   * @return nCk.
   */
  def combinations(n: BigInt, k: BigInt): BigInt = {
    require(n >= 0, s"Combinations undefined for sets of size $n")
    require(k <= n, s"Combinations undefined for subsets of size $k > $n")
    if (k > n) 0 else factorial(n) / (factorial(k) * factorial(n - k))
  }

  /**
   * Returns the number ways to choose ordered k-subsets from a set of size n.
   *
   * @param n Size of set.
   * @param k Size of subsets.
   * @return nPr.
   */
  def permutations(n: BigInt, k: BigInt): BigInt = {
    require(n >= 0, s"Permutations undefined for sets of size $n")
    require(k <= n, s"Permutations undefined for subsets of size $k > $n")
    if (k > n) 0 else factorial(n) / factorial(n - k)
  }

}
