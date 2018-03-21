package beaker.benchmark.experiment

import beaker.benchmark.util.Probability

import scala.util.Random

object Contention {

  /**
   * Returns the likelihood that two randomized transactions conflict. Let K be the set of all
   * possible keys, and let |K| = n. Given two transactions A and B each containing l keys drawn
   * uniformly at random from K, what is the probability that they conflict? Equivalently, we may
   * find the complement of the probability that A and B are disjoint which is the likelihood that B
   * is drawn from K - A.
   *
   * @param n Size of key space.
   * @param l Transaction size.
   * @return Contention probability.
   */
  def contention(n: BigInt, l: BigInt): BigDecimal =
    1 - BigDecimal(Probability.combinations(n - l, l)) / BigDecimal(Probability.combinations(n, l))

  /**
   * Returns the average number of attempts required for a transaction to successfully complete. We
   * may model the number of attempts A as the negative binomial distribution A ~ 1 + NB(p, 1) where
   * p is the contention probability. Therefore, the average number of attempts is 1 + p / (1 - p).
   *
   * @param p Contention probability.
   * @return Average number of attempts.
   */
  def attempts(p: BigDecimal): BigDecimal =
    1 + p / (1 - p)

  /**
   * Returns a sequence of l integers drawn uniformly at random from [0, n).
   *
   * @param n Population size.
   * @param l Sample size.
   * @return Uniformly random integers.
   */
  def random(n: Int, l: Int): Seq[Int] =
    Random.shuffle(Seq.range(0, n)).take(l)

  /**
   * Returns the sequence of integers [i, i + l). Used to construct disjoint transactions.
   *
   * @param i Initial number.
   * @param l Sample size.
   * @return Sequential integers.
   */
  def disjoint(i: Int, l: Int): Seq[Int] =
    Seq.range(i * l, i * l + l)

}
