package beaker

import beaker.common.relation._
import beaker.common.util.{Order, Relation}
import scala.math.Ordering.Implicits._
import scala.util.Try

package object common {

  implicit class MapOps[A, B](x: Map[A, B]) {

    /**
     * Returns the largest value for each key present in either map.
     *
     * @param y A map.
     * @param ordering Implicit value ordering.
     * @return Maximal map.
     */
    def max(y: Map[A, B])(implicit ordering: Ordering[B]): Map[A, B] =
      x ++ y map { case (k, v) => k -> (x.getOrElse(k, v) max v) }

    /**
     * Returns the smallest value for each key present in both maps.
     *
     * @param y A map.
     * @param ordering Implicit value ordering.
     * @return Minimal map.
     */
    def min(y: Map[A, B])(implicit ordering: Ordering[B]): Map[A, B] =
      x collect { case (k, v) if y.contains(k) => k -> (v min y(k)) }

  }

  implicit class TryOps[T](x: Try[T]) {

    /**
     * Performs the side-effecting function if the try completes successfully.
     *
     * @param f Side-effect.
     * @return Side-effecting try.
     */
    def andThen[U](f: T => U): Try[T] = x map { t => f(t); t }

  }

  implicit class RelationOps[T](x: T)(implicit relation: Relation[T]) {

    /**
     * Returns whether or not the elements are related.
     *
     * @param y An element.
     * @return Whether or not they are related.
     */
    def ~(y: T): Boolean = relation.related(x, y)

  }

  implicit class OrderOps[T](x: T)(implicit order: Order[T]) {

    /**
     * Returns whether or not x is partially ordered before y.
     *
     * @param y An element.
     * @return Whether or not x is before y.
     */
    def <|(y: T): Boolean = order.before(x, y).exists(identity)

    /**
     * Returns whether or not y is partially ordered before x.
     *
     * @param y An element.
     * @return Whether or not y is before x.
     */
    def |>(y: T): Boolean = order.before(y, x).exists(identity)

    /**
     * Returns whether or not x and y are not partially ordered.
     *
     * @param y An element.
     * @return Whether or not x and y are incomparable.
     */
    def <>(y: T): Boolean = order.before(x, y).isEmpty

  }

}
