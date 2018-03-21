package beaker.common

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits._
import scala.language.implicitConversions
import scala.math.Ordering.Implicits._
import scala.util.Try

package object util extends Retry {

  implicit def mmap2map[A, B](x: mutable.Map[A, B]): Map[A, B] = x.toMap
  implicit def mmap2ops[A, B](x: mutable.Map[A, B]): MapOps[A, B] = MapOps(x.toMap)

  implicit class MapOps[A, B](x: Map[A, B]) {

    /**
     * Returns the inverse mapping.
     *
     * @return Inverted map.
     */
    def invert: Map[B, Set[A]] = x.groupBy(_._2).mapValues(_.keySet)

    /**
     * Returns the largest value for each key present in either map.
     *
     * @param y A map.
     * @param ordering Implicit value ordering.
     * @return Maximal map.
     */
    def maximum(y: Map[A, B])(implicit ordering: Ordering[B]): Map[A, B] =
      x ++ y map { case (k, v) => k -> (x.getOrElse(k, v) max v) }

    /**
     * Returns the smallest value for each key present in both maps.
     *
     * @param y A map.
     * @param ordering Implicit value ordering.
     * @return Minimal map.
     */
    def minimum(y: Map[A, B])(implicit ordering: Ordering[B]): Map[A, B] =
      x collect { case (k, v) if y.contains(k) => k -> (v min y(k)) }

  }

  implicit class MutableMapOps[A, B](x: mutable.Map[A, B]) {

    /**
     * Removes all entries that satisfy the predicate and returns their values.
     *
     * @param f Predicate.
     * @return Values of removed keys.
     */
    def remove(f: (A, B) => Boolean): Map[A, B] = {
      val removed = x.filter(f.tupled)
      x --= removed.keys
      removed.toMap
    }

    /**
     * Removes all keys that satisfy the predicate and returns their values.
     *
     * @param f Predicate.
     * @return Values of removed keys.
     */
    def removeKeys(f: A => Boolean): Map[A, B] = {
      val removed = x.filterKeys(!f(_))
      x --= removed.keys
      removed.toMap
    }

  }

  implicit class SeqOps[T](x: Seq[T]) {

    /**
     * Removes all occurrences of the element from the sequence.
     *
     * @param y An element.
     * @return Filtered sequence.
     */
    def remove(y: T): Seq[T] = x.filterNot(_ != y)

  }

  implicit class TryOps[T](x: Try[T]) {

    /**
     * Performs the side-effecting function if the try completes successfully.
     *
     * @param f Side-effect.
     * @return Side-effecting try.
     */
    def andThen[U](f: T => U): Try[T] = x map { t => f(t); t }

    /**
     * Converts the try to a future.
     *
     * @return Asynchronous try.
     */
    def toFuture: Future[T] = Future.fromTry(x)

    /**
     * Converts the try to a unit.
     *
     * @return Unit try.
     */
    def toUnit: Try[Unit] = x.map(_ => ())

  }

  implicit class OptionOps[T](x: Option[T]) {

    /**
     * Converts the option to a try.
     *
     * @return Try.
     */
    def toTry: Try[T] = Try(x) collect { case Some(v) => v }

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

  implicit class OrderOps[T](x: T)(implicit order: PartialOrder[T]) {

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
