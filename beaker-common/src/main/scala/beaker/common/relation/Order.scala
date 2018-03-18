package beaker.common.relation

import scala.language.implicitConversions

/**
 * A partial ordering. Orders are reflexive (x <| x) and anti-symmetric (x <| y -> y |> x), but,
 * unlike their mathematical counterparts, may not be transitive (x <| y, y <| z -> x <| z).
 * Orders induce a [[Relation]] on comparable elements.
 */
trait Order[-T] extends Relation[T] {

  /**
   * Returns whether or not x is partially ordered before y if x and y are comparable.
   *
   * @param x An element.
   * @param y Another element.
   * @return Whether or not x is before y.
   */
  def before(x: T, y: T): Option[Boolean]

  override def related(x: T, y: T): Boolean =
    before(x, y).exists(identity) || before(y, x).exists(identity)

}