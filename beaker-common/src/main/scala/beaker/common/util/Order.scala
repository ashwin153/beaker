package beaker.common.util

import scala.language.implicitConversions

/**
 * A partial ordering. Orders are reflexive (x <| x) and anti-symmetric (x <| y -> y |> x), but,
 * unlike their mathematical counterparts, may not be transitive (x <| y, y <| z -> x <| z).
 */
trait Order[-T] {

  /**
   * Returns whether or not x is partially ordered before y if x and y are comparable.
   *
   * @param x An element.
   * @param y Another element.
   * @return Whether or not x is before y.
   */
  def before(x: T, y: T): Option[Boolean]

}