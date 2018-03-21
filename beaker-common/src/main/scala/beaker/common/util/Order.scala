package beaker.common.util

import scala.language.implicitConversions

/**
 * A partial ordering. Orders are anti-symmetric (x <| y -> y |> x).
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