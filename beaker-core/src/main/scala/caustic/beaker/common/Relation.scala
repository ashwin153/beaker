package caustic.beaker.common

import scala.language.implicitConversions

/**
 * A binary relation. Relations are reflexive (x ~ x) and symmetric (x ~ y -> y ~ x), but not
 * necessarily transitive (x ~ y, y ~ z -> x ~ z).
 */
trait Relation[-T] {

  def related(x: T, y: T): Boolean

}

object Relation {

  // All elements are related.
  val Total: Relation[Any] = (x, y) => true

  // All elements are unrelated.
  val Identity: Relation[Any] = (x, y) => x == y

}