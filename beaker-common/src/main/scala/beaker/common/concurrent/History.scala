package beaker.common.concurrent

import scala.reflect.ClassTag

/**
 * A collection of events. Histories remember a finite number of previous events, which may be used
 * to prevent the same events from occurring again. Histories are implemented as circular arrays.
 *
 * @param buffer Underlying array.
 */
class History[T](buffer: Array[T]) extends Locking {

  private[this] var cursor = 0

  /**
   * Returns whether or not the event has recently happened.
   *
   * @param event Event to check.
   * @return Whether or not the event recently occurred.
   */
  def happened(event: T): Boolean = shared {
    this.buffer.contains(event)
  }

  /**
   * Adds the event to the history if it has not recently happened.
   *
   * @param event Event that occurred.
   */
  def occurred(event: T): Unit = exclusive {
    if (!this.buffer.contains(event)) {
      this.buffer(this.cursor) = event
      this.cursor = (this.cursor + 1) % this.buffer.length
    }
  }

}

object History {

  /**
   * Constructs a history of the specified size.
   *
   * @param size History length.
   * @return Empty history.
   */
  def apply[T: ClassTag](size: Int): History[T] = new History(Array.ofDim[T](size))

}