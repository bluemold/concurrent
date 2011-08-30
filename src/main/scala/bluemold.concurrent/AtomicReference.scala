package bluemold.concurrent

import org.bluemold.unsafe.Unsafe

object AtomicReference {
  private[concurrent] val unsafeValueIndex = Unsafe.objectDeclaredFieldOffset( classOf[AtomicReference[AnyRef]], "value" )

  def create[T <: AnyRef ](): AtomicReference[T] = new AtomicReference[T]()
  def create[T <: AnyRef ]( value: T ): AtomicReference[T] = new AtomicReference[T]( value )  
}

final class AtomicReference[T <: AnyRef ]( _default: T ) {
  import AtomicReference._

  def this() = this( null.asInstanceOf[T] )

  @volatile private var value: T = _default 

  def get(): T = value
  def set(value: T) { this.value = value }
  def compareAndSet(expect: T, update: T): Boolean = 
    Unsafe.compareAndSwapObject( this, unsafeValueIndex, expect, update )

  /**
   * Returns the String representation of the current value.
   * @return the String representation of the current value.
   */
  override def toString: String =
  {
    get().toString
  }
}
