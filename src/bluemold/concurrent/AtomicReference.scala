package bluemold.concurrent

import org.bluemold.unsafe.Unsafe

/**
 * AtomicReference<br/>
 * Author: Neil Essy<br/>
 * Created: 4/22/11<br/>
 * <p/>
 * [Description]
 */

object AtomicReference {
  val unsafeValueIndex = Unsafe.objectDeclaredFieldOffset( classOf[AtomicReference[AnyRef]], "value" )

  def create[T <: AnyRef ](): AtomicReference[T] = new AtomicReference[T]()
  def create[T <: AnyRef ]( value: T ): AtomicReference[T] = create[T]().initialize( value )  
}

class AtomicReference[T <: AnyRef ] {
  @volatile var value: T = _ 

  private[concurrent] final def initialize( value: T ): AtomicReference[T] = { set( value ); this }

  def get(): T = value
  def set(value: T) { this.value = value }
  def compareAndSet(expect: T, update: T): Boolean = 
    Unsafe.compareAndSwapObject( this, AtomicReference.unsafeValueIndex, expect, update )

  /**
   * Returns the String representation of the current value.
   * @return the String representation of the current value.
   */
  override def toString: String =
  {
    get().toString
  }
}
