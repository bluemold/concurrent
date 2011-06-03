package bluemold.concurrent

import org.bluemold.unsafe.Unsafe

/**
 * AtomicLong<br/>
 * Author: Neil Essy<br/>
 * Created: 4/22/11<br/>
 * <p/>
 * [Description]
 */

object AtomicBoolean {
  val unsafeValueIndex = Unsafe.objectDeclaredFieldOffset( classOf[AtomicBooleanUnsafe], "value")

  def create(): AtomicBoolean = new AtomicBooleanUnsafe()
  def create( value: Boolean ): AtomicBoolean = create().initialize( value )  
}

abstract class AtomicBoolean {
  def get(): Boolean
  def set( value: Boolean )
  def compareAndSet( expect: Boolean, update: Boolean ): Boolean

  private[concurrent] final def initialize( value: Boolean ): AtomicBoolean = { set( value ); this }

  /**
   * Atomically sets to the given value and returns the old value.
   *
   * @param newValue the new value
   * @return the previous value
   */
  final def getAndSet(newValue: Boolean): Boolean =
  {
      val current: Boolean = get()
      if ( compareAndSet(current, newValue) ) current else getAndSet( newValue )
  }

  /**
   * Atomically increments by one the current value.
   *
   * @return the previous value
   */
  final def getAndNegate(): Boolean =
  {
      val current: Boolean = get()
      val next: Boolean = ! current
      if ( compareAndSet(current, next) ) current else getAndNegate()
  }

  /**
   * Atomically increments by one the current value.
   *
   * @return the updated value
   */
  final def negateAndGet(): Boolean =
  {
      val current: Boolean = get()
      val next: Boolean = ! current
      if ( compareAndSet(current, next) ) next else negateAndGet()
  }

  /**
   * Returns the String representation of the current value.
   * @return the String representation of the current value.
   */
  override def toString: String =
  {
    get().toString
  }
}

private class AtomicBooleanUnsafe extends AtomicBoolean {
  @volatile var value: Int = 0
  override def get(): Boolean = ( value != 0 )
  override def set(value: Boolean) { this.value = if ( value ) 1 else 0 }
  override def compareAndSet(expect: Boolean, update: Boolean): Boolean = 
    compareAndSet0( if ( expect ) 1 else 0, if ( update ) 1 else 0 )
  def compareAndSet0(expect: Int, update: Int): Boolean = 
    Unsafe.compareAndSwapInt( this, AtomicBoolean.unsafeValueIndex, expect, update )
}
