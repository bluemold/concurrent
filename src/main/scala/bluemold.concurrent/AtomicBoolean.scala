package bluemold.concurrent

import org.bluemold.unsafe.Unsafe

object AtomicBoolean {
  private[concurrent] val unsafeValueIndex = Unsafe.objectDeclaredFieldOffset( classOf[AtomicBoolean], "value")

  def create(): AtomicBoolean = new AtomicBoolean()
  def create( value: Boolean ): AtomicBoolean = new AtomicBoolean( value )  
}

final class AtomicBoolean( _default: Boolean ) {
  import AtomicBoolean._

  def this() = this( false )

  @volatile private var value: Int = if ( _default ) 1 else 0

  def get(): Boolean = ( value != 0 )
  def set(value: Boolean) { this.value = if ( value ) 1 else 0 }
  def compareAndSet(expect: Boolean, update: Boolean): Boolean = 
    compareAndSet0( if ( expect ) 1 else 0, if ( update ) 1 else 0 )
  def compareAndSet0(expect: Int, update: Int): Boolean = 
    Unsafe.compareAndSwapInt( this, unsafeValueIndex, expect, update )

  /**
   * Atomically sets to the given value and returns the old value.
   *
   * @param newValue the new value
   * @return the previous value
   */
  def getAndSet(newValue: Boolean): Boolean =
  {
      val current: Boolean = get()
      if ( compareAndSet(current, newValue) ) current else getAndSet( newValue )
  }

  /**
   * Atomically increments by one the current value.
   *
   * @return the previous value
   */
  def getAndNegate(): Boolean =
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
  def negateAndGet(): Boolean =
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
